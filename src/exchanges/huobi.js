const Exchange = require('../exchange')
const pako = require('pako')
const WebSocket = require('websocket').w3cwebsocket
const axios = require('axios')
const { getHms } = require('../helper')

class Huobi extends Exchange {
  constructor() {
    super()

    this.id = 'HUOBI'

    this.receivedInitialData = {}
    this.liquidationOrdersSubscriptions = {}

    this.contractTypesAliases = {
      this_week: 'CW',
      next_week: 'NW',
      quarter: 'CQ',
      next_quarter: 'NQ',
    }

    this.prices = {}

    this.endpoints = {
      PRODUCTS: [
        'https://api.huobi.pro/v1/common/symbols',
        'https://api.hbdm.com/api/v1/contract_contract_info',
        'https://api.hbdm.com/swap-api/v1/swap_contract_info',
        'https://api.hbdm.com/linear-swap-api/v1/swap_contract_info',
      ],
    }

    this.url = (pair) => {
      if (this.types[pair] === 'futures') {
        return 'wss://www.hbdm.com/ws'
      } else if (this.types[pair] === 'swap') {
        return 'wss://api.hbdm.com/swap-ws'
      } else if (this.types[pair] === 'linear') {
        return 'wss://api.hbdm.com/linear-swap-ws'
      } else {
        return 'wss://api.huobi.pro/ws'
      }
    }
  }

  formatProducts(response) {
    const products = []
    const specs = {}
    const types = {}

    for (let data of response) {
      let type = ['spot', 'futures', 'swap', 'linear'][response.indexOf(data)]

      for (let product of data.data) {
        let pair

        switch (type) {
          case 'spot':
            pair = (product['base-currency'] + product['quote-currency']).toLowerCase()
            break
          case 'futures':
            pair = product.symbol + '_' + this.contractTypesAliases[product.contract_type]
            specs[pair] = product.contract_size
            break
          case 'swap':
          case 'linear':
            pair = product.contract_code
            specs[pair] = product.contract_size
            break
        }

        types[pair] = type

        products.push(pair)
      }
    }

    return {
      products,
      specs,
      types,
    }
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async subscribe(api, pair) {
    if (!(await super.subscribe.apply(this, arguments))) {
      return
    }

    this.receivedInitialData[pair] = false

    api.send(
      JSON.stringify({
        sub: 'market.' + pair + '.trade.detail',
        id: pair,
      })
    )

    this.subscribeLiquidations(api, pair)
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    api.send(
      JSON.stringify({
        unsub: 'market.' + pair + '.trade.detail',
        id: pair,
      })
    )
  }

  onMessage(event, api) {
    const json = JSON.parse(pako.inflate(event.data, { to: 'string' }))

    if (!json) {
      return
    }

    if (json.ping) {
      api.send(JSON.stringify({ pong: json.ping }))
      return
    } else if (json.tick && json.tick.data && json.tick.data.length) {
      const pair = json.ch.replace(/market.(.*).trade.detail/, '$1')

      if (!this.receivedInitialData[api.id + pair]) {
        // huobi send a bunch of trades at the beginning of the feed that has nothing to do with the realtime data we are looking for
        this.receivedInitialData[api.id + pair] = true

        // ignore them
        return
      }

      this.emitTrades(
        api.id,
        json.tick.data.map((trade) => this.formatTrade(trade, pair))
      )

      return true
    }
  }

  formatTrade(trade, pair) {
    let size = +trade.amount

    if (typeof this.specs[pair] === 'number') {
      size = (size * this.specs[pair]) / (this.types[pair] === 'linear' ? 1 : trade.price)
    }

    this.prices[pair] = trade.price // used for liquidation amount in quote currency

    return {
      exchange: this.id,
      pair: pair,
      timestamp: trade.ts,
      price: +trade.price,
      size: size,
      side: trade.direction,
    }
  }

  formatLiquidation(trade, pair) {
    return {
      exchange: this.id,
      pair: pair,
      timestamp: +new Date(),
      price: this.prices[pair] || trade.price,
      size: +trade.amount,
      side: trade.direction,
      liquidation: true,
    }
  }

  openMarketDataApi(api) {
    if (api.url === 'wss://api.hbdm.com/swap-ws') {
      api._marketDataApi = new WebSocket('wss://api.hbdm.com/swap-notification') // coin margined
    } else if (api.url === 'wss://api.hbdm.com/linear-swap-ws') {
      api._marketDataApi = new WebSocket('wss://api.hbdm.com/linear-swap-notification') // usdt margined
    }

    if (api._marketDataApi) {
      // coin/linear swap
      console.log(`[${this.id}] opened market data api ${api._marketDataApi.url}`)
      api._marketDataApi.onmessage = (event) => {
        const json = JSON.parse(pako.inflate(event.data, { to: 'string' }))

        if (json.op === 'ping' && api._marketDataApi.readyState === WebSocket.OPEN) {
          api._marketDataApi.send(JSON.stringify({ op: 'pong', ts: json.ts }))
        } else if (json.data) {
          const pair = json.topic.replace(/public.(.*).liquidation_orders/, '$1')

          this.emitTrades(
            api.id,
            json.data.map((trade) => this.formatLiquidation(trade, pair))
          )
        }
      }

      api._marketDataApi.onopen = () => {
        for (const pair of api._connected) {
          this.subscribeLiquidations(api, pair)
        }
      }

      api._marketDataApi.onerror = (event) => {
        console.error(`[${this.id}] market data api errored ${event.message}`)
      }

      api._marketDataApi.onclose = (event) => {
        if (!event.wasClean) {
          console.error(`[${this.id}] market data api closed unexpectedly`)
        }
        api._marketDataApi = null

        setTimeout(() => {
          if (api.readyState === WebSocket.OPEN) {
            this.openMarketDataApi(api)
          }
        }, 1000)
      }
    }
  }

  async getMissingTrades(range, totalRecovered = 0) {
    let endpoint

    if (this.types[range.pair] === 'futures') {
      // https://api.hbdm.com/market/history/trade?symbol=BTC_CQ&size=2000
      endpoint = `https://api.hbdm.com/market/history/trade?symbol=${range.pair}&size=2000`
    } else if (this.types[range.pair] === 'swap') {
      // https://api.hbdm.com/swap-ex/market/history/trade?contract_code=BTC-USD&size=2000
      endpoint = `https://api.hbdm.com/swap-ex/market/history/trade?contract_code=${range.pair}&size=2000`
    } else if (this.types[range.pair] === 'linear') {
      // https://api.hbdm.com/linear-swap-ex/market/history/trade?contract_code=BTC-USDT&size=2000
      endpoint = `https://api.hbdm.com/linear-swap-ex/market/history/trade?contract_code=${range.pair}&size=2000`
    } else {
      // https://api.huobi.pro/market/history/trade?symbol=btcusdt&size=2000
      endpoint = `https://api.huobi.pro/market/history/trade?symbol=${range.pair}&size=2000`
    }

    return axios
      .get(endpoint)
      .then((response) => {
        if (response.data.data.length) {
          const trades = response.data.data
            .reduce((acc, batch) => {
              return acc.concat(batch.data)
            }, [])
            .map((trade) => this.formatTrade(trade, range.pair))
            .filter((a) => a.timestamp >= range.from + 1 && a.timestamp < range.to)

          if (trades.length) {
            this.emitTrades(null, trades)

            totalRecovered += trades.length
            range.from = trades[trades.length - 1].timestamp
          }

          const remainingMissingTime = range.to - range.from

          console.log(`[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} (${getHms(remainingMissingTime)} remaining)`)
        }

        return totalRecovered
      })
      .catch((err) => {
        console.error(`[${this.id}] failed to get missing trades on ${range.pair}`, err.message)

        return totalRecovered
      })
  }

  subscribeLiquidations(api, pair, unsubscribe = false) {
    if (
      api._marketDataApi &&
      api._marketDataApi.readyState === WebSocket.OPEN &&
      (this.types[pair] === 'futures' || this.types[pair] === 'swap' || this.types[pair] === 'linear')
    ) {
      const symbol = this.types[pair] === 'futures' ? pair.replace(/\d+/, '').replace(/-.*/, '') : pair

      api._marketDataApi.send(
        JSON.stringify({
          op: unsubscribe ? 'unsub' : 'sub',
          topic: 'public.' + symbol + '.liquidation_orders',
        })
      )
    }
  }

  onApiCreated(api) {
    this.liquidationOrdersSubscriptions[api.id] = []

    // open api providing liquidations data
    this.openMarketDataApi(api)
  }

  onApiRemoved(api) {
    if (api._marketDataApi) {
      if (api._marketDataApi.readyState === WebSocket.OPEN) {
        api._marketDataApi.close()
      }
    }
  }
}

module.exports = Huobi
