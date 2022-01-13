const e = require('express')
const Exchange = require('../exchange')
const { sleep, getHms } = require('../helper')
const axios = require('axios')

class BinanceFutures extends Exchange {
  constructor(options) {
    super(options)

    this.id = 'BINANCE_FUTURES'
    this.lastSubscriptionId = 0
    this.subscriptions = {}
    
    this.maxConnectionsPerApi = 100
    this.endpoints = {
      PRODUCTS: ['https://fapi.binance.com/fapi/v1/exchangeInfo', 'https://dapi.binance.com/dapi/v1/exchangeInfo'],
    }

    this.options = Object.assign(
      {
        url: (pair) => {
          if (this.dapi[pair]) {
            return 'wss://dstream.binance.com/ws'
          } else {
            return 'wss://fstream.binance.com/ws'
          }
        },
      },
      this.options
    )
  }

  formatProducts(response) {
    const products = []
    const specs = {}
    const dapi = {}

    for (const data of response) {
      const type = ['fapi', 'dapi'][response.indexOf(data)]

      for (const product of data.symbols) {
        if ((product.contractStatus && product.contractStatus !== 'TRADING') || (product.status && product.status !== 'TRADING')) {
          continue
        }

        const symbol = product.symbol.toLowerCase()

        if (type === 'dapi') {
          dapi[symbol] = true
        }

        if (product.contractSize) {
          specs[symbol] = product.contractSize
        }

        products.push(symbol)
      }
    }

    return {
      products,
      specs,
      dapi,
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

    this.subscriptions[pair] = ++this.lastSubscriptionId

    const params = [pair + '@trade', pair + '@forceOrder']

    api.send(
      JSON.stringify({
        method: 'SUBSCRIBE',
        params,
        id: this.subscriptions[pair],
      })
    )

    // BINANCE: WebSocket connections have a limit of 10 incoming messages per second.
    await sleep(101)
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

    const params = [pair + '@trade', pair + '@forceOrder']

    api.send(
      JSON.stringify({
        method: 'UNSUBSCRIBE',
        params,
        id: this.subscriptions[pair],
      })
    )

    delete this.subscriptions[pair]

    // BINANCE: WebSocket connections have a limit of 10 incoming messages per second.
    await sleep(101)
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json) {
      return
    } else if (json.e === 'trade' && json.X !== 'INSURANCE_FUND') {
      return this.emitTrades(api.id, [this.formatTrade(json, json.s.toLowerCase())])
    } else if (json.e === 'forceOrder') {
      return this.emitLiquidations(api.id, [this.formatLiquidation(json)])
    }
  }

  formatTrade(trade, symbol) {
    let size = +trade.q

    if (typeof this.specs[symbol] === 'number') {
      size = (size * this.specs[symbol]) / trade.p
    }

    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.T,
      price: +trade.p,
      size: size,
      side: trade.m ? 'sell' : 'buy',
    }
  }

  formatLiquidation(trade) {
    let size = +trade.q

    const symbol = trade.o.s.toLowerCase()

    if (typeof this.specs[symbol] === 'number') {
      size = (size * this.specs[symbol]) / trade.p
    }

    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.o.T,
      price: +trade.o.p,
      size: size,
      side: trade.o.S === 'BUY' ? 'buy' : 'sell',
      liquidation: true,
    }
  }

  getMissingTrades(pair, timestamps, endTime, totalRecovered = 0) {
    const startTime = timestamps[pair]
    let endpoint = `?symbol=${pair.toUpperCase()}&startTime=${startTime + 1}&endTime=${endTime}&limit=1000`

    if (this.dapi[pair]) {
      endpoint = 'https://dapi.binance.com/dapi/v1/aggTrades' + endpoint
    } else {
      endpoint = 'https://fapi.binance.com/fapi/v1/aggTrades' + endpoint
    }
    
    return axios
      .get(endpoint)
      .then((response) => {
        if (response.data.length) {
          const trades = response.data.map((trade) => ({
            ...this.formatTrade(trade, pair),
            count: trade.l - trade.f + 1,
            recovered: true
          }))
  
          this.emitTrades(null, trades)

          totalRecovered += trades.length
          timestamps[pair] = trades[trades.length - 1].timestamp

          if (response.data.length === 1000) {
            const remainingMissingTime = endTime - timestamps[pair]

            if (remainingMissingTime > 1000 * 60) {
              console.info(`[${this.id}] try again (${getHms(remainingMissingTime)} remaining)`)
              return this.getMissingTrades(pair, timestamps[pair], endTime)
            }
          }
        }

        return totalRecovered
      })
      .catch((err) => {
        console.error(`[${this.id}] failed to get missing trades`, err)
      })
  }

  onApiCreated(api) {
    // A single connection is only valid for 24 hours; expect to be disconnected at the 24 hour mark
    api._reconnectionTimeout = setTimeout(() => {
      api._reconnectionTimeout = null
      console.log(`[exchange/${this.id}] reconnect API`)
      this.reconnectApi(api)
    }, 1000 * 60 * 60 * 24)
  }

  onApiRemoved(api) {
    if (api._reconnectionTimeout) {
      clearTimeout(api._reconnectionTimeout)
    }
  }
}

module.exports = BinanceFutures
