const e = require('express')
const Exchange = require('../exchange')
const { sleep, getHms } = require('../helper')
const axios = require('axios')
const { debugReportedTrades } = require('../services/connections')

class BinanceFutures extends Exchange {
  constructor() {
    super()

    this.id = 'BINANCE_FUTURES'
    this.lastSubscriptionId = 0
    this.subscriptions = {}

    this.maxConnectionsPerApi = 100
    this.endpoints = {
      PRODUCTS: ['https://fapi.binance.com/fapi/v1/exchangeInfo', 'https://dapi.binance.com/dapi/v1/exchangeInfo'],
    }

    this.url = (pair) => {
      if (this.dapi[pair]) {
        return 'wss://dstream.binance.com/ws'
      } else {
        return 'wss://fstream.binance.com/ws'
      }
    }
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
    await sleep(250)
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

  getSize(qty, price, symbol) {
    let size = +qty

    if (typeof this.specs[symbol] === 'number') {
      size = (size * this.specs[symbol]) / price
    }

    return size
  }

  formatTrade(trade, symbol) {
    if (symbol === 'btcusdt' && debugReportedTrades.btcusdt) {
      if (!this._lastTimestamp || trade.T - this._lastTimestamp > 10) {
        console.log(trade.T, this._lastTimestampCount + 1)
        this._lastTimestampCount = 0
      }
      this._lastTimestamp = trade.T
      this._lastTimestampCount++
    }

    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.T,
      price: +trade.p,
      size: this.getSize(trade.q, trade.p, symbol),
      side: trade.m ? 'sell' : 'buy',
    }
  }

  formatLiquidation(trade) {
    const symbol = trade.o.s.toLowerCase()

    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.o.T,
      price: +trade.o.p,
      size: this.getSize(trade.o.q, trade.o.p, symbol),
      side: trade.o.S === 'BUY' ? 'buy' : 'sell',
      liquidation: true,
    }
  }

  getMissingTrades(range, totalRecovered = 0) {
    const startTime = range.from
    let endpoint = `?symbol=${range.pair.toUpperCase()}&startTime=${startTime + 1}&endTime=${range.to}&limit=1000`

    if (this.dapi[range.pair]) {
      endpoint = 'https://dapi.binance.com/dapi/v1/aggTrades' + endpoint
    } else {
      endpoint = 'https://fapi.binance.com/fapi/v1/aggTrades' + endpoint
    }

    return axios
      .get(endpoint)
      .then((response) => {
        if (response.data.length) {
          const trades = response.data.map((trade) => ({
            ...this.formatTrade(trade, range.pair),
            count: trade.l - trade.f + 1,
          }))

          this.emitTrades(null, trades)

          totalRecovered += trades.length
          range.from = trades[trades.length - 1].timestamp

          const remainingMissingTime = range.to - range.from

          if (remainingMissingTime > 1000) {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} ... but theres more (${getHms(
                remainingMissingTime
              )} remaining)`
            )
            return this.getMissingTrades(range, totalRecovered)
          } else {
            console.log(`[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} (${getHms(remainingMissingTime)} remaining)`)
          }
        }

        return totalRecovered
      })
      .catch((err) => {
        console.error(`[${this.id}] failed to get missing trades on ${range.pair}`, err.message)

        return totalRecovered
      })
  }
}

module.exports = BinanceFutures
