const Exchange = require('../exchange')
const { sleep, getHms } = require('../helper')
const axios = require('axios')

class Binance extends Exchange {
  constructor() {
    super()

    this.id = 'BINANCE'
    this.lastSubscriptionId = 0
    this.subscriptions = {}

    this.endpoints = {
      PRODUCTS: 'https://api.binance.com/api/v1/ticker/allPrices',
    }

    this.url = () => `wss://stream.binance.com:9443/ws`
  }

  formatProducts(data) {
    return data.map((product) => product.symbol.toLowerCase())
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

    const params = [pair + '@trade']

    api.send(
      JSON.stringify({
        method: 'SUBSCRIBE',
        params,
        id: this.subscriptions[pair],
      })
    )

    // this websocket api have a limit of about 5 messages per second.
    await sleep(250 * this.apis.length)
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

    const params = [pair + '@trade']

    api.send(
      JSON.stringify({
        method: 'UNSUBSCRIBE',
        params,
        id: this.subscriptions[pair],
      })
    )

    delete this.subscriptions[pair]

    // this websocket api have a limit of about 5 messages per second.
    await sleep(250 * this.apis.length)
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json.E) {
      return this.emitTrades(api.id, [this.formatTrade(json, json.s.toLowerCase())])
    }
  }

  formatTrade(trade, symbol) {
    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.E,
      price: +trade.p,
      size: +trade.q,
      side: trade.m ? 'sell' : 'buy',
    }
  }

  getMissingTrades(range, totalRecovered = 0) {
    const startTime = range.from
    const below1HEndTime = Math.min(range.to, startTime + 1000 * 60 * 60)

    const endpoint = `https://api.binance.com/api/v3/aggTrades?symbol=${range.pair.toUpperCase()}&startTime=${
      startTime + 1
    }&endTime=${below1HEndTime}&limit=1000`

    return axios
      .get(endpoint)
      .then((response) => {
        if (response.data.length) {
          const trades = response.data.map((trade) => ({
            ...this.formatTrade(trade, range.pair),
            count: trade.l - trade.f + 1,
            timestamp: trade.T,
          }))

          this.emitTrades(null, trades)

          totalRecovered += trades.length
          range.from = trades[trades.length - 1].timestamp

          const remainingMissingTime = range.to - range.from

          if (remainingMissingTime > 1000) {
            console.log(`[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} ... (${getHms(remainingMissingTime)} remaining)`)
            return this.waitBeforeContinueRecovery().then(() => this.getMissingTrades(range, totalRecovered))
          } else {
            console.log(`[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} (${getHms(remainingMissingTime)} remaining)`)
          }
        }

        return totalRecovered
      })
      .catch((err) => {
        console.error(`Failed to get historical trades on ${range.pair}`, err.message)
      })
  }
}

module.exports = Binance
