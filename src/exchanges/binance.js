const Exchange = require('../exchange')
const { sleep, getHms } = require('../helper')
const axios = require('axios')

class Binance extends Exchange {
  constructor(options) {
    super(options)

    this.id = 'BINANCE'
    this.lastSubscriptionId = 0
    this.subscriptions = {}

    this.endpoints = {
      PRODUCTS: 'https://api.binance.com/api/v1/ticker/allPrices',
    }

    this.options = Object.assign(
      {
        url: () => `wss://stream.binance.com:9443/ws`,
      },
      this.options
    )
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

    // BINANCE: WebSocket connections have a limit of 5 incoming messages per second.
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

    const params = [pair + '@trade']

    api.send(
      JSON.stringify({
        method: 'UNSUBSCRIBE',
        params,
        id: this.subscriptions[pair],
      })
    )

    delete this.subscriptions[pair]

    // BINANCE: WebSocket connections have a limit of 5 incoming messages per second.
    return new Promise((resolve) => setTimeout(resolve, 250))
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

  getMissingTrades(pair, timestamps, endTime, totalRecovered = 0) {
    const startTime = timestamps[pair]
    const endpoint = `https://api.binance.com/api/v3/aggTrades?symbol=${pair.toUpperCase()}&startTime=${
      startTime + 1
    }&endTime=${endTime}&limit=1000`
    
    return axios
      .get(endpoint)
      .then((response) => {
        if (response.data.length) {
          const trades = response.data.map((trade) => ({
            ...this.formatTrade(trade, pair),
            timestamp: trade.T,
            count: trade.l - trade.f + 1,
          }))

          this.emitTrades(null, trades)

          totalRecovered += trades.length
          timestamps[pair] = trades[trades.length - 1].timestamp + 1

          if (response.data.length === 1000) {
            // we assume there is more since 1k trades is max returned by the api

            const remainingMissingTime = endTime - timestamps[pair]

            if (remainingMissingTime > 1000 * 60) {
              console.info(`[${this.id}] try again (${getHms(remainingMissingTime)} remaining)`)
              return this.getMissingTrades(pair, timestamps, endTime, totalRecovered)
            }
          }
        }

        return totalRecovered
      })
      .catch((err) => {
        console.error(`Failed to get historical trades`, err)
      })
  }
}

module.exports = Binance
