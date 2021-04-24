const Exchange = require('../exchange')

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
  subscribe(api, pair) {
    if (!super.subscribe.apply(this, arguments)) {
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
    return new Promise((resolve) => setTimeout(resolve, 250))
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  unsubscribe(api, pair) {
    if (!super.unsubscribe.apply(this, arguments)) {
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
      return this.emitTrades(api.id, [
        {
          exchange: this.id,
          pair: json.s.toLowerCase(),
          timestamp: json.E,
          price: +json.p,
          size: +json.q,
          side: json.m ? 'sell' : 'buy',
        },
      ])
    }
  }
}

module.exports = Binance
