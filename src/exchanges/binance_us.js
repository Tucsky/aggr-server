const Exchange = require('../exchange')
const { sleep } = require('../helper')

class BinanceUs extends Exchange {
  constructor() {
    super()

    this.id = 'BINANCE_US'
    this.lastSubscriptionId = 0
    this.subscriptions = {}

    this.endpoints = {
      PRODUCTS: 'https://api.binance.us/api/v3/exchangeInfo',
    }

    this.url = () => `wss://stream.binance.us:9443/ws`
  }

  formatProducts(data) {
    return data.symbols.filter((product) => product.status === 'TRADING').map((product) => product.symbol.toLowerCase())
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

    // this websocket api have a limit of about 5 messages per second.
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

module.exports = BinanceUs
