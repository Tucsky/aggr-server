const Exchange = require('../exchange')

class Coinbase extends Exchange {
  constructor(options) {
    super(options)

    this.id = 'COINBASE'

    this.endpoints = {
      PRODUCTS: 'https://api.pro.coinbase.com/products',
    }

    this.options = Object.assign(
      {
        url: 'wss://ws-feed.pro.coinbase.com',
      },
      this.options
    )
  }

  formatProducts(data) {
    return data.map((product) => product.id)
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

    api.send(
      JSON.stringify({
        type: 'subscribe',
        channels: [{ name: 'matches', product_ids: [pair] }],
      })
    )
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
        type: 'unsubscribe',
        channels: [{ name: 'matches', product_ids: [pair] }],
      })
    )
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json && json.size > 0) {
      return this.emitTrades(api.id, [
        {
          exchange: this.id,
          pair: json.product_id,
          timestamp: +new Date(json.time),
          price: +json.price,
          size: +json.size,
          side: json.side === 'buy' ? 'sell' : 'buy',
        },
      ])
    }
  }
}

module.exports = Coinbase
