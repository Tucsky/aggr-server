const Exchange = require('../exchange')
const WebSocket = require('websocket').w3cwebsocket

class HitBtc extends Exchange {
  constructor() {
    super()

    this.id = 'HITBTC'

    this.endpoints = {
      PRODUCTS: 'https://api.hitbtc.com/api/2/public/symbol',
    }

    this.url = 'wss://api.hitbtc.com/api/2/ws'
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
        method: 'subscribeTrades',
        params: {
          symbol: pair,
        },
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
        method: 'unsubscribeTrades',
        params: {
          symbol: pair,
        },
      })
    )
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (
      !json ||
      json.method !== 'updateTrades' ||
      !json.params ||
      !json.params.data ||
      !json.params.data.length
    ) {
      return
    }

    return this.emitTrades(
      api.id,
      json.params.data.map((trade) => ({
        exchange: this.id,
        pair: json.params.symbol,
        timestamp: +new Date(trade.timestamp),
        price: +trade.price,
        size: +trade.quantity,
        side: trade.side,
      }))
    )
  }
}

module.exports = HitBtc
