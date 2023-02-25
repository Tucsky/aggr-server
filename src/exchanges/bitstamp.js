const Exchange = require('../exchange')
const WebSocket = require('websocket').w3cwebsocket

class Bitstamp extends Exchange {
  constructor() {
    super()

    this.id = 'BITSTAMP'

    this.endpoints = {
      PRODUCTS: 'https://www.bitstamp.net/api/v2/trading-pairs-info',
    }

    this.url = () => {
          return `wss://ws.bitstamp.net`
        };
  }

  formatProducts(data) {
    return data.map((a) => a.url_symbol)
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
        event: 'bts:subscribe',
        data: {
          channel: 'live_trades_' + pair,
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
        event: 'bts:unsubscribe',
        data: {
          channel: 'live_trades_' + pair,
        },
      })
    )
  }

  onMessage(event, api) {
    // channel:"live_trades_btcusd"
    const json = JSON.parse(event.data)

    if (!json || !json.data || !json.data.amount) {
      return
    }

    const trade = json.data

    return this.emitTrades(api.id, [
      {
        exchange: this.id,
        pair: json.channel.split('_').pop(),
        timestamp: +new Date(trade.microtimestamp / 1000),
        price: trade.price,
        size: trade.amount,
        side: trade.type === 0 ? 'buy' : 'sell',
      },
    ])
  }
}

module.exports = Bitstamp
