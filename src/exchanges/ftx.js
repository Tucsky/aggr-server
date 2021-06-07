const Exchange = require('../exchange')
const WebSocket = require('ws')

class Ftx extends Exchange {
  constructor(options) {
    super(options)

    this.id = 'FTX'

    this.endpoints = {
      PRODUCTS: 'https://ftx.com/api/markets',
    }

    this.options = Object.assign(
      {
        url: () => {
          return `wss://ftx.com/ws/`
        },
      },
      this.options
    )
  }

  formatProducts(data) {
    return data.result.map((product) => product.name)
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
        op: 'subscribe',
        channel: 'trades',
        market: pair,
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
        op: 'unsubscribe',
        channel: 'trades',
        market: pair,
      })
    )
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json || !json.data || !json.data.length) {
      return
    }

    return this.emitTrades(
      api.id,
      json.data.map((trade) => {
        const output = {
          exchange: this.id,
          pair: json.market,
          timestamp: +new Date(trade.time),
          price: +trade.price,
          size: trade.size,
          side: trade.side,
        }

        if (trade.liquidation) {
          output.liquidation = true
        }

        return output
      })
    )
  }

  onApiCreated(api) {
    this.startKeepAlive(api, { op: 'ping' }, 15000)
  }

  onApiRemoved(api) {
    this.stopKeepAlive(api)
  }
}

module.exports = Ftx
