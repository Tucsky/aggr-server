const Exchange = require('../exchange')

class Deribit extends Exchange {
  constructor(options) {
    super(options)

    this.id = 'DERIBIT'

    this.endpoints = {
      PRODUCTS: 'https://www.deribit.com/api/v1/public/getinstruments',
    }

    this.options = Object.assign(
      {
        url: () => {
          return `wss://www.deribit.com/ws/api/v2`
        },
      },
      this.options
    )
  }

  formatProducts(data) {
    return data.result.map((product) => product.instrumentName)
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
        method: 'public/subscribe',
        params: {
          channels: ['trades.' + pair + '.raw'],
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
        method: 'public/unsubscribe',
        params: {
          channels: ['trades.' + pair + '.raw'],
        },
      })
    )
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json || !json.params || !json.params.data || !json.params.data.length) {
      return
    }

    return this.emitTrades(
      api.id,
      json.params.data.map((a) => {
        const trade = {
          exchange: this.id,
          pair: a.instrument_name,
          timestamp: +a.timestamp,
          price: +a.price,
          size: a.amount / a.price,
          side: a.direction,
        }

        if (a.liquidation) {
          trade.liquidation = true
        }

        return trade
      })
    )
  }

  onApiCreated(api) {
    this.startKeepAlive(api, { method: 'public/ping' }, 45000)
  }

  onApiRemoved(api) {
    this.stopKeepAlive(api)
  }
}

module.exports = Deribit
