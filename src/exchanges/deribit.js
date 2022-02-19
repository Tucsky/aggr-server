const axios = require('axios')
const config = require('../config')
const Exchange = require('../exchange')

class Deribit extends Exchange {
  constructor() {
    super()

    this.id = 'DERIBIT'

    this.endpoints = {
      PRODUCTS: 'https://www.deribit.com/api/v1/public/getinstruments',
    }

    this.url = `wss://www.deribit.com/ws/api/v2`
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

    if (api._connected.length === 1) {
      if (!config.deribitClientId) {
        throw new Error('As of 15 Jan 2022 Deribit will no longer allow unauthenticated connections to subscribe to raw feeds\n\nAdd deribitClientId & deribitClientSecret to the config and restart server')
      }
      
      api.send(
        JSON.stringify({
          jsonrpc: '2.0',
          method: 'public/auth',
          params: {
            grant_type: 'client_credentials',
            client_id: config.deribitClientId,
            client_secret: config.deribitClientSecret,
          },
        })
      )
    }

    api.send(
      JSON.stringify({
        access_token: this.accessToken,
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
        access_token: this.accessToken,
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
