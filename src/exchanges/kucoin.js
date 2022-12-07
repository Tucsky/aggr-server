const Exchange = require('../exchange')
const WebSocket = require('ws')
const axios = require('axios')

const KUCOIN_TOKEN_EXPIRATION = 1000 * 60 * 5

class Kucoin extends Exchange {
  constructor() {
    super()

    this.id = 'KUCOIN'

    this.promiseOfToken = null
    this.token = null

    this.endpoints = {
      PRODUCTS: 'https://api.kucoin.com/api/v1/symbols',
    }

    this.url = () => {
      if (this.promiseOfToken) {
        return this.promiseOfToken
      }

      const timestamp = Date.now()
      
      if (this.token && this.token.timestamp + KUCOIN_TOKEN_EXPIRATION > timestamp) {
        return this.token.value
      }

      this.promiseOfToken = axios.post('https://api.kucoin.com/api/v1/bullet-public').then(({data})=>{
        this.endpoints.WS = 'wss://ws-api.kucoin.com/endpoint'

        this.token = {
          timestamp,
          value: data.data.instanceServers[0].endpoint + '?token=' + data.data.token
        }

        return this.token.value
      }).catch(err => {
        console.error(`[${this.id}] failed to get token`, err)
      }).finally(() => {
        this.promiseOfToken = null
      })

      return this.promiseOfToken
    }
  }

  formatProducts(response) {
    return response.data.map((a) => a.symbol)
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
        topic: '/market/match:' + pair,
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
        topic: '/market/match:' + pair,
      })
    )
  }

  formatTrade(trade) {
    return {
      exchange: this.id,
      pair: trade.symbol,
      timestamp: trade.time / 1000000,
      price: +trade.price,
      size: +trade.size,
      side: trade.side === 'buy' ? 'buy' : 'sell',
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json || !json.data) {
      return
    }

    return this.emitTrades(
      api.id,
      [this.formatTrade(json.data)]
    )
  }

  onApiCreated(api) {
    this.startKeepAlive(api, { type: 'ping' }, 18000)
  }

  onApiRemoved(api) {
    this.stopKeepAlive(api)
  }
}

module.exports = Kucoin
