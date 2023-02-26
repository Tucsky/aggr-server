const Exchange = require('../exchange')
const WebSocket = require('websocket').w3cwebsocket
const axios = require('axios')

const KUCOIN_TOKEN_EXPIRATION = 1000 * 60 * 5

class Kucoin extends Exchange {
  constructor() {
    super()

    this.id = 'KUCOIN'

    this.promiseOfToken = null
    this.token = null

    this.endpoints = {
      PRODUCTS: [
        'https://api.kucoin.com/api/v1/symbols',
        'https://api-futures.kucoin.com/api/v1/contracts/active'
      ]
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

  formatProducts(responses) {
    const products = []
    const multipliers = {}

    for (const response of responses) {
      const type = ['spot', 'futures'][responses.indexOf(response)]
      for (const product of response.data) {
        const symbol = product.symbolName || product.symbol

        products.push(product.symbol)

        if (type === 'futures') {
          multipliers[symbol] = product.multiplier
        }
      }
    }

    return {
      products,
      multipliers
    }
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

    let topic

    if (this.multipliers[pair]) {
      topic = `/contractMarket/execution:${pair}`
    } else {
      topic = `/market/match:${pair}`
    }

    api.send(
      JSON.stringify({
        type: 'subscribe',
        topic: topic,
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

    let topic

    if (this.multipliers[pair]) {
      topic = `/contractMarket/execution:${pair}`
    } else {
      topic = `/market/match:${pair}`
    }

    api.send(
      JSON.stringify({
        type: 'unsubscribe',
        topic
      })
    )
  }

  formatTrade(trade) {
    let timestamp
    let size

    if (this.multipliers[trade.symbol]) {
      timestamp = trade.ts / 1000000
      if (this.multipliers[trade.symbol] < 0) {
        size = trade.size / trade.price
      } else {
        size = trade.size * this.multipliers[trade.symbol]
      }
    } else {
      timestamp = trade.time / 1000000
      size = +trade.size
    }

    return {
      exchange: this.id,
      pair: trade.symbol,
      price: +trade.price,
      side: trade.side === 'buy' ? 'buy' : 'sell',
      timestamp,
      size
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
