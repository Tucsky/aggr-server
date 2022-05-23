const Exchange = require('../exchange')
const WebSocket = require('ws')
const axios = require('axios')

class Kucoin extends Exchange {
  constructor() {
    super()

    this.id = 'KUCOIN'

    this.endpoints = {
      PRODUCTS: 'https://api.kucoin.com/api/v1/symbols',
    }

    this.url = () => {
      if (this.endpoints.WS) {
        return this.endpoints.WS
      }

      return axios.post('https://api.kucoin.com/api/v1/bullet-public').then(({data})=>{
        console.log(data)

        this.endpoints.WS = 'wss://ws-api.kucoin.com/endpoint'

        if (data.data.instanceServers.length) {
          this.endpoints.WS = data.data.instanceServers[0].endpoint
        }

        this.endpoints.WS += '?token=' + data.data.token

        return this.endpoints.WS
      }).catch(err=>{
        console.log(err)
      })
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
}

module.exports = Kucoin
