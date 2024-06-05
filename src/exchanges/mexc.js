const Exchange = require('../exchange')
const { sleep } = require('../helper')
const WebSocket = require('websocket').w3cwebsocket

class Mexc extends Exchange {
  constructor() {
    super()

    this.id = 'MEXC'
    this.maxConnectionsPerApi = 50

    this.endpoints = {
      PRODUCTS: [
        'https://api.mexc.com/api/v3/exchangeInfo',
        'https://contract.mexc.com/api/v1/contract/detail'
      ]
    }

    this.url = pair => {
      if (typeof this.contractSizes[pair] === 'number') {
        return 'wss://contract.mexc.com/edge'
      }

      return 'wss://wbs.mexc.com/ws'
    }
  }

  formatProducts(responses) {
    const products = []
    const contractSizes = {}
    const inversed = {}
    const [spot, perp] = responses

    if (spot) {
      for (const product of spot.symbols) {
        products.push(product.symbol)
      }
    }

    if (perp) {
      for (const product of perp.data) {
        products.push(product.symbol)
        contractSizes[product.symbol] = product.contractSize
        inversed[product.symbol] = product.quoteCoin === product.settleCoin
      }
    }

    return {
      products,
      contractSizes,
      inversed
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

    if (typeof this.contractSizes[pair] === 'number') {
      api.send(
        JSON.stringify({
          method: 'sub.deal',
          param: {
            symbol: pair
          }
        })
      )
    } else {
      api.send(
        JSON.stringify({
          method: 'SUBSCRIPTION',
          params: [`spot@public.deals.v3.api@${pair}`]
        })
      )
    }

    // this websocket api have a limit of about 10 messages per second.
    await sleep(25 * this.apis.length)
  }

  /**
   * Unsub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    if (typeof this.contractSizes[pair] !== 'undefined') {
      api.send(
        JSON.stringify({
          method: 'unsub.deal',
          param: {
            symbol: pair
          }
        })
      )
    } else {
      api.send(
        JSON.stringify({
          method: 'UNSUBSCRIPTION',
          params: [`spot@public.deals.v3.api@${pair}`]
        })
      )
    }

    await sleep(25 * this.apis.length)
  }

  formatSpotTrade(trade, pair) {
    return {
      exchange: this.id,
      pair: pair,
      timestamp: trade.t,
      price: +trade.p,
      size: trade.v,
      side: trade.S === 1 ? 'buy' : 'sell'
    }
  }

  formatContractTrade(trade, pair) {
    return {
      exchange: this.id,
      pair: pair,
      timestamp: trade.t,
      price: trade.p,
      size:
        (trade.v * this.contractSizes[pair]) /
        (this.inversed[pair] ? 1 : trade.p),
      side: trade.T === 1 ? 'buy' : 'sell'
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json.channel === 'push.deal') {
      return this.emitTrades(api.id, [
        this.formatContractTrade(json.data, json.symbol)
      ])
    } else if (json.d && json.d.e === 'spot@public.deals.v3.api') {
      return this.emitTrades(
        api.id,
        json.d.deals.map(trade => this.formatSpotTrade(trade, json.s))
      )
    }
  }

  onApiCreated(api) {
    this.startKeepAlive(
      api,
      {
        method: /contract/.test(api.url) ? 'ping' : 'PING'
      },
      15000
    )
  }

  onApiRemoved(api) {
    this.stopKeepAlive(api)
  }
}

module.exports = Mexc
