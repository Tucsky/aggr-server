const Exchange = require('../exchange')
const { sleep } = require('../helper')
const WebSocket = require('websocket').w3cwebsocket

const SPOT_PAIR_REGEX = /-SPOT$/

class Bitget extends Exchange {
  constructor() {
    super()

    this.id = 'BITGET'
    this.maxConnectionsPerApi = 50

    this.endpoints = {
      PRODUCTS: [
        'https://api.bitget.com/api/v2/spot/public/symbols',
        'https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES',
        'https://api.bitget.com/api/v2/mix/market/contracts?productType=COIN-FUTURES',
        'https://api.bitget.com/api/v2/mix/market/contracts?productType=USDC-FUTURES'
      ]
    }

    this.url = pair => {
      // V2 API uses a single WebSocket URL for both spot and futures
      return 'wss://ws.bitget.com/v2/ws/public'
    }
  }

  formatProducts(responses) {
    const products = []
    const types = {}

    /*
    V2 API product types:
    spot - Spot trading (will add -SPOT suffix)
    USDT-FUTURES - USDT-M Futures
    COIN-FUTURES - Coin-M Futures
    USDC-FUTURES - USDC-M Futures
    */

    for (const response of responses) {
      const type = ['spot', 'USDT-FUTURES', 'COIN-FUTURES', 'USDC-FUTURES'][
        responses.indexOf(response)
      ]

      for (const product of response.data) {
        // Add -SPOT suffix for spot pairs to avoid conflicts with futures
        const symbol = type === 'spot' ? `${product.symbol}-SPOT` : product.symbol

        products.push(symbol)

        types[symbol] = type
      }
    }

    return {
      products,
      types
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

    const isSpot = this.types[pair] === 'spot'
    const realPair = isSpot ? pair.replace(SPOT_PAIR_REGEX, '') : pair
    const instType = isSpot ? 'SPOT' : this.types[pair]

    api.send(
      JSON.stringify({
        op: 'subscribe',
        args: [
          {
            instType: instType,
            channel: 'trade',
            instId: realPair
          }
        ]
      })
    )

    // this websocket api have a limit of about 10 messages per second.
    await sleep(150 * this.apis.length)
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

    const isSpot = this.types[pair] === 'spot'
    const realPair = isSpot ? pair.replace(SPOT_PAIR_REGEX, '') : pair
    const instType = isSpot ? 'SPOT' : this.types[pair]

    api.send(
      JSON.stringify({
        op: 'unsubscribe',
        args: [
          {
            instType: instType,
            channel: 'trade',
            instId: realPair
          }
        ]
      })
    )

    // this websocket api have a limit of about 10 messages per second.
    await sleep(150 * this.apis.length)
  }

  formatTrade(trade, pair) {
    // V2 API trade format
    // spot: { ts: "timestamp", price: "price", size: "size", side: "buy/sell", tradeId: "id" }
    // futures: { ts: "timestamp", px: "price", sz: "size", side: "buy/sell", tradeId: "id" }
    
    // Futures use 'px' and 'sz', spot uses 'price' and 'size'
    const price = trade.px !== undefined ? trade.px : trade.price
    const size = trade.sz !== undefined ? trade.sz : trade.size
    
    return {
      exchange: this.id,
      pair: pair,
      timestamp: +trade.ts,
      price: +price,
      size: +size,
      side: trade.side
    }
  }

  onMessage(event, api) {
    if (event.data === 'pong') {
      return
    }

    const json = JSON.parse(event.data)

    // V2 API uses 'action' field for message type
    // Only process 'update' events (real-time trades), ignore 'snapshot' (historical trades sent after subscription)
    if (json.action !== 'update') {
      return
    }

    if (json.data && json.data.length && json.arg) {
      const instType = json.arg.instType
      const instId = json.arg.instId
      
      // Add -SPOT suffix for spot trades
      const pair = instType === 'SPOT' ? `${instId}-SPOT` : instId

      return this.emitTrades(
        api.id,
        json.data.map(trade => this.formatTrade(trade, pair))
      )
    }
  }

  onApiCreated(api) {
    this.startKeepAlive(api, 'ping', 30000)
  }

  onApiRemoved(api) {
    this.stopKeepAlive(api)
  }
}

module.exports = Bitget
