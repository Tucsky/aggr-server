const Exchange = require('../../exchange')
const { sleep } = require('../../helper')
const { handleFuturesMessage } = require('./mexc.futures')
const {
  decodeMexcProtobuf,
  extractTradesFromProtobuf,
  handleSpotJsonMessage
} = require('./mexc.spot')

class Mexc extends Exchange {
  constructor() {
    super()

    this.id = 'MEXC'
    this.maxConnectionsPerApi = 50
    this.contractSizes = {}
    this.inversed = {}

    this.endpoints = {
      PRODUCTS: [
        'https://api.mexc.com/api/v3/exchangeInfo', // Spot: BTCUSDT
        'https://contract.mexc.com/api/v1/contract/detail' // Futures: BTC_USDT, BTC_USD
      ]
    }
  }

  async getUrl(pair) {
    // Spot pairs don't have underscores (BTCUSDT)
    // Futures pairs have underscores (BTC_USDT, BTC_USD)
    if (pair.includes('_')) {
      return 'wss://contract.mexc.com/edge'
    }
    return 'wss://wbs-api.mexc.com/ws'
  }

  formatProducts(responses) {
    const products = []
    const contractSizes = {}
    const inversed = {}

    // First response: spot products
    if (responses[0] && responses[0].symbols) {
      for (const product of responses[0].symbols) {
        products.push(product.symbol)
      }
    }

    // Second response: futures products
    if (responses[1] && responses[1].data) {
      for (const product of responses[1].data) {
        products.push(product.symbol)
        contractSizes[product.symbol] = product.contractSize
        inversed[product.symbol] = product.quoteCoin !== product.settleCoin
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

    const isFutures = pair.includes('_')

    if (isFutures) {
      // Futures subscription
      api.send(
        JSON.stringify({
          method: 'sub.deal',
          param: {
            symbol: pair
          }
        })
      )
    } else {
      // Spot subscription
      const normalizedPair = pair.replace(/_/g, '')

      api.send(
        JSON.stringify({
          method: 'SUBSCRIPTION',
          params: [`spot@public.aggre.deals.v3.api.pb@10ms@${normalizedPair}`]
        })
      )
    }

    // This websocket api has a limit of about 10 messages per second
    await sleep(150 * this.apis.length)

    return true
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

    const isFutures = pair.includes('_')

    if (isFutures) {
      // Futures unsubscription
      api.send(
        JSON.stringify({
          method: 'unsub.deal',
          param: {
            symbol: pair
          }
        })
      )
    } else {
      // Spot unsubscription
      const normalizedPair = pair.replace(/_/g, '')

      api.send(
        JSON.stringify({
          method: 'UNSUBSCRIPTION',
          params: [`spot@public.aggre.deals.v3.api.pb@10ms@${normalizedPair}`]
        })
      )
    }

    await sleep(150 * this.apis.length)

    return true
  }

  onMessage(event, api) {
    // Handle ArrayBuffer messages (protobuf) - spot only
    if (event.data instanceof ArrayBuffer) {
      return this.handleProtobufMessage(event.data, api)
    }

    // Handle JSON messages (both spot and futures)
    try {
      const json = JSON.parse(event.data)

      // Try futures message handler
      const futuresResult = handleFuturesMessage(
        json,
        this.contractSizes,
        this.inversed
      )
      if (futuresResult) {
        // Can be either trades array or true for acknowledgments
        if (Array.isArray(futuresResult)) {
          return this.emitTrades(api.id, futuresResult)
        }
        return true
      }

      // Try spot JSON message handler (acknowledgments only)
      const spotResult = handleSpotJsonMessage(json)
      if (spotResult) {
        return true
      }
    } catch (e) {
      // Skip non-JSON messages or parse errors
    }
  }

  onApiCreated(api) {
    // Futures uses lowercase 'ping', spot uses uppercase 'PING'
    // Check which type by looking at the URL
    const isFutures = api.url.includes('contract.mexc.com')

    this.startKeepAlive(
      api,
      {
        method: isFutures ? 'ping' : 'PING'
      },
      15000
    )
  }

  handleProtobufMessage(arrayBuffer, api) {
    try {
      const message = decodeMexcProtobuf(arrayBuffer)
      if (!message) {
        return true
      }

      const trades = extractTradesFromProtobuf(message, message.symbol)

      if (trades.length > 0) {
        return this.emitTrades(api.id, trades)
      }
    } catch (e) {
      // Silent fail for protobuf decode errors
      return true
    }
  }

  onApiRemoved(api) {
    this.stopKeepAlive(api)
  }
}

module.exports = Mexc
