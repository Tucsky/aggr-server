const Exchange = require('../exchange')
const { sleep, getHms } = require('../helper')
const axios = require('axios')

const { inflateRaw } = require('pako')

const WS_API_SPOT = 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1'
const WS_API_FUTURES = 'wss://openapi-ws-v2.bitmart.com/api?protocol=1.1'

/**
 * Bitmart class representing the Bitmart exchange.
 * @extends Exchange
 */
class Bitmart extends Exchange {
  constructor() {
    super()

    /**
     * @type {string}
     */
    this.id = 'BITMART'

    /**
     * @type {object}
     */
    this.subscriptions = {}

    /**
     * @type {object}
     */
    this.endpoints = {
      /**
       * URLs for product details on the Bitmart spot and contract markets.
       * @type {string[]}
       */
      PRODUCTS: [
        'https://api-cloud.bitmart.com/spot/v1/symbols',
        'https://api-cloud-v2.bitmart.com/contract/public/details'
      ],
      SPOT: {
        RECENT_TRADES: 'https://api-cloud.bitmart.com/spot/quotation/v3/trades'
      },
      FUTURES: {
        RECENT_TRADES: ''
      }
    }
  }

  async getUrl(pair) {
    if (this.specs[pair]) {
      return WS_API_FUTURES
    }

    return WS_API_SPOT
  }

  validateProducts(data) {
    if (!data.specs) {
      return false
    }

    return true
  }

  formatProducts(responses) {
    const products = []
    const specs = {}
    const types = {}

    for (const response of responses) {
      for (const product of response.data.symbols) {
        if (typeof product === 'string') {
          // spot endpoint
          products.push(product)
        } else {
          // contracts endpoint
          products.push(product.symbol)

          if (product.contract_size) {
            specs[product.symbol] = +product.contract_size
          }
        }
      }
    }

    return { products, specs, types }
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async subscribe(api, pair) {
    if (!(await super.subscribe(api, pair))) {
      return
    }

    const isContract = !!this.specs[pair]

    const typeImpl = isContract
      ? {
        prefix: 'futures',
        arg: 'action'
      }
      : {
        prefix: 'spot',
        arg: 'op'
      }

    api.send(
      JSON.stringify({
        [typeImpl.arg]: `subscribe`,
        args: [`${typeImpl.prefix}/trade:${pair}`]
      })
    )

    return true
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe(api, pair))) {
      return
    }

    const isContract = !!this.specs[pair]

    const typeImpl = isContract
      ? {
        prefix: 'futures',
        arg: 'action'
      }
      : {
        prefix: 'spot',
        arg: 'op'
      }

    api.send(
      JSON.stringify({
        [typeImpl.arg]: `unsubscribe`,
        args: [`${typeImpl.prefix}/trade:${pair}`]
      })
    )

    return true
  }

  formatTrade(trade) {
    if (this.specs[trade.symbol]) {
      return {
        exchange: this.id,
        pair: trade.symbol,
        timestamp: +new Date(trade.created_at),
        price: +trade.deal_price,
        size:
          (trade.deal_vol * this.specs[trade.symbol]) /
          (this.specs[trade.symbol] > 1 ? trade.deal_price : 1),
        side: typeof trade.m === 'boolean' ? (trade.m ? 'sell' : 'buy') : (trade.way <= 4 ? 'buy' : 'sell'),
        liquidation: trade.type === 1
      }
    } else {
      return {
        exchange: this.id,
        pair: trade.symbol,
        timestamp: trade.s_t * 1000,
        price: +trade.price,
        size: +trade.size,
        side: trade.side
      }
    }
  }

  onMessage(message, api) {
    let data = message.data

    if (typeof data !== 'string') {
      data = inflateRaw(message.data, { to: 'string' })
    }

    if (data === 'pong') {
      return
    }

    const json = JSON.parse(data)

    if (!json) {
      throw Error(`${this.id}: Can't parse json data from messageEvent`)
    }

    if (!json.data || !Array.isArray(json.data)) {
      return
    }

    const liquidations = json.data.filter(trade => trade.type === 1)

    if (liquidations.length) {
      this.emitLiquidations(api.id, liquidations.map(liquidation => this.formatTrade(liquidation)))
    }

    const trades = json.data
      .filter(trade => !trade.type || trade.type === 8)
      .map(trade => this.formatTrade(trade))

    if (trades.length) {
      this.emitTrades(
        api.id,
        trades
      )
    }
  }

  getMissingTrades(range, totalRecovered = 0) {
    if (this.specs[range.pair]) {
      return 0
    }

    // limit to 50 trades and no way to go back further
    let endpoint =
      this.endpoints.SPOT.RECENT_TRADES + `?symbol=${range.pair.toUpperCase()}`

    const mapResponseToTradeFormat = trade => {
      return {
        exchange: this.id,
        pair: trade[0],
        timestamp: +trade[1],
        price: +trade[2],
        size: +trade[3],
        side: trade[4]
      }
    }
    return axios
      .get(endpoint)
      .then(response => {
        if (response.data.data && response.data.data.length) {
          const trades = response.data.data.map(trade =>
            mapResponseToTradeFormat(trade)
          )

          this.emitTrades(null, trades)

          totalRecovered += trades.length

          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
            }`
          )
        }

        return totalRecovered
      })
      .catch(err => {
        console.error(
          `[${this.id}] failed to get missing trades on ${range.pair}`,
          err.message
        )

        return totalRecovered
      })
  }

  onApiCreated(api) {
    if (api.url === WS_API_FUTURES) {
      this.startKeepAlive(api, { action: 'ping' }, 15000)
    } else if (api.url === WS_API_SPOT) {
      this.startKeepAlive(api, 'ping', 15000)
    }
  }

  onApiRemoved(api) {
    this.stopKeepAlive(api)
  }
}

module.exports = Bitmart
