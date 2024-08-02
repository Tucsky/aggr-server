const Exchange = require('../exchange')
const { sleep, getHms } = require('../helper')
const axios = require('axios')

const { inflateRaw } = require('pako')

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
        'https://api-cloud.bitmart.com/spot/v1/symbols/details',
        'https://api-cloud.bitmart.com/contract/public/details'
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
      return 'wss://openapi-ws.bitmart.com/api?protocol=1.1'
    }
    return 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1'
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
        products.push(product.symbol)

        if (product.contract_size) {
          specs[product.symbol] = +product.contract_size
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
        timestamp: trade.create_time_mill,
        price: +trade.deal_price,
        size:
          (trade.deal_vol * this.specs[trade.symbol]) /
          (this.specs[trade.symbol] > 1 ? trade.deal_price : 1),
        side: trade.way < 4 ? 'buy' : 'sell'
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

    const json = JSON.parse(data)

    if (!json || !json.data) {
      throw Error(`${this.id}: Can't parse json data from messageEvent`)
    }
    const trades = json.data.map(trade => this.formatTrade(trade))
    this.emitTrades(
      api.id,
      trades
    )
  }

  getMissingTrades(range, totalRecovered = 0) {
    // limit to 50 trades and no way to go back further
    let endpoint =
      this.endpoints.SPOT.RECENT_TRADES + `?symbol=${range.pair.toUpperCase()}`

    const mapResponseToTradeFormat = trade => {
      return {
        exchange: this.id,
        pair: trade[0],
        timestamp: trade[1],
        price: trade[2],
        size: trade[3],
        side: trade[4]
      }
    }
    return axios
      .get(endpoint)
      .then(response => {
        if (response.data.length) {
          const trades = response.data.map(trade =>
            mapResponseToTradeFormat(trade)
          )

          this.emitTrades(null, trades)

          totalRecovered += trades.length

          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${
              range.pair
            } (${getHms(remainingMissingTime)} remaining)`
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
}

module.exports = Bitmart
