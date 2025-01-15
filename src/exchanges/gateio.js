const Exchange = require('../exchange')
const axios = require('axios')

const IS_USDT_SETTLEMENT_REGEX = /USDT$/

class Gateio extends Exchange {
  constructor() {
    super()
    this.id = 'GATEIO'
    this.liquidationApi = {}
    this.maxConnectionsPerApi = 100

    this.endpoints = {
      PRODUCTS: [
        'https://api.gateio.ws/api/v4/spot/currency_pairs',
        'https://api.gateio.ws/api/v4/futures/usdt/contracts',
        'https://api.gateio.ws/api/v4/futures/btc/contracts'
      ]
    }
  }

  async getUrl(pair) {
    if (typeof this.specs[pair] === 'number') {
      if (IS_USDT_SETTLEMENT_REGEX.test(pair)) {
        return 'wss://fx-ws.gateio.ws/v4/ws/usdt'
      }
      return 'wss://fx-ws.gateio.ws/v4/ws/btc'
    }

    return 'wss://api.gateio.ws/ws/v4/'
  }

  formatProducts(responses) {
    const products = []
    const specs = {}

    for (const response of responses) {
      for (const product of response) {
        if (product.id) {
          products.push(`${product.id}-SPOT`)
        } else {
          products.push(product.name)
          specs[product.name] = product.quanto_multiplier
            ? parseFloat(product.quanto_multiplier)
            : 1
        }
      }
    }

    return { products, specs }
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async subscribe(api, pair) {
    const isFutures = typeof this.specs[pair] === 'number'

    if (isFutures) {
      const settlement = IS_USDT_SETTLEMENT_REGEX.test(pair) ? 'usdt' : 'btc'

      if (this.liquidationApi[settlement]) {
        const index = this.liquidationApi[settlement].pairs.indexOf(pair)
        if (index === -1) {
          this.liquidationApi[settlement].pairs.push(pair)
        }
      }
    }

    if (!(await super.subscribe(api, pair))) {
      return
    }

    api.send(
      JSON.stringify({
        channel: `${isFutures ? 'futures' : 'spot'}.trades`,
        event: 'subscribe',
        payload: [pair.replace('-SPOT', '')]
      })
    )

    return true
  }

  /**
   * Unsub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    const isFutures = typeof this.specs[pair] === 'number'

    if (isFutures) {
      const settlement = IS_USDT_SETTLEMENT_REGEX.test(pair) ? 'usdt' : 'btc'

      if (this.liquidationApi[settlement]) {
        const index = this.liquidationApi[settlement].pairs.indexOf(pair)

        if (index !== -1) {
          this.liquidationApi[settlement].pairs.splice(index, 1)
        }
      }
    }

    if (!(await super.unsubscribe(api, pair))) {
      return
    }

    api.send(
      JSON.stringify({
        channel: `${isFutures ? 'futures' : 'spot'}.trades`,
        event: 'unsubscribe',
        payload: [pair.replace('-SPOT', '')]
      })
    )

    return true
  }

  formatSpotTrade(trade) {
    return {
      exchange: this.id,
      pair: trade.currency_pair + '-SPOT',
      timestamp: +trade.create_time_ms,
      price: +trade.price,
      size: +trade.amount,
      side: trade.side
    }
  }

  formatFuturesTrade(trade) {
    return {
      exchange: this.id,
      pair: trade.contract,
      timestamp: +trade.create_time_ms,
      price: +trade.price,
      size:
        this.specs[trade.contract] > 0
          ? Math.abs(trade.size) * this.specs[trade.contract]
          : Math.abs(trade.size) / trade.price,
      side: trade.size > 0 ? 'buy' : 'sell'
    }
  }

  formatLiquidation(trade) {
    return {
      ...this.formatFuturesTrade(trade),
      side: trade.size > 0 ? 'sell' : 'buy',
      liquidation: true
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json) {
      return
    }

    if (json.event && json.event === 'update' && json.result) {
      if (json.result.length && json.channel === 'futures.trades') {
        const results = json.result
          .filter(trade => !trade.is_internal)
          .map(trade => this.formatFuturesTrade(trade))
        if (results.length) {
          return this.emitTrades(api.id, results)
        }
      }
      if (json.channel === 'spot.trades') {
        return this.emitTrades(api.id, [this.formatSpotTrade(json.result)])
      }
    }
  }

  onApiCreated(api) {
    const isFutures = /fx-ws/.test(api.url)

    if (isFutures) {
      const settlement = api.url.split('/').pop()
      this.startLiquidationInterval(settlement)
    }

    this.startKeepAlive(
      api,
      isFutures ? { channel: 'futures.ping' } : { channel: 'spot.ping' },
      18000
    )
  }

  onApiRemoved(api) {
    const isFutures = /fx-ws/.test(api.url)

    if (isFutures) {
      const settlement = api.url.split('/').pop()
      this.stopLiquidationInterval(settlement)
    }

    this.stopKeepAlive(api)
  }

  startLiquidationInterval(settlement) {
    if (!this.liquidationApi[settlement]) {
      this.liquidationApi[settlement] = {
        count: 0,
        interval: null,
        timestamp: null,
        pairs: []
      }
    }

    if (this.liquidationApi[settlement].interval) {
      this.liquidationApi[settlement].count++
      return
    }

    this.liquidationApi[settlement].count = 1
    this.liquidationApi[settlement].timestamp = +new Date()
    this.liquidationApi[settlement].interval = setInterval(
      () => {
        this.fetchLiquidations(settlement)
      },
      30000 + Math.random() * 1000
    )
  }

  stopLiquidationInterval(settlement) {
    if (!this.liquidationApi[settlement]) {
      return
    }

    if (this.liquidationApi[settlement].count) {
      this.liquidationApi[settlement].count--
    }

    if (
      !this.liquidationApi[settlement].count &&
      this.liquidationApi[settlement].interval
    ) {
      clearInterval(this.liquidationApi[settlement].interval)
      this.liquidationApi[settlement].interval = null
    }
  }

  async fetchLiquidations(settlement) {
    if (this.liquidationApi[settlement].loading) {
      return
    }
    this.liquidationApi[settlement].loading = true
    try {
      const response = await axios.get(
        `https://api.gateio.ws/api/v4/futures/${settlement}/liq_orders?from=${Math.round(
          this.liquidationApi[settlement].timestamp / 1000
        )}`
      )
      if (response.data && response.data.length) {
        this.emitLiquidations(
          null,
          response.data
            .filter(
              liquidation =>
                this.liquidationApi[settlement].pairs.indexOf(
                  liquidation.contract
                ) !== -1
            )
            .map(liquidation =>
              this.formatLiquidation({
                create_time_ms: liquidation.time * 1000,
                price: liquidation.fill_price,
                contract: liquidation.contract,
                size: liquidation.size
              })
            )
        )
      }

      this.liquidationApi[settlement].timestamp = +new Date()

      return []
    } catch (error) {
      console.error(`[${this.id}.fetchLiquidations] ${error.message}`)
    } finally {
      this.liquidationApi[settlement].loading = false
    }
  }
}
module.exports = Gateio
