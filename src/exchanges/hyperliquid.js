const Exchange = require('../exchange')
const { readProducts, saveProducts } = require('../services/catalog')
const axios = require('axios')

class Hyperliquid extends Exchange {
  constructor() {
    super()
    this.id = 'HYPERLIQUID'

    this.endpoints = {
      PRODUCTS: 'https://api.hyperliquid.xyz/info'
    }
  }

  async getUrl() {
    return 'wss://api.hyperliquid.xyz/ws'
  }

  // Custom getProducts implementation for POST API
  async getProducts(forceRefreshProducts = false) {
    let formatedProducts

    // Load from cache if not forcing refresh
    if (!forceRefreshProducts) {
      try {
        formatedProducts = await readProducts(this.id)
      } catch (error) {
        console.error(`[${this.id}/getProducts] failed to read products`, error)
      }
    }

    // Fetch new products if no cache available
    if (!formatedProducts) {
      try {
        console.log(`[${this.id}] fetching products via POST API`)

        const response = await axios.post(
          this.endpoints.PRODUCTS,
          { type: 'meta' },
          {
            headers: { 'Content-Type': 'application/json' }
          }
        )

        formatedProducts = this.formatProducts(response.data) || []

        // Save to cache
        await saveProducts(this.id, formatedProducts)
        console.log(
          `[${this.id}] saved ${
            formatedProducts.products?.length || 0
          } products`
        )
      } catch (error) {
        console.error(
          `[${this.id}/getProducts] failed to fetch products`,
          error.message
        )
        throw error
      }
    }

    // Set products to instance
    if (formatedProducts && formatedProducts.products) {
      this.products = formatedProducts.products
    }

    return this.products
  }

  formatProducts(response) {
    const products = []

    if (response && response.universe && response.universe.length) {
      for (const product of response.universe) {
        products.push(product.name)
      }
    }

    console.log(`[${this.id}] formatted ${products.length} products`)
    return { products }
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
        method: 'subscribe',
        subscription: {
          type: 'trades',
          coin: pair
        }
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
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    api.send(
      JSON.stringify({
        method: 'unsubscribe',
        subscription: {
          type: 'trades',
          coin: pair
        }
      })
    )

    return true
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json && json.channel === 'trades') {
      return this.emitTrades(
        api.id,
        json.data.map(trade => this.formatTrade(trade))
      )
    }
  }

  formatTrade(trade) {
    return {
      exchange: this.id,
      pair: trade.coin,
      timestamp: +new Date(trade.time),
      price: +trade.px,
      size: +trade.sz,
      side: trade.side === 'B' ? 'buy' : 'sell'
    }
  }
}

module.exports = Hyperliquid
