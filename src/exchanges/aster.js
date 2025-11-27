// src/exchanges/aster.js
const Exchange = require('../exchange')

class Aster extends Exchange {
  constructor() {
    super()
    this.id = 'ASTER'

    this.endpoints = {
      // Binance-futures style exchangeInfo
      PRODUCTS: 'https://fapi.asterdex.com/fapi/v1/exchangeInfo'
    }
  }

  async getUrl() {
    // Binance fstream clone
    return 'wss://fstream.asterdex.com/ws'
  }

  /**
   * Map /fapi/v1/exchangeInfo to a flat list of tradable perp symbols
   */
  formatProducts(response) {
    const products = []

    if (response && Array.isArray(response.symbols)) {
      for (const s of response.symbols) {
        if (s.contractType === 'PERPETUAL' && s.status === 'TRADING') {
          products.push(s.symbol) // e.g. 'BTCUSDT'
        }
      }
    }

    console.log(`[${this.id}] formatted ${products.length} products`)
    return { products }
  }

  /**
   * Subscribe to trades
   * @param {WebSocket} api
   * @param {string} pair  e.g. 'BTCUSDT'
   */
  async subscribe(api, pair) {
    if (!(await super.subscribe.apply(this, arguments))) {
      return
    }

    api.send(
      JSON.stringify({
        method: 'SUBSCRIBE',
        params: [`${pair.toLowerCase()}@aggTrade`],
        id: Date.now()
      })
    )

    return true
  }

  /**
   * Unsubscribe from trades
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    api.send(
      JSON.stringify({
        method: 'UNSUBSCRIBE',
        params: [`${pair.toLowerCase()}@aggTrade`],
        id: Date.now()
      })
    )

    return true
  }

  /**
   * Handle incoming WS messages
   */
  onMessage(event, api) {
    let json
    try {
      json = JSON.parse(event.data)
    } catch (err) {
      console.error(`[${this.id}/onMessage] failed to parse message`, err)
      return
    }

    // Support both raw event and /stream wrapper style
    let payload = null
    if (json && json.stream && json.data && json.data.e === 'aggTrade') {
      payload = json.data
    } else if (json && json.e === 'aggTrade') {
      payload = json
    }

    if (!payload) return

    const trade = this.formatTrade(payload)
    return this.emitTrades(api.id, [trade])
  }

  /**
   * Normalize Aster/Binance aggTrade to aggr internal trade format
   */
  formatTrade(t) {
    // if buyer is maker => taker is seller => 'sell'
    const takerSide = t.m ? 'sell' : 'buy'

    return {
      exchange: this.id,
      pair: t.s,          // 'BTCUSDT'
      timestamp: +t.T,    // ms
      price: +t.p,
      size: +t.q,
      side: takerSide
    }
  }
}

module.exports = Aster
