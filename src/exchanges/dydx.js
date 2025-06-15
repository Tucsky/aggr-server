const Exchange = require('../exchange')
const { sleep } = require('../helper')
const WebSocket = require('websocket').w3cwebsocket

class Dydx extends Exchange {
  constructor() {
    super()

    this.id = 'DYDX'

    this.endpoints = {
      PRODUCTS: 'https://indexer.dydx.trade/v4/perpetualMarkets'
    }

    this.url = () => {
      return `wss://indexer.dydx.trade/v4/ws`
    }
  }

  formatProducts(data) {
    return Object.keys(data.markets)
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

    api.send(
      JSON.stringify({
        type: 'subscribe',
        channel: 'v4_trades',
        id: pair
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

    api.send(
      JSON.stringify({
        type: 'unsubscribe',
        channel: 'v4_trades',
        id: pair
      })
    )

    return true
  }

  formatTrade(trade, pair) {
    return {
      exchange: this.id,
      pair: pair,
      timestamp: +new Date(trade.createdAt),
      price: +trade.price,
      size: +trade.size,
      side: trade.side === 'BUY' ? 'buy' : 'sell'
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json.type === 'channel_data') {
      const trades = []
      const liquidations = []

      for (let i = 0; i < json.contents.trades.length; i++) {
        const trade = this.formatTrade(json.contents.trades[i], json.id)

        if (json.contents.trades[i].liquidation) {
          trade.liquidation = true

          liquidations.push(trade)
        } else {
          trades.push(trade)
        }
      }

      if (trades.length) {
        this.emitTrades(api.id, trades)
      }

      if (liquidations.length) {
        this.emitLiquidations(api.id, liquidations)
      }

      return true
    }
  }
}
module.exports = Dydx
