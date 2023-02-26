const Exchange = require('../exchange')
const WebSocket = require('websocket').w3cwebsocket
const axios = require('axios')
const { getHms } = require('../helper')

class Bitmex extends Exchange {
  constructor() {
    super()

    this.id = 'BITMEX'
    this.pairCurrencies = {}
    this.xbtPrice = 48000
    this.types = {}
    this.multipliers = {}
    this.underlyingToPositionMultipliers = {}

    this.endpoints = {
      PRODUCTS: 'https://www.bitmex.com/api/v1/instrument/active',
    }

    this.url = () => {
          return `wss://www.bitmex.com/realtime`
        };
  }

  formatProducts(data) {
    const products = []
    const types = {}
    const multipliers = {}
    const underlyingToPositionMultipliers = {}

    for (const product of data) {
      types[product.symbol] = product.isInverse ? 'inverse' : product.isQuanto ? 'quanto' : 'linear'
      multipliers[product.symbol] = product.multiplier

      if (types[product.symbol] === 'linear') {
        underlyingToPositionMultipliers[product.symbol] = product.underlyingToPositionMultiplier
      }

      products.push(product.symbol)
    }

    return {
      products,
      types,
      multipliers,
      underlyingToPositionMultipliers,
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

    api.send(
      JSON.stringify({
        op: 'subscribe',
        args: ['trade:' + pair, 'liquidation:' + pair],
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
        op: 'unsubscribe',
        args: ['trade:' + pair, 'liquidation:' + pair],
      })
    )
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json && json.data && json.data.length) {
      if (json.table === 'liquidation' && json.action === 'insert') {
        return this.emitLiquidations(
          api.id,
          json.data.map((trade) => this.formatLiquidation(trade))
        )
      } else if (json.table === 'trade' && json.action === 'insert') {
        return this.emitTrades(
          api.id,
          json.data.map((trade) => this.formatTrade(trade))
        )
      } else if (json.table === 'instrument' && json.data[0].lastPrice) {
        this.xbtPrice = json.data[0].lastPrice
      }
    }
  }

  formatTrade(trade) {
    return {
      exchange: this.id,
      pair: trade.symbol,
      timestamp: +new Date(trade.timestamp),
      price: trade.price,
      size: trade.homeNotional,
      side: trade.side === 'Buy' ? 'buy' : 'sell',
    }
  }

  formatLiquidation(trade) {
    let size

    if (this.types[trade.symbol] === 'quanto') {
      size = (this.multipliers[trade.symbol] / 100000000) * trade.leavesQty * this.xbtPrice
    } else if (this.types[trade.symbol] === 'inverse') {
      size = trade.leavesQty / trade.price
    } else {
      size = (1 / this.underlyingToPositionMultipliers[trade.symbol]) * trade.leavesQty
    }

    return  {
      exchange: this.id,
      pair: trade.symbol,
      timestamp: +new Date(),
      price: trade.price,
      size: size,
      side: trade.side === 'Buy' ? 'buy' : 'sell',
      liquidation: true,
    }
  }

  getMissingTrades(range, totalRecovered = 0) {
    const startTimeISO = new Date(range.from + 1).toISOString()
    const endTimeISO = new Date(range.to).toISOString()

    const endpoint = `https://www.bitmex.com/api/v1/trade?symbol=${range.pair}&startTime=${startTimeISO}&endTime=${endTimeISO}&count=1000`
    
    return axios
      .get(endpoint)
      .then((response) => {
        if (response.data.length) {
          const trades = response.data.map((trade) => this.formatTrade(trade, range.pair))
  
          this.emitTrades(null, trades)

          totalRecovered += trades.length
          range.from = trades[trades.length - 1].timestamp

          const remainingMissingTime = range.to - range.from

          if (remainingMissingTime > 1000 && response.data.length >= 1000) {
            console.log(`[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} ... but theres more (${getHms(remainingMissingTime)} remaining)`)
            return this.waitBeforeContinueRecovery().then(() => this.getMissingTrades(range, totalRecovered))
          } else {
            console.log(`[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} (${getHms(remainingMissingTime)} remaining)`)
          }
        }

        return totalRecovered
      })
      .catch((err) => {
        console.error(`[${this.id}] failed to get missing trades on ${range.pair}`, err.message)
      })
  }

  onApiCreated(api) {
    api.send(
      JSON.stringify({
        op: 'subscribe',
        args: ['instrument:XBTUSD'],
      })
    )
  }
}

module.exports = Bitmex
