const Exchange = require('../exchange')
const axios = require('axios')
const { getHms, sleep } = require('../helper')

class Coinbase extends Exchange {
  constructor() {
    super()

    this.id = 'COINBASE'

    this.endpoints = {
      PRODUCTS: 'https://api.pro.coinbase.com/products',
    }

    this.url = 'wss://ws-feed.pro.coinbase.com'
  }

  formatProducts(data) {
    return data.filter((product) => product.status === 'online').map((product) => product.id)
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
        type: 'subscribe',
        channels: [{ name: 'matches', product_ids: [pair] }],
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
        type: 'unsubscribe',
        channels: [{ name: 'matches', product_ids: [pair] }],
      })
    )
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json && json.type === 'match') {
      return this.emitTrades(api.id, [this.formatTrade(json, json.product_id)])
    }
  }

  formatTrade(trade, pair) {
    return {
      exchange: this.id,
      pair: pair,
      timestamp: +new Date(trade.time),
      price: +trade.price,
      size: +trade.size,
      side: trade.side === 'buy' ? 'sell' : 'buy',
    }
  }

  async getMissingTrades(range, totalRecovered = 0, fromTradeId) {
    const endpoint = `https://api.exchange.coinbase.com/products/${range.pair}/trades?limit=1000${
      fromTradeId ? '&after=' + fromTradeId : ''
    }`

    if (+new Date() - range.to < 10000) {
      // coinbase api lags a lot
      // wait 10s before fetching initial results
      await sleep(10000)
    }

    return axios
      .get(endpoint)
      .then((response) => {
        if (response.data.length) {
          const earliestTradeId = response.data[response.data.length - 1].trade_id
          const earliestTradeTime = +new Date(response.data[response.data.length - 1].time)

          const trades = response.data
            .map((trade) => this.formatTrade(trade, range.pair))
            .filter((a) => a.timestamp >= range.from + 1 && a.timestamp < range.to)

          if (trades.length) {
            this.emitTrades(null, trades)

            totalRecovered += trades.length
            range.to = trades[trades.length - 1].timestamp
          }

          const remainingMissingTime = range.to - range.from

          if (trades.length && remainingMissingTime > 1000 && earliestTradeTime >= range.from) {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} ... but theres more (${getHms(
                remainingMissingTime
              )} remaining)`
            )
            return this.waitBeforeContinueRecovery().then(() => this.getMissingTrades(range, totalRecovered, earliestTradeId))
          } else {
            console.log(`[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} (${getHms(remainingMissingTime)} remaining)`)
          }
        }

        return totalRecovered
      })
      .catch((err) => {
        console.error(`[${this.id}] failed to get missing trades on ${range.pair}`, err.message)

        return totalRecovered
      })
  }
}

module.exports = Coinbase
