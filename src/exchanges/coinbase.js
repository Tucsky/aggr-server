const Exchange = require('../exchange')
const axios = require('axios')
const { getHms, sleep } = require('../helper')
const INTX_PAIR_REGEX = /-INTX$/

class Coinbase extends Exchange {
  constructor() {
    super()

    this.id = 'COINBASE'

    this.endpoints = {
      PRODUCTS: [
        'https://api.pro.coinbase.com/products',
        'https://api.coinbase.com/api/v3/brokerage/market/products?product_type=FUTURE&contract_expiry_type=PERPETUAL'
      ]
    }

    this.url = pair => {
      if (INTX_PAIR_REGEX.test(pair)) {
        return 'wss://advanced-trade-ws.coinbase.com'
      }

      return 'wss://ws-feed.exchange.coinbase.com'
    }
  }

  formatProducts(response) {
    const products = []

    const [spotResponse, perpResponse] = response

    if (spotResponse && spotResponse.length) {
      for (const product of spotResponse) {
        if (product.status !== 'online') {
          continue
        }

        products.push(product.id)
      }
    }

    if (perpResponse && perpResponse.products && perpResponse.products.length) {
      for (const product of perpResponse.products) {
        products.push(product.product_id)
      }
    }

    return {
      products
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

    const isIntx = INTX_PAIR_REGEX.test(pair)

    api.send(
      JSON.stringify({
        type: 'subscribe',
        ...(isIntx
          ? {
              channel: 'market_trades',
              product_ids: [pair]
            }
          : {
              channels: [{ name: 'matches', product_ids: [pair] }]
            })
      })
    )
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

    const isIntx = INTX_PAIR_REGEX.test(pair)

    api.send(
      JSON.stringify({
        type: 'unsubscribe',
        ...(isIntx
          ? {
              channel: 'market_trades',
              product_ids: [pair]
            }
          : {
              channels: [{ name: 'matches', product_ids: [pair] }]
            })
      })
    )
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json) {
      if (json.type === 'match') {
        return this.emitTrades(api.id, [
          this.formatTrade(json, json.product_id)
        ])
      } else if (json.channel === 'market_trades') {
        return this.emitTrades(
          api.id,
          json.events.reduce((acc, event) => {
            if (event.type === 'update') {
              acc.push(...event.trades.map(trade => this.formatTrade(trade, trade.product_id)))
            }

            return acc
          }, [])
        )
      }
    }
  }

  formatTrade(trade, pair) {
    return {
      exchange: this.id,
      pair: pair,
      timestamp: +new Date(trade.time),
      price: +trade.price,
      size: +trade.size,
      side: trade.side === 'buy' || trade.side === 'BUY' ? 'sell' : 'buy'
    }
  }

  async getMissingTrades(range, totalRecovered = 0, fromTradeId) {
    let endpoint

    const isIntx = INTX_PAIR_REGEX.test(range.pair)

    if (isIntx) {
      endpoint = `https://api.coinbase.com/api/v3/brokerage/market/products/${
        range.pair
      }/ticker?limit=100&end=${Math.round(range.to / 1000)}&start=${Math.round(
        range.from / 1000
      )}`
    } else {
      endpoint = `https://api.exchange.coinbase.com/products/${
        range.pair
      }/trades?limit=1000${fromTradeId ? '&after=' + fromTradeId : ''}`
    }

    if (+new Date() - range.to < 10000) {
      // coinbase api lags a lot
      // wait 10s before fetching initial results
      await sleep(10000)
    }

    return axios
      .get(endpoint)
      .then(response => {
        if (isIntx) {
          return response.data.trades
        }

        return response.data
      })
      .then(data => {
        if (data.length) {
          const earliestTradeId =
            data[data.length - 1].trade_id
          const earliestTradeTime = +new Date(
            data[data.length - 1].time
          )

          const trades = data
            .map(trade => this.formatTrade(trade, range.pair))
            .filter(
              a => a.timestamp >= range.from + 1 && a.timestamp < range.to
            )

          if (trades.length) {
            this.emitTrades(null, trades)

            totalRecovered += trades.length
            range.to = trades[trades.length - 1].timestamp
          }

          const remainingMissingTime = range.to - range.from

          if (
            trades.length &&
            remainingMissingTime > 1000 &&
            earliestTradeTime >= range.from
          ) {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${
                range.pair
              } ... but theres more (${getHms(remainingMissingTime)} remaining)`
            )
            return this.waitBeforeContinueRecovery().then(() =>
              this.getMissingTrades(range, totalRecovered, earliestTradeId)
            )
          } else {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${
                range.pair
              } (${getHms(remainingMissingTime)} remaining)`
            )
          }
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

module.exports = Coinbase
