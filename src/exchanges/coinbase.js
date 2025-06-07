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
        'https://api.coinbase.com/api/v3/brokerage/market/products?product_type=SPOT',
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

    if (spotResponse && spotResponse.products && spotResponse.products.length) {
      for (const product of spotResponse.products) {
        if (product.status !== 'online') {
          continue
        }

        if (product.alias) {
          // Skip alias-only products like LTC-USDC
          continue
        }

        products.push(product.product_id)
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

    if (isIntx) {
      // this websocket api have a limit of about 10 messages per second.
      await sleep(100 * this.apis.length)
    }
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

    if (isIntx) {
      // this websocket api have a limit of about 10 messages per second.
      await sleep(100 * this.apis.length)
    }
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
              acc.push(
                ...event.trades.map(trade =>
                  this.formatTrade(trade, trade.product_id)
                )
              )
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
  async getMissingTrades(range, totalRecovered = 0) {
    const isIntx = INTX_PAIR_REGEX.test(range.pair)
    let endpoint
    if (isIntx || !range.earliestTradeId) {
      endpoint = `https://api.coinbase.com/api/v3/brokerage/market/products/${range.pair
        }/ticker?limit=100&end=${Math.round(range.to / 1000)}&start=${Math.round(
          range.from / 1000
        )}`

      // If close to current time, wait to allow trades to accumulate
      if (+new Date() - range.to < 10000) {
        await sleep(10000)
      }
    } else {
      endpoint = `https://api.exchange.coinbase.com/products/${range.pair}/trades?limit=1000&after=${range.earliestTradeId}`
    }

    try {
      const response = await axios.get(endpoint)
      const rawData = Array.isArray(response.data)
        ? response.data
        : response.data.trades || []
      if (!rawData.length) {
        console.log(
          `[${this.id}.recoverMissingTrades] no more trades for ${range.pair}`
        )
        return totalRecovered
      }

      const trades = rawData
        .map(trade => this.formatTrade(trade, range.pair))
        .filter(a => a.timestamp >= range.from + 1 && a.timestamp < range.to)

      if (trades.length) {
        this.emitTrades(null, trades)
        totalRecovered += trades.length
        range.to = trades[trades.length - 1].timestamp
      }

      const remainingMissingTime = range.to - range.from

      const earliestRawTrade = rawData[rawData.length - 1]
      const earliestTradeTime = +new Date(earliestRawTrade.time)

      if (!range.earliestTradeId) {
        if (
          trades.length &&
          remainingMissingTime > 1000 &&
          earliestTradeTime >= range.from
        ) {
          range.earliestTradeId = parseInt(earliestRawTrade.trade_id, 10)
          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
            } ... switching to Exchange API (${getHms(
              remainingMissingTime
            )} remaining)`
          )
          await this.waitBeforeContinueRecovery()
          return this.getMissingTrades(range, totalRecovered)
        } else {
          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
            } (${getHms(remainingMissingTime)} remaining)`
          )
          return totalRecovered
        }
      }

      if (range.earliestTradeId) {
        if (trades.length && remainingMissingTime > 1000 && earliestTradeTime >= range.from) {
          range.earliestTradeId = earliestRawTrade.trade_id
          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
            } ... but there's more (${getHms(remainingMissingTime)} remaining)`
          )
          await this.waitBeforeContinueRecovery()
          return this.getMissingTrades(range, totalRecovered)
        } else {
          // No more needed or no more available
          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
            } (${getHms(remainingMissingTime)} remaining)`
          )
          return totalRecovered
        }
      }
    } catch (err) {
      console.error(
        `[${this.id}] failed to get missing trades on ${range.pair}`,
        err.message
      )
      return totalRecovered
    }
  }
}

module.exports = Coinbase
