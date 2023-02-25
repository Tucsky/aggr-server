const Exchange = require('../exchange')
const WebSocket = require('websocket').w3cwebsocket
const axios = require('axios')
const { getHms, sleep } = require('../helper')

class Kraken extends Exchange {
  constructor() {
    super()

    this.id = 'KRAKEN'
    this.keepAliveIntervals = {}

    this.endpoints = {
      PRODUCTS: ['https://api.kraken.com/0/public/AssetPairs', 'https://futures.kraken.com/derivatives/api/v3/instruments'],
    }

    this.url = (pair) => {
      if (typeof this.specs[pair] !== 'undefined') {
        return 'wss://futures.kraken.com/ws/v1'
      } else {
        return 'wss://ws.kraken.com'
      }
    }
  }

  formatProducts(response) {
    const products = []
    const specs = {}

    for (let data of response) {
      if (data.instruments) {
        for (let product of data.instruments) {
          if (!product.tradeable) {
            continue
          }

          const pair = product.symbol.toUpperCase()

          specs[pair] = product.contractSize

          if (products.find((a) => a.toLowerCase() === product.symbol.toLowerCase())) {
            throw new Error('duplicate pair detected on kraken exchange (' + pair + ')')
          }
          products.push(pair)
        }
      } else if (data.result) {
        for (let id in data.result) {
          if (data.result[id].wsname) {
            if (products.find((a) => a.toLowerCase() === data.result[id].wsname.toLowerCase())) {
              throw new Error('duplicate pair detected on kraken exchange (' + data.result[id].wsname + ')')
            }
            products.push(data.result[id].wsname)
          }
        }
      }
    }

    return {
      products,
      specs,
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

    const event = {
      event: 'subscribe',
    }

    if (typeof this.specs[pair] !== 'undefined') {
      // futures contract
      event.product_ids = [pair]
      event.feed = 'trade'
    } else {
      // spot
      event.pair = [pair]
      event.subscription = {
        name: 'trade',
      }
    }

    api.send(JSON.stringify(event))
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

    const event = {
      event: 'unsubscribe',
    }

    if (typeof this.specs[pair] !== 'undefined') {
      // futures contract
      event.product_ids = [pair]
      event.feed = 'trade'
    } else {
      // spot
      event.pair = [pair]
      event.subscription = {
        name: 'trade',
      }
    }

    api.send(JSON.stringify(event))
  }

  formatTrade(trade, pair, isFutures) {
    if (isFutures) {
      const output = {
        exchange: this.id,
        pair: pair,
        timestamp: isNaN(trade.time) ? +new Date(trade.time) : trade.time,
        price: trade.price,
        size: (typeof trade.qty !== 'undefined' ? trade.qty : trade.size) / trade.price,
        side: trade.side,
      }

      if (trade.type === 'liquidation') {
        output.liquidation = true
      }

      return output
    } else {
      return {
        exchange: this.id,
        pair: pair,
        timestamp: trade[2] * 1000,
        price: +trade[0],
        size: +trade[1],
        side: trade[3] === 'b' ? 'buy' : 'sell',
      }
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json || json.event === 'heartbeat') {
      return
    }

    if (json.feed === 'trade' && json.qty) {
      // futures

      if (json.type === 'fill') {
        this.emitTrades(api.id, [this.formatTrade(json, json.product_id, true)])
      } else if (json.type === 'liquidation') {
        this.emitLiquidations(api.id, [this.formatTrade(json, json.product_id, true)])
      }
    } else if (json[1] && json[1].length) {
      // spot

      return this.emitTrades(
        api.id,
        json[1].map((trade) => this.formatTrade(trade, json[3]))
      )
    }

    return false
  }

  async getMissingTrades(range, totalRecovered = 0) {
    const isFutures = typeof this.specs[range.pair] !== 'undefined'
    const pair = range.pair.replace('/', '')

    let endpoint

    if (isFutures) {
      // https://futures.kraken.com/derivatives/api/v3/history?symbol=PI_XBTUSD&lastTime=2022-06-16T12:32:23.002Z
      endpoint = `https://futures.kraken.com/derivatives/api/v3/history?symbol=${pair}&lastTime=${new Date(range.to).toISOString()}`
    } else {
      // https://api.kraken.com/0/public/Trades?pair=XBTUSD&since=1655381732.9661162
      endpoint = `https://api.kraken.com/0/public/Trades?pair=${pair}&since=${range.from / 1000}`
    }

    return axios
      .get(endpoint)
      .then((response) => {
        let raw

        if (isFutures) {
          raw = response.data.history
        } else {
          raw = Object.values(response.data.result)[0]
        }

        if (raw.length) {
          const trades = raw
            .map((trade) => this.formatTrade(trade, range.pair, isFutures))
            .filter((a) => a.timestamp >= range.from + 1 && a.timestamp < range.to)

          if (trades.length) {
            this.emitTrades(null, trades)

            totalRecovered += trades.length

            if (isFutures) {
              range.to = trades[0].timestamp
            } else {
              range.from = trades[trades.length - 1].timestamp
            }

            const remainingMissingTime = range.to - range.from

            if (remainingMissingTime > 1000) {
              console.log(
                `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} ... but theres more (${getHms(
                  remainingMissingTime
                )} remaining)`
              )

              return this.waitBeforeContinueRecovery().then(() => this.getMissingTrades(range, totalRecovered))
            } else {
              console.log(
                `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} ... (${getHms(
                  remainingMissingTime
                )} remaining)`
              )
            }
          }
        }

        return totalRecovered
      })
      .catch((err) => {
        console.error(`Failed to get historical trades on ${range.pair}`, err.message)
      })
  }
}

module.exports = Kraken
