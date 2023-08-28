const Exchange = require('../exchange')
const WebSocket = require('websocket').w3cwebsocket
const axios = require('axios')
const { getHms } = require('../helper')

class CryptoCom extends Exchange {
  constructor() {
    super()

    this.id = 'CRYPTOCOM'

    this.endpoints = {
      PRODUCTS: 'https://api.crypto.com/exchange/v1/public/get-instruments'
    }
  }

  async getUrl() {
    return `wss://stream.crypto.com/v2/market`
  }

  formatProducts(response) {
    return response.result.data.map(s => s.symbol)
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
    const params = {
      channels: [`trade.${pair}`]
    }

    api.send(
      JSON.stringify({
        method: 'subscribe',
        params,
        id: Date.now()
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

    const params = {
      channels: [`trade.${pair}`]
    }

    api.send(
      JSON.stringify({
        method: 'unsubscribe',
        params,
        id: Date.now()
      })
    )

    return true
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)
    if (json.result) {
      return this.emitTrades(
        api.id,
        json.result.data.map(t => this.formatResponse(t))
      )
    }
  }

  formatResponse(t) {
    // Trade format
    // {
    // 	"s": "sell", 				// direction
    // 	"p": "28914.00",			// price
    // 	"q": "0.00004",				// quantity
    // 	"t": 1687347262760,			// timestamp
    // 	"d": "4611686018456190046", // id
    // 	"i": "BTC_USD"				// pair
    // }

    return {
      exchange: this.id,
      pair: t.i,
      timestamp: +new Date(t.t),
      price: +t.p,
      size: +t.q,
      side: t.s.toLowerCase()
    }
  }

  async getMissingTrades(range, totalRecovered = 0, fromTradeId) {
    // https://exchange-docs.crypto.com/exchange/v1/rest-ws/index.html#public-get-trades
    // Retuns maximum 150 trades per request

    const endpoint =
      `https://api.crypto.com/exchange/v1/public/get-trades?` +
      `instrument_name=${range.pair}&` +
      `count=1000&` +
      `start_ts=${range.from}&` +
      `end_ts=${range.to}`

    return axios
      .get(endpoint)
      .then(response => {
        const data = response.data.result.data

        if (data.length) {
          const { d: earliestTradeId, t: earliestTradeTime } =
            data[(data.length - 1, 1)]

          const trades = []

          data.map(t => {
            const trade = this.formatResponse(t)
            if (
              trade.timestamp >= range.from + 1 &&
              trade.timestamp < range.to
            ) {
              trades.push(trade)
            }
          })

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

module.exports = CryptoCom
