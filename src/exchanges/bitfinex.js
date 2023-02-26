const Exchange = require('../exchange')
const axios = require('axios')
const fs = require('fs')
const { getHms } = require('../helper')

class Bitfinex extends Exchange {
  constructor() {
    super()

    this.id = 'BITFINEX'
    this.channels = {}
    this.prices = {}

    this.maxConnectionsPerApi = 24
    this.endpoints = {
      PRODUCTS: 'https://api.bitfinex.com/v1/symbols',
    }

    this.url = 'wss://api-pub.bitfinex.com/ws/2'
  }

  formatProducts(pairs) {
    return pairs.map((product) => product.toUpperCase())
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
        event: 'subscribe',
        channel: 'trades',
        symbol: 't' + pair,
      })
    )

    if (api._connected.length === 1) {
      api.send(
        JSON.stringify({
          event: 'subscribe',
          channel: 'status',
          key: 'liq:global',
        })
      )
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

    if (api._connected.length === 0) {
      const chanId = Object.keys(this.channels).find((id) => this.channels[id].name === 'status')

      if (chanId) {
        api.send(
          JSON.stringify({
            event: 'unsubscribe',
            chanId: chanId,
          })
        )

        delete this.channels[chanId]
      }
    }

    const channelsToUnsubscribe = Object.keys(this.channels).filter(
      (id) => this.channels[id].pair === pair && this.channels[id].apiId === api.id
    )

    if (!channelsToUnsubscribe.length) {
      console.warn(`[${this}.id}/unsubscribe] no channel to unsubscribe from`, this.channels)
      return
    }

    for (let id of channelsToUnsubscribe) {
      api.send(
        JSON.stringify({
          event: 'unsubscribe',
          chanId: id,
        })
      )
      delete this.channels[id]
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json.event === 'subscribed' && json.chanId) {
      // console.debug(`[${this.id}] register channel ${json.chanId} (${json.channel}:${json.pair})`)
      this.channels[json.chanId] = {
        name: json.channel,
        pair: json.pair,
        apiId: api.id,
      }
      return
    }

    if (!this.channels[json[0]] || json[1] === 'hb') {
      return
    }

    const channel = this.channels[json[0]]

    if (!channel.hasSentInitialMessage) {
      // console.debug(`[${this.id}] skip first payload ${channel.name}:${channel.pair}`)
      channel.hasSentInitialMessage = true
      return
    }

    if (channel.name === 'trades' && json[1] === 'te') {
      if (this.prices[channel.pair]) {
        const percentChange = ((1 - +json[2][3] / this.prices[channel.pair]) * -1) * 100

        if (Math.abs(percentChange) > 1) {
          console.log(`[${this.id}] unusual price difference (${percentChange.toFixed(2)}%) for ${channel.pair} : ${this.prices[channel.pair]} -> ${json[2][3]}`)
        }
      }
      this.prices[channel.pair] = +json[2][3]

      return this.emitTrades(api.id, [this.formatTrade(json[2], channel.pair)])
    } else if (channel.name === 'status' && json[1]) {
      return this.emitLiquidations(
        api.id,
        json[1]
          .filter((liquidation) => {
            return !liquidation[8] && !liquidation[10] && !liquidation[11] && api._connected.indexOf(liquidation[4].substring(1)) !== -1
          })
          .map((liquidation) => this.formatLiquidation(liquidation, liquidation[4].substring(1)))
      )
    }
  }

  formatTrade(trade, pair) {
    return {
      exchange: this.id,
      pair: pair,
      timestamp: parseInt(trade[1]),
      price: trade[3],
      size: Math.abs(trade[2]),
      side: trade[2] < 0 ? 'sell' : 'buy',
    }
  }

  formatLiquidation(trade, pair) {
    return {
      exchange: this.id,
      pair: pair,
      timestamp: parseInt(trade[2]),
      price: this.prices[pair],
      size: Math.abs(trade[5]),
      side: trade[5] > 1 ? 'sell' : 'buy',
      liquidation: true,
    }
  }

  getMissingTrades(range, totalRecovered = 0) {
    const startTime = range.from

    const endpoint = `https://api-pub.bitfinex.com/v2/trades/Symbol/hist?symbol=${'t' + range.pair}&start=${startTime + 1}&end=${
      range.to
    }&limit=1000&sort=1`

    return axios
      .get(endpoint)
      .then((response) => {
        if (response.data.length) {
          const trades = response.data.map((trade) => this.formatTrade(trade, range.pair))

          this.emitTrades(null, trades)

          totalRecovered += trades.length
          range.from = trades[trades.length - 1].timestamp

          const remainingMissingTime = range.to - range.from

          if (remainingMissingTime > 1000 && trades.length >= 1000) {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} ... but theres more (${getHms(
                remainingMissingTime
              )} remaining)`
            )
            return this.waitBeforeContinueRecovery().then(() => this.getMissingTrades(range, totalRecovered))
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

  onApiRemoved(api) {
    console.log(`[${this.id}] has ${Object.keys(this.channels).length} channels`)

    const apiChannels = Object.keys(this.channels).filter((id) => this.channels[id].apiId === api.id)

    for (const id of apiChannels) {
      delete this.channels[id]
    }
  }
}

module.exports = Bitfinex
