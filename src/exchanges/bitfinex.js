const Exchange = require('../exchange')

class Bitfinex extends Exchange {
  constructor(options) {
    super(options)

    this.id = 'BITFINEX'
    this.channels = {}
    this.prices = {}

    this.endpoints = {
      PRODUCTS: 'https://api.bitfinex.com/v1/symbols',
    }

    this.options = Object.assign(
      {
        url: 'wss://api-pub.bitfinex.com/ws/2',
      },
      this.options
    )
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

    if (/f0:ustf0$/.test(pair)) {
      this.api.send(
        JSON.stringify({
          event: 'subscribe',
          channel: 'status',
          key: 'liq:t' + pair,
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

    const channelsToUnsubscribe = Object.keys(this.channels).filter(
      (id) => this.channels[id].pair === pair
    )

    if (!channelsToUnsubscribe) {
      console.log(
        `[${this}.id}/unsubscribe] no channel to unsubscribe to, but server called unsubscibr(${pair}). Here is the active channels on bitfinex exchange :`,
        this.channels
      )
    }

    for (let id of channelsToUnsubscribe) {
      api.send(
        JSON.stringify({
          event: 'unsubscribe',
          chanId: id,
        })
      )
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json.event) {
      if (json.chanId && json.pair) {
        console.debug(`[${this.id}] register channel ${json.chanId} (${json.channel}:${json.pair})`)
        this.channels[json.chanId] = {
          name: json.channel,
          pair: json.pair,
        }
      }
      return
    }

    if (!this.channels[json[0]] || json[1] === 'hb') {
      if (json[1] !== 'hb') {
        console.warn(`[${this.id}] received unknown event ${event.data}`)
      }
      return
    }

    const channel = this.channels[json[0]]

    if (channel.name !== 'status' && !channel.hasReceivedInitialData) {
      console.debug(`[${this.id}] skip first payload ${channel.name}:${channel.pair}`)
      channel.hasReceivedInitialData = true
      return
    }

    if (channel.name === 'trades' && json[1] === 'te') {
      this.prices[api.id + channel.pair] = +json[2][3]

      return this.emitTrades(api.id, [
        {
          exchange: this.id,
          pair: channel.pair,
          timestamp: +new Date(json[2][1]),
          price: +json[2][3],
          size: Math.abs(json[2][2]),
          side: json[2][2] < 0 ? 'sell' : 'buy',
        },
      ])
    } else if (channel.name === 'status' && json[1]) {
      console.debug(`[${this.id}] status ${JSON.stringify(json[1])}`)
      console.log(json, api._connected)
      return this.emitLiquidations(
        api.id,
        json[1]
          .filter((a) => api._connected.indexOf(a[4].substring(1)) !== -1)
          .map((a) => {
            const pair = a[4].substring(1)

            return {
              exchange: this.id,
              pair: a[4].substring(1),
              timestamp: parseInt(a[2]),
              price: this.prices[api.id + pair],
              size: Math.abs(a[5]),
              side: a[5] > 1 ? 'buy' : 'sell',
              liquidation: true,
            }
          })
      )
    }
  }
}

module.exports = Bitfinex
