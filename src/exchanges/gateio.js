const Exchange = require('../exchange')
const WebSocket = require('websocket').w3cwebsocket
const axios = require('axios')

class GateIO extends Exchange {
  constructor() {
    super()

    this.id = 'GATEIO'
    this.endpoints = {
      PRODUCTS: [
        'https://api.gateio.ws/api/v4/spot/currency_pairs',
        'https://api.gateio.ws/api/v4/futures/usdt/contracts'
      ]
    }
    this.types = {}
    this.multipliers = {}
  }

  async getUrl(pair) {
    // https://www.gate.io/docs/developers/apiv4/ws/en/
    const type = this.types[pair]
    if (type === 'spot') return 'wss://ws.gate.io/v3/'
    if (type === 'futures') return 'wss://fx-ws.gateio.ws/v4/ws/usdt'
  }

  formatProducts(response) {
    const products = []
    const multipliers = {}
    const types = {}

    response.forEach((data, index) => {
      const type = ['spot', 'futures'][index]

      data.forEach(product => {
        let pair
        switch (type) {
          case 'spot':
            pair = product.id + '_SPOT'
            multipliers[pair] = parseFloat(product.min_base_amount)
            break
          case 'futures':
            pair = product.name + '_FUTURES'
            multipliers[pair] = parseFloat(product.quanto_multiplier)
            break
        }

        if (products.find(a => a.toLowerCase() === pair.toLowerCase())) {
          throw new Error(
            'Duplicate pair detected on gateio exchange (' + pair + ')'
          )
        }

        types[pair] = type
        products.push(pair)
      })
    })
    return {
      products,
      multipliers,
      types
    }
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async subscribe(api, pair) {
    // Public Trades Channel
    // https://www.gate.io/docs/developers/apiv4/ws/en/#public-trades-channel
    if (!(await super.subscribe(api, pair))) {
      return
    }

    if (this.types[pair] === 'spot') {
      api.send(
        JSON.stringify({
          method: `trades.subscribe`,
          params: [pair.split('_').slice(0, 2).join('_')]
        })
      )
    }
    if (this.types[pair] === 'futures') {
      api.send(
        JSON.stringify({
          time: Date.now(),
          channel: `${this.types[pair]}.trades`,
          event: 'subscribe',
          payload: [pair.split('_').slice(0, 2).join('_')]
        })
      )
    }

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

    if (this.types[pair] === 'spot') {
      api.send(
        JSON.stringify({
          method: `trades.unsubscribe`,
          params: [pair.split('_').slice(0, 2).join('_')]
        })
      )
    } else if (this.types[pair] === 'futures') {
      api.send(
        JSON.stringify({
          time: Date.now(),
          channel: `${this.types[pair]}.trades`,
          event: 'unsubscribe',
          payload: [pair.split('_').slice(0, 2).join('_')]
        })
      )
    }

    return true
  }

  formatPerpTrade(trade, channel) {
    return {
      exchange: this.id,
      pair: trade.contract + '_' + channel,
      timestamp: +new Date(trade.create_time_ms),
      price: +trade.price,
      size: +(
        Math.abs(trade.size) * this.multipliers[trade.contract + '_' + channel]
      ),
      side: trade.size > 0 ? 'buy' : 'sell'
    }
  }

  formatSpotTrade()

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json) {
      return
    }

    let trades = []

    const { channel, event, result, method, params } = json

    isPerpChannel = () => (
		channel && channel?.endsWith('trades') && 
		event && event === 'update' &&
		result && result?.length
	)
      

	isSpotChannel = () =>
		method === 'trades.update' &&
		Array.isArray(params)

    if (isPerpChannel()) {
      trades = result.map(trade => this.formatPerpTrade(
				trade,
				channel.split('.')[0].toUpperCase()
			)
		)
    } else if ( isSpotChannel() ) {
      const [contract, tradesData] = params
      trades = tradesData.map(trade => {
        return {
          exchange: this.id,
          pair: contract + '_SPOT',
          timestamp: +new Date(
            parseInt((trade.time * 1000).toString().split('.')[0])
          ),
          price: +trade.price,
          size: +trade.amount,
          side: trade.type
        }
      })
    }
    return this.emitTrades(api.id, trades)
  }
}

module.exports = GateIO
