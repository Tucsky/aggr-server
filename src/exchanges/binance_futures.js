const Exchange = require('../exchange')
const { sleep, getHms } = require('../helper')
const axios = require('axios')
const WebSocket = require('websocket').w3cwebsocket

class BinanceFutures extends Exchange {
  constructor() {
    super()

    this.id = 'BINANCE_FUTURES'
    this.lastSubscriptionId = 0
    this.subscriptions = {}

    this.maxConnectionsPerApi = 100
    this.endpoints = {
      PRODUCTS: [
        'https://fapi.binance.com/fapi/v1/exchangeInfo',
        'https://dapi.binance.com/dapi/v1/exchangeInfo'
      ]
    }

    this.url = pair => {
      if (this.dapi[pair]) {
        return 'wss://dstream.binance.com/ws'
      }

      return 'wss://fstream.binance.com/public/ws'
    }
  }

  isDapiApi(api) {
    return api.url.includes('dstream.binance.com')
  }

  formatProducts(response) {
    const products = []
    const specs = {}
    const dapi = {}

    for (const data of response) {
      const type = ['fapi', 'dapi'][response.indexOf(data)]

      for (const product of data.symbols) {
        if (
          (product.contractStatus && product.contractStatus !== 'TRADING') ||
          (product.status && product.status !== 'TRADING')
        ) {
          continue
        }

        const symbol = product.symbol.toLowerCase()

        if (type === 'dapi') {
          dapi[symbol] = true
        }

        if (product.contractSize) {
          specs[symbol] = product.contractSize
        }

        products.push(symbol)
      }
    }

    return {
      products,
      specs,
      dapi
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

    this.subscriptions[pair] = ++this.lastSubscriptionId

    const params = this.dapi[pair]
      ? [pair + '@trade', pair + '@forceOrder']
      : [pair + '@trade']

    api.send(
      JSON.stringify({
        method: 'SUBSCRIBE',
        params,
        id: this.subscriptions[pair]
      })
    )

    if (!this.dapi[pair]) {
      this.subscribeLiquidations(api, pair)
    }

    // this websocket api has a limit of about 5 messages per second.
    await sleep(500 * this.apis.length)
  }

  /**
   * Unsub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      delete this.subscriptions[pair]

      if (!this.dapi[pair]) {
        this.unsubscribeLiquidations(api, pair)
      }

      return
    }

    const params = this.dapi[pair]
      ? [pair + '@trade', pair + '@forceOrder']
      : [pair + '@trade']

    api.send(
      JSON.stringify({
        method: 'UNSUBSCRIBE',
        params,
        id: this.subscriptions[pair]
      })
    )

    delete this.subscriptions[pair]

    if (!this.dapi[pair]) {
      this.unsubscribeLiquidations(api, pair)
    }

    // this websocket api has a limit of about 5 messages per second.
    await sleep(500 * this.apis.length)
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json) {
      return
    }

    // Binance SUBSCRIBE / UNSUBSCRIBE ack
    if (json.result === null && typeof json.id !== 'undefined') {
      return true
    }

    if (
      json.e === 'trade' &&
      (!json.X || json.X === 'MARKET' || json.X === 'RPI')
    ) {
      return this.emitTrades(api.id, [
        this.formatTrade(json, json.s.toLowerCase())
      ])
    }

    if (json.e === 'forceOrder') {
      return this.emitLiquidations(api.id, [this.formatLiquidation(json)])
    }
  }

  getSize(qty, price, symbol) {
    let size = +qty

    if (typeof this.specs[symbol] === 'number') {
      size = (size * this.specs[symbol]) / price
    }

    return size
  }

  /**
   *
   * @param {} trade
   * @param {*} symbol
   * @return {Trade}
   */
  formatTrade(trade, symbol) {
    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.T,
      price: +trade.p,
      size: this.getSize(trade.q, trade.p, symbol),
      side: trade.m ? 'sell' : 'buy'
    }
  }

  formatLiquidation(trade) {
    const symbol = trade.o.s.toLowerCase()

    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.o.T,
      price: +trade.o.p,
      size: this.getSize(trade.o.q, trade.o.p, symbol),
      side: trade.o.S === 'BUY' ? 'buy' : 'sell',
      liquidation: true
    }
  }

  getMissingTrades(range, totalRecovered = 0) {
    const startTime = range.from
    let endpoint = `?symbol=${range.pair.toUpperCase()}&startTime=${startTime + 1}&endTime=${range.to}&limit=1000`

    if (this.dapi[range.pair]) {
      endpoint = 'https://dapi.binance.com/dapi/v1/aggTrades' + endpoint
    } else {
      endpoint = 'https://fapi.binance.com/fapi/v1/aggTrades' + endpoint
    }

    return axios
      .get(endpoint)
      .then(response => {
        if (response.data.length) {
          const trades = response.data
            .filter(trade => trade.T > range.from && trade.T < range.to)
            .map(trade => ({
              ...this.formatTrade(trade, range.pair),
              count: trade.l - trade.f + 1
            }))

          if (trades.length) {
            this.emitTrades(null, trades)

            totalRecovered += trades.length
            range.from = trades[trades.length - 1].timestamp
          }

          const remainingMissingTime = range.to - range.from

          if (trades.length) {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
              } ... but theres more (${getHms(remainingMissingTime)} remaining)`
            )

            return this.waitBeforeContinueRecovery().then(() =>
              this.getMissingTrades(range, totalRecovered)
            )
          } else {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
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

  openLiquidationApi(api) {
    if (this.isDapiApi(api)) {
      return
    }

    if (
      api._liquidationApi &&
      (
        api._liquidationApi.readyState === WebSocket.OPEN ||
        api._liquidationApi.readyState === WebSocket.CONNECTING
      )
    ) {
      return
    }

    api._liquidationApiClosing = false
    api._liquidationApi = new WebSocket('wss://fstream.binance.com/market/ws')

    api._liquidationApi.onopen = () => {
      for (const pair of api._connected) {
        this.subscribeLiquidations(api, pair)
      }
    }

    api._liquidationApi.onmessage = event => this.onMessage(event, api)

    api._liquidationApi.onclose = () => {
      api._liquidationApi = null

      for (const pair of api._connected || []) {
        delete this.subscriptions[pair + '@forceOrder']
      }

      if (api._liquidationApiClosing) {
        return
      }

      if (api.readyState === WebSocket.OPEN) {
        console.log(
          `[${this.id}] liquidation api closed unexpectedly, reopen now`
        )

        this.openLiquidationApi(api)
      }
    }

    api._liquidationApi.onerror = event => {
      console.error(
        `[${this.id}] liquidation api errored`,
        event.message || ''
      )
    }
  }

  subscribeLiquidations(api, pair) {
    if (this.dapi[pair]) {
      return
    }

    if (
      !api._liquidationApi ||
      api._liquidationApi.readyState !== WebSocket.OPEN
    ) {
      return
    }

    const param = pair + '@forceOrder'

    if (this.subscriptions[param]) {
      return
    }

    this.subscriptions[param] = ++this.lastSubscriptionId

    api._liquidationApi.send(
      JSON.stringify({
        method: 'SUBSCRIBE',
        params: [param],
        id: this.subscriptions[param]
      })
    )
  }

  unsubscribeLiquidations(api, pair) {
    if (this.dapi[pair]) {
      return
    }

    const param = pair + '@forceOrder'

    if (
      !this.subscriptions[param] ||
      !api._liquidationApi ||
      api._liquidationApi.readyState !== WebSocket.OPEN
    ) {
      delete this.subscriptions[param]
      return
    }

    api._liquidationApi.send(
      JSON.stringify({
        method: 'UNSUBSCRIBE',
        params: [param],
        id: this.subscriptions[param]
      })
    )

    delete this.subscriptions[param]
  }

  onApiCreated(api) {
    api._liquidationApiClosing = false
    this.openLiquidationApi(api)
  }

  onApiRemoved(api) {
    api._liquidationApiClosing = true

    if (
      api._liquidationApi &&
      (
        api._liquidationApi.readyState === WebSocket.OPEN ||
        api._liquidationApi.readyState === WebSocket.CONNECTING
      )
    ) {
      api._liquidationApi.close()
    }
  }
}

module.exports = BinanceFutures