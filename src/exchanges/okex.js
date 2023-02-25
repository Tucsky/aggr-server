const Exchange = require('../exchange')
const WebSocket = require('websocket').w3cwebsocket
const axios = require('axios')
const { getHms } = require('../helper')

class Okex extends Exchange {
  constructor() {
    super()

    this.id = 'OKEX'

    this.endpoints = {
      PRODUCTS: [
        'https://www.okex.com/api/v5/public/instruments?instType=SPOT',
        'https://www.okex.com/api/v5/public/instruments?instType=FUTURES',
        'https://www.okex.com/api/v5/public/instruments?instType=SWAP',
      ],
    }

    this.liquidationProducts = []
    this.liquidationProductsReferences = {}

    this.url = 'wss://ws.okex.com:8443/ws/v5/public'
  }

  formatProducts(response) {
    const products = []
    const specs = {}
    const aliases = {}
    const types = {}
    const inversed = {}

    for (let data of response) {
      for (let product of data.data) {
        const type = product.instType
        const pair = product.instId

        if (type === 'FUTURES') {
          // futures

          specs[pair] = +product.ctVal
          types[pair] = 'futures'
          aliases[pair] = product.alias

          if (product.ctType === 'inverse') {
            inversed[pair] = true
          }
        } else if (type === 'SWAP') {
          // swap

          specs[pair] = +product.ctVal
          types[pair] = 'swap'

          if (product.ctType === 'inverse') {
            inversed[pair] = true
          }
        } else {
          types[pair] = 'spot'
        }

        products.push(pair)
      }
    }

    return {
      products,
      specs,
      aliases,
      types,
      inversed,
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

    const type = this.types[pair]

    if (type !== 'spot') {
      this.liquidationProducts.push(pair)
      this.liquidationProductsReferences[pair] = Date.now()

      if (this.liquidationProducts.length === 1) {
        this.startLiquidationTimer()
      }

      console.debug(`[${this.id}] listen ${pair} for liquidations`)
    }

    api.send(
      JSON.stringify({
        op: 'subscribe',
        args: [
          {
            channel: 'trades',
            instId: pair,
          },
        ],
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

    const type = this.types[pair]

    if (type !== 'spot' && this.liquidationProducts.indexOf(pair) !== -1) {
      this.liquidationProducts.splice(this.liquidationProducts.indexOf(pair), 1)

      console.debug(`[${this.id}] unsubscribe ${pair} from liquidations loop`)

      if (this.liquidationProducts.length === 0) {
        this.stopLiquidationTimer()
      }
    }

    api.send(
      JSON.stringify({
        op: 'unsubscribe',
        args: [
          {
            channel: 'trades',
            instId: pair,
          },
        ],
      })
    )
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json || !json.data) {
      return
    }

    return this.emitTrades(
      api.id,
      json.data.map((trade) => this.formatTrade(trade))
    )
  }

  formatTrade(trade) {
    let size

    if (typeof this.specs[trade.instId] !== 'undefined') {
      size = (trade.sz * this.specs[trade.instId]) / (this.inversed[trade.instId] ? trade.px : 1)
    } else {
      size = trade.sz
    }

    return {
      exchange: this.id,
      pair: trade.instId,
      timestamp: +trade.ts,
      price: +trade.px,
      size: +size,
      side: trade.side,
    }
  }

  formatLiquidation(trade, pair) {
    const size = (trade.sz * this.specs[pair]) / (this.inversed[pair] ? trade.bkPx : 1)
    //console.debug(`[${this.id}] okex liquidation at ${new Date(+trade.ts).toISOString()}`)

    return {
      exchange: this.id,
      pair: pair,
      timestamp: +trade.ts,
      price: +trade.bkPx,
      size: size,
      side: trade.side,
      liquidation: true,
    }
  }

  startLiquidationTimer() {
    if (this._liquidationInterval) {
      return
    }

    console.debug(`[${this.id}] start liquidation timer`)

    this._liquidationProductIndex = 0

    this._liquidationInterval = setInterval(this.fetchLatestLiquidations.bind(this), 2500)
  }

  stopLiquidationTimer() {
    if (!this._liquidationInterval) {
      return
    }

    console.debug(`[${this.id}] stop liquidation timer`)

    clearInterval(this._liquidationInterval)

    delete this._liquidationInterval
  }

  async getMissingTrades(range, totalRecovered = 0, beforeTradeId) {
    let endpoint
    if (beforeTradeId) {
      endpoint = `https://www.okex.com/api/v5/market/history-trades?instId=${range.pair}&limit=500${
        beforeTradeId ? '&after=' + beforeTradeId : ''
      }`
    } else {
      endpoint = `https://www.okex.com/api/v5/market/trades?instId=${range.pair}&limit=500`
    }

    return axios
      .get(endpoint)
      .then((response) => {
        if (response.data.data.length) {
          const lastTradeId = response.data.data[response.data.data.length - 1].tradeId
          const earliestTradeTime = +response.data.data[response.data.data.length - 1].ts

          const trades = response.data.data
            .map((trade) => this.formatTrade(trade))
            .filter((a) => a.timestamp >= range.from + 1 && a.timestamp < range.to)

          if (trades.length) {
            this.emitTrades(null, trades)

            totalRecovered += trades.length
            range.to = trades[trades.length - 1].timestamp
          }

          const remainingMissingTime = range.to - range.from

          if (trades.length && remainingMissingTime > 500 && earliestTradeTime >= range.from) {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair} ... but theres more (${getHms(
                remainingMissingTime
              )} remaining)`
            )
            return this.waitBeforeContinueRecovery().then(() => this.getMissingTrades(range, totalRecovered, lastTradeId))
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

  getLiquidationEndpoint(productId) {
    const productType = this.types[productId].toUpperCase()
    let endpoint = `https://www.okex.com/api/v5/public/liquidation-orders?instId=${productId}&instType=${productType}&uly=${productId
      .split('-')
      .slice(0, 2)
      .join('-')}&state=filled`

    if (productType === 'FUTURES') {
      endpoint += `&alias=${this.aliases[productId]}`
    }

    return endpoint
  }

  fetchLatestLiquidations() {
    const productId = this.liquidationProducts[this._liquidationProductIndex++ % this.liquidationProducts.length]
    const endpoint = this.getLiquidationEndpoint(productId)

    this._liquidationAxiosHandler && this._liquidationAxiosHandler.cancel()
    this._liquidationAxiosHandler = axios.CancelToken.source()

    axios
      .get(endpoint)
      .then((response) => {
        if (
          !this.apis.length ||
          !response.data ||
          response.data.msg ||
          !response.data.data ||
          !response.data.data.length ||
          !response.data.data[0]
        ) {
          //throw new Error(response.data ? response.data.msg : 'empty REST /liquidation-order')
          return
        }

        const liquidations = response.data.data[0].details.filter((liquidation) => {
          return !this.liquidationProductsReferences[productId] || liquidation.ts > this.liquidationProductsReferences[productId]
        })

        if (!liquidations.length) {
          return
        }

        this.liquidationProductsReferences[productId] = +liquidations[0].ts

        this.emitLiquidations(
          null,
          liquidations.map((liquidation) => this.formatLiquidation(liquidation, productId))
        )
      })
      .catch((err) => {
        let message = `[okex.fetchLatestLiquidations] ${productId} ${err.message} at ${endpoint}`

        if (err.response && err.response.data && err.response.data.msg) {
          message += '\n\t' + err.response.data.msg
        }

        console.error(message, `(instId: ${productId}})`)

        if (axios.isCancel(err)) {
          return
        }

        return err
      })
      .then(() => {
        delete this._liquidationAxiosHandler
      })
  }
}

module.exports = Okex
