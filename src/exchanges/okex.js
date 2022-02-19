const Exchange = require('../exchange')
const WebSocket = require('ws')
const axios = require('axios')

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
      timestamp: trade.ts,
      price: +trade.px,
      size: +size,
      side: trade.side,
    }
  }

  formatLiquidation(trade, pair) {
    const size = (trade.sz * this.specs[pair]) / (this.inversed[pair] ? trade.bkPx : 1)

    return {
      exchange: this.id,
      pair: pair,
      timestamp: trade.ts,
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

  fetchLatestLiquidations() {
    const productId = this.liquidationProducts[this._liquidationProductIndex++ % this.liquidationProducts.length]
    const productType = this.types[productId].toUpperCase()
    let endpoint = `https://www.okex.com/api/v5/public/liquidation-orders?instId=${productId}&instType=${productType}&uly=${productId
      .split('-')
      .slice(0, 2)
      .join('-')}&state=filled`

    if (productType === 'FUTURES') {
      endpoint += `&alias=${this.aliases[productId]}`
    }

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
          throw new Error(response.data ? response.data.msg : 'empty REST /liquidation-order')
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
        let message = `[okex.fetchLatestLiquidations] ${productType}/${productId} ${err.message} at ${endpoint}`

        if (err.response && err.response.data && err.response.data.msg) {
          message += '\n\t' + err.response.data.msg
        }

        console.error(message, `(instType: ${productType}, instId: ${productId}})`)

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
