const Exchange = require('../exchange')
const WebSocket = require('ws')
const pako = require('pako')
const axios = require('axios')

class Okex extends Exchange {
  constructor(options) {
    super(options)

    this.id = 'OKEX'

    this.endpoints = {
      PRODUCTS: [
        'https://www.okex.com/api/spot/v3/instruments',
        'https://www.okex.com/api/futures/v3/instruments',
        'https://www.okex.com/api/swap/v3/instruments',
      ],
    }

    this.liquidationProducts = []
    this.liquidationProductsReferences = {}

    this.options = Object.assign(
      {
        url: 'wss://real.okex.com:8443/ws/v3',
      },
      this.options
    )
  }

  formatProducts(response) {
    const products = []
    const specs = {}
    const types = {}
    const inversed = {}

    for (let data of response) {
      for (let product of data) {
        const pair = product.instrument_id

        if (product.alias) {
          // futures

          specs[pair] = +product.contract_val
          types[pair] = 'futures'

          if (product.is_inverse === 'true') {
            inversed[pair] = true
          }
        } else if (/-SWAP/.test(product.instrument_id)) {
          // swap

          specs[pair] = +product.contract_val
          types[pair] = 'swap'

          if (product.is_inverse === 'true') {
            inversed[pair] = true
          }
        } else {
          types[pair] = 'spot'
        }

        if (!pair) {
          throw new Error('failed to parse product on okex exchange', product)
        }

        products.push(pair)
      }
    }

    return {
      products,
      specs,
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
      this.liquidationProductsReferences[pair] = +new Date()

      if (this.liquidationProducts.length === 1) {
        this.startLiquidationTimer()
      }

      console.debug(`[${this.id}] listen ${pair} for liquidations`)
    }

    api.send(
      JSON.stringify({
        op: 'subscribe',
        args: [`${type}/trade:${pair}`],
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
        args: [`${type}/trade:${pair}`],
      })
    )
  }

  onMessage(event, api) {
    let json

    try {
      if (event instanceof String) {
        json = JSON.parse(event)
      } else {
        json = JSON.parse(pako.inflateRaw(event.data, { to: 'string' }))
      }
    } catch (error) {
      return
    }

    !json.table && console.log(json)

    if (!json || !json.data || !json.data.length) {
      return
    }

    return this.emitTrades(
      api.id,
      json.data.map((trade) => {
        let size
        let name = this.id

        if (typeof this.specs[trade.instrument_id] !== 'undefined') {
          size = ((trade.size || trade.qty) * this.specs[trade.instrument_id]) / (this.inversed[trade.instrument_id] ? trade.price : 1)
          // name += '_futures'
        } else {
          size = trade.size
        }

        return {
          exchange: name,
          pair: trade.instrument_id,
          timestamp: +new Date(trade.timestamp),
          price: +trade.price,
          size: +size,
          side: trade.side,
        }
      })
    )
  }

  onApiCreated(api) {
    // this.startKeepAlive(api)
  }

  onApiRemoved(api) {
    // this.stopKeepAlive(api)
  }

  startLiquidationTimer() {
    if (this._liquidationInterval) {
      return
    }

    console.debug(`[${this.id}] start liquidation timer`)

    this._liquidationProductIndex = 0

    this._liquidationInterval = setInterval(this.fetchLatestLiquidations.bind(this), 1500)
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

    const productType = this.types[productId]

    this._liquidationAxiosHandler && this._liquidationAxiosHandler.cancel()
    this._liquidationAxiosHandler = axios.CancelToken.source()

    axios
      .get(`https://www.okex.com/api/${productType}/v3/instruments/${productId}/liquidation?status=1&limit=15`)
      .then((response) => {
        if (!this.apis.length || !response.data || (response.data.error && response.data.error.length)) {
          console.log('getLiquidations => then => contain error(s)')
          console.error(response.data.error.join('\n'), productType, productId)
          return
        }

        const liquidations = response.data.filter((a) => {
          return !this.liquidationProductsReferences[productId] || +new Date(a.created_at) > this.liquidationProductsReferences[productId]
        })

        if (!liquidations.length) {
          return
        }

        this.liquidationProductsReferences[productId] = +new Date(liquidations[0].created_at)

        this.emitLiquidations(
          this.apis[0].id,
          liquidations.map((trade) => {
            const timestamp = +new Date(trade.created_at)
            const size = (trade.size * this.specs[productId]) / (this.inversed[productId] ? trade.price : 1)
            return {
              exchange: this.id,
              pair: productId,
              timestamp,
              price: +trade.price,
              size: size,
              side: trade.type === '4' ? 'buy' : 'sell',
              liquidation: true,
            }
          })
        )
      })
      .catch((error) => {
        console.log(
          'catch',
          error.message,
          'on',
          `https://www.okex.com/api/${productType}/v3/instruments/${productId}/liquidation?status=1&limit=10`
        )
        if (axios.isCancel(error)) {
          console.log('axios.isCancel')
          return
        }

        return error
      })
      .then(() => {
        delete this._liquidationAxiosHandler
      })
  }
}

module.exports = Okex
