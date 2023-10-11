const Exchange = require('../exchange')
const WebSocket = require('websocket').w3cwebsocket
const axios = require('axios')
const { getHms } = require('../helper')

class Okex extends Exchange {
  constructor() {
    super()

    this.id = 'OKEX'

    this.endpoints = {
      LIQUIDATIONS: 'https://www.okx.com/api/v5/public/liquidation-orders',
      PRODUCTS: [
        'https://www.okex.com/api/v5/public/instruments?instType=SPOT',
        'https://www.okex.com/api/v5/public/instruments?instType=FUTURES',
        'https://www.okex.com/api/v5/public/instruments?instType=SWAP'
      ]
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
          aliases[pair] = product.alias

          if (product.ctType === 'inverse') {
            inversed[pair] = true
          }
        } else if (type === 'SWAP') {
          // swap

          specs[pair] = +product.ctVal

          if (product.ctType === 'inverse') {
            inversed[pair] = true
          }
        }

        types[pair] = type
        products.push(pair)
      }
    }

    return {
      products,
      specs,
      aliases,
      types,
      inversed
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

    api.send(
      JSON.stringify({
        op: 'subscribe',
        args: [
          {
            channel: 'trades',
            instId: pair
          }
        ]
      })
    )

    if (this.types[pair] !== 'SPOT') {
      api.send(
        JSON.stringify({
          op: 'subscribe',
          args: [
            {
              channel: 'liquidation-orders',
              instType: this.types[pair]
            }
          ]
        })
      )
    }
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

    api.send(
      JSON.stringify({
        op: 'unsubscribe',
        args: [
          {
            channel: 'trades',
            instId: pair
          }
        ]
      })
    )

    if (this.types[pair] !== 'SPOT') {
      api.send(
        JSON.stringify({
          op: 'subscribe',
          args: [
            {
              channel: 'liquidation-orders',
              instType: this.types[pair]
            }
          ]
        })
      )
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json || !json.data) {
      return
    }

    if (json.arg.channel === 'liquidation-orders') {
      const liqs = json.data.reduce((acc, pairData) => {
        if (api._connected.indexOf(pairData.instId) === -1) {
          return acc
        }

        return acc.concat(
          pairData.details.map(liquidation =>
            this.formatLiquidation(liquidation, pairData.instId)
          )
        )
      }, [])

      return this.emitLiquidations(
        api.id,
        liqs
      )
    }

    return this.emitTrades(
      api.id,
      json.data.map(trade => this.formatTrade(trade))
    )
  }

  formatTrade(trade) {
    let size

    if (typeof this.specs[trade.instId] !== 'undefined') {
      size =
        (trade.sz * this.specs[trade.instId]) /
        (this.inversed[trade.instId] ? trade.px : 1)
    } else {
      size = trade.sz
    }

    return {
      exchange: this.id,
      pair: trade.instId,
      timestamp: +trade.ts,
      price: +trade.px,
      size: +size,
      side: trade.side
    }
  }

  formatLiquidation(liquidation, pair) {
    const size =
      (liquidation.sz * this.specs[pair]) /
      (this.inversed[pair] ? liquidation.bkPx : 1)

    return {
      exchange: this.id,
      pair: pair,
      timestamp: +liquidation.ts,
      price: +liquidation.bkPx,
      size: size,
      side: liquidation.side,
      liquidation: true
    }
  }

  getLiquidationsUrl(range) {
    // after query param = before
    // (get the 100 trades prededing endTimestamp)
    return `${this.endpoints.LIQUIDATIONS}?instId=${range.pair}&instType=SWAP&uly=${range.pair.replace('-SWAP', '')}&state=filled&after=${range.to}`
  }

  /**
   * Fetch pair liquidations before timestamp
   * @param {*} range
   * @returns
   */
  async fetchLiquidationOrders(range) {
    const url = this.getLiquidationsUrl(range)
    console.log(url)
    try {
      const response = await axios.get(url)
      if (response.data.data && response.data.data.length) {
        return response.data.data[0].details
      }
      return []
    } catch (error) {
      throw new Error(`Error fetching data: ${error}`)
    }
  }

  async fetchAllLiquidationOrders(range) {
    const allLiquidations = []

    while (true) {
      console.log(`[${this.id}] fetch all ${range.pair} liquidations in`, new Date(range.from).toISOString(), new Date(range.to).toISOString())
      const liquidations = await this.fetchLiquidationOrders(range)

      if (!liquidations || liquidations.length === 0) {
        console.log('received', liquidations.length, 'liquidations -> break')
        return allLiquidations
      }
      console.log('received', liquidations.length, 'liquidations -> process')

      console.log('first liq @ ', new Date(+liquidations[0].ts).toISOString(), new Date(+liquidations[liquidations.length - 1].ts).toISOString())
      for (const liquidation of liquidations) {
        if (liquidation.ts < range.from) {
          console.log(`liquidation ${liquidations.indexOf(liquidation) + 1}/${liquidations.length} is outside range -> break`)
          return allLiquidations
        }

        allLiquidations.push(liquidation)
      }

      // new range
      console.log(`[${this.id}] set new end date to last liquidation`, new Date(+liquidations[liquidations.length - 1].ts).toISOString())
      range.to = +liquidations[liquidations.length - 1].ts
    }
  }

  async getMissingTrades(range, totalRecovered = 0, beforeTradeId) {
    if (this.types[range.pair] !== 'SPOT' && !beforeTradeId) {
      const liquidations = await this.fetchAllLiquidationOrders({ ...range })
      console.log(`[${this.id}.recoverMissingTrades] +${liquidations.length} liquidations for ${range.pair}`)

      if (liquidations.length) {
        this.emitLiquidations(null, liquidations.map(
          liquidation => this.formatLiquidation(liquidation, range.pair)
        ))
      }
    }

    let endpoint
    if (beforeTradeId) {
      endpoint = `https://www.okex.com/api/v5/market/history-trades?instId=${range.pair
        }&limit=100${beforeTradeId ? '&after=' + beforeTradeId : ''}`
    } else {
      endpoint = `https://www.okex.com/api/v5/market/trades?instId=${range.pair}&limit=500`
    }

    return axios
      .get(endpoint)
      .then(response => {
        if (response.data.data.length) {
          const lastTradeId =
            response.data.data[response.data.data.length - 1].tradeId
          const earliestTradeTime =
            +response.data.data[response.data.data.length - 1].ts
          const trades = response.data.data
            .filter(trade => Number(trade.ts) > range.from && (beforeTradeId || Number(trade.ts) < range.to))
            .map(trade => this.formatTrade(trade))

          if (trades.length) {
            this.emitTrades(null, trades)

            totalRecovered += trades.length
            range.to = trades[trades.length - 1].timestamp
          }

          const remainingMissingTime = range.to - range.from

          if (
            trades.length &&
            remainingMissingTime > 500 &&
            earliestTradeTime >= range.from
          ) {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
              } ... but theres more (${getHms(remainingMissingTime)} remaining)`
            )
            return this.waitBeforeContinueRecovery().then(() =>
              this.getMissingTrades(range, totalRecovered, lastTradeId)
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
}

module.exports = Okex
