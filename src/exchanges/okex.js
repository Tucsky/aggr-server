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
        'https://www.okx.com/api/v5/public/instruments?instType=SPOT',
        'https://www.okx.com/api/v5/public/instruments?instType=FUTURES',
        'https://www.okx.com/api/v5/public/instruments?instType=SWAP'
      ]
    }

    this.liquidationProducts = []
    this.liquidationProductsReferences = {}

    this.url = 'wss://ws.okx.com:8443/ws/v5/public'
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
   * Unsub
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

      return this.emitLiquidations(api.id, liqs)
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
    // (get the 100 trades preceding endTimestamp)
    return `${this.endpoints.LIQUIDATIONS}?instId=${range.pair
      }&instType=SWAP&uly=${range.pair.replace('-SWAP', '')}&state=filled&after=${range.to
      }`
  }

  /**
   * Fetch pair liquidations before timestamp
   * @param {*} range
   * @returns
   */
  async fetchLiquidationOrders(range) {
    const url = this.getLiquidationsUrl(range)

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
      const liquidations = await this.fetchLiquidationOrders(range)

      if (!liquidations || liquidations.length === 0) {
        return allLiquidations
      }

      for (const liquidation of liquidations) {
        if (liquidation.ts < range.from) {
          return allLiquidations
        }

        allLiquidations.push(liquidation)
      }

      range.to = +liquidations[liquidations.length - 1].ts
    }
  }

  async getMissingTrades(range, totalRecovered = 0, first = true) {
    if (this.types[range.pair] !== 'SPOT' && first) {
      try {
        const liquidations = await this.fetchAllLiquidationOrders({ ...range });
        console.log(
          `[${this.id}.recoverMissingTrades] +${liquidations.length} liquidations for ${range.pair}`
        );

        if (liquidations.length) {
          this.emitLiquidations(
            null,
            liquidations.map(liquidation =>
              this.formatLiquidation(liquidation, range.pair)
            )
          );
        }
      } catch (error) {
        console.error(
          `[${this.id}] failed to get missing liquidations on ${range.pair}:`,
          error.message
        );
      }
    }

    const endpoint = `https://www.okx.com/api/v5/market/history-trades?instId=${range.pair}&type=2&limit=100&after=${range.to}`;

    try {
      const response = await this.retryWithDelay(
        () => axios.get(endpoint),
        5, // Retry up to 5 times
        1, // Start with a multiplier of 1
        range
      );

      if (response.data.data.length) {
        const trades = response.data.data
          .filter(
            trade =>
              Number(trade.ts) > range.from &&
              Number(trade.ts) < range.to
          )
          .map(trade => this.formatTrade(trade));

        if (trades.length) {
          this.emitTrades(null, trades);
          totalRecovered += trades.length;
          range.to = trades[trades.length - 1].timestamp;
        }

        const remainingMissingTime = range.to - range.from;

        if (trades.length) {
          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
            } ... but there's more (${getHms(remainingMissingTime)} remaining)`
          );
          return this.waitBeforeContinueRecovery().then(() =>
            this.getMissingTrades(range, totalRecovered, false)
          );
        } else {
          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
            } (${getHms(remainingMissingTime)} remaining)`
          );
        }
      }

      return totalRecovered;
    } catch (err) {
      console.error(
        `[${this.id}] failed to get missing trades on ${range.pair} after retries:`,
        err.message
      );
      return totalRecovered;
    }
  }

  /**
   * Retry a function with delays between attempts, using backoff
   * @param {Function} fn The function to execute
   * @param {number} retries Number of retry attempts
   * @param {number} multiplier Multiplier for backoff delays (starts at 1)
   * @param {Object} range The range object for logging retries (optional)
   * @returns {Promise<any>} The result of the function or an error if retries fail
   */
  async retryWithDelay(fn, retries, multiplier = 1, range = {}) {
    try {
      return await fn();
    } catch (err) {
      if (retries > 0) {
        console.warn(
          `[${this.id}] Retrying with delay (${range.pair || 'unknown pair'}) attempt ${multiplier
          }...`
        );
        await this.waitBeforeContinueRecovery(multiplier);
        return this.retryWithDelay(fn, retries - 1, multiplier + 1, range);
      }
      console.error(
        `[${this.id}] Exceeded retry limit for ${range.pair || 'unknown pair'
        }:`,
        err.message
      );
      throw err; // Propagate the error after exceeding retries
    }
  }
}

module.exports = Okex
