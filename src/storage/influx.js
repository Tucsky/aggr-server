const Influx = require('influx')
const { getHms, groupTrades } = require('../helper')

require('../typedef')

class InfluxStorage {
  constructor(options) {
    this.name = this.constructor.name
    this.format = 'point'

    this.lastBar = {}
    this.lastClose = {}
    this.options = options
  }

  async connect() {
    if (/\-/.test(this.options.influxDatabase)) {
      throw new Error('dashes not allowed inside influxdb database')
    }

    let host = this.options.influxHost
    let port = this.options.influxPort

    if (typeof this.options.influxUrl === 'string' && this.options.influxUrl.length) {
      ;[host, port] = this.options.influxUrl.split(':')
    }

    console.log(`[storage/${this.name}] connecting to ${host}:${port}`)

    this.influx = new Influx.InfluxDB({
      host: host || 'localhost',
      port: port || '8086',
      database: this.options.influxDatabase,
    })

    const databases = await this.influx.getDatabaseNames()

    if (!databases.includes(this.options.influxDatabase)) {
      await this.influx.createDatabase(this.options.influxDatabase)
    }

    await this.getPreviousCloses()
  }

  /**
   * Create continuous queries if not exists
   *
   * @memberof InfluxStorage
   */
  async resample(range) {
    let sourceTimeframe
    let destinationTimeframe

    this.options.influxResampleTo.sort((a, b) => a - b)

    for (let timeframe of this.options.influxResampleTo) {
      const flooredRange = {
        from: Math.floor(range.from / timeframe) * timeframe,
        to: Math.floor(range.to / timeframe) * timeframe + timeframe,
      }

      for (let i = this.options.influxResampleTo.indexOf(timeframe); i >= 0; i--) {
        if (timeframe <= this.options.influxResampleTo[i] || timeframe % this.options.influxResampleTo[i] !== 0) {
          if (i === 0) {
            sourceTimeframe = getHms(this.options.influxTimeframe)
          }
          continue
        }

        sourceTimeframe = getHms(this.options.influxResampleTo[i])
        break
      }

      destinationTimeframe = getHms(timeframe)

      const query = `SELECT min(low) AS low, 
      max(high) AS high, 
      first(open) AS open, 
      last(close) AS close, 
      sum(count) AS count, 
      sum(cbuy) AS cbuy, 
      sum(csell) AS csell, 
      sum(lbuy) AS lbuy, 
      sum(lsell) AS lsell, 
      sum(vol) AS vol, 
      sum(vbuy) AS vbuy, 
      sum(vsell) AS vsell`

      const query_from = `${this.options.influxDatabase}.autogen.${this.options.influxMeasurement}_${sourceTimeframe}`
      const query_into = `${this.options.influxDatabase}.autogen.${this.options.influxMeasurement}_${destinationTimeframe}`
      const coverage = `WHERE time >= ${flooredRange.from}ms AND time < ${flooredRange.to}ms`
      const group = `GROUP BY time(${destinationTimeframe}), market fill(none)`

      await this.influx.query(`${query} INTO ${query_into} FROM ${query_from} ${coverage} ${group}`).catch((err) => {
        console.log(err.message, '\nquery:\n', `${query} INTO ${query_into} FROM ${query_from} ${coverage} ${group}`)
      })
    }
  }

  getPreviousCloses() {
    this.influx
      .query(
        `SELECT close FROM ${this.options.influxMeasurement}${
          this.options.influxTimeframe ? '_' + getHms(this.options.influxTimeframe) : ''
        } GROUP BY "market" ORDER BY time DESC LIMIT 1`
      )
      .then((data) => {
        for (let bar of data) {
          this.lastClose[bar.market] = bar.close
        }
      })
  }

  /**
   * Trigger when save fired from main controller
   *
   * @param {Trade[]} trades
   * @returns {Promise<any>}
   * @memberof InfluxStorage
   */
  async save(trades) {
    if (!trades || !trades.length) {
      return Promise.resolve()
    }

    const resampleRange = await this.import(trades)

    await this.resample(resampleRange)
  }

  /**
   * close a bar (register close + reference for next bar)
   * @param {Bar} bar
   */
  closeBar(bar) {
    const barIdentifier = bar.exchange + ':' + bar.pair

    if (typeof bar.close === 'number') {
      // reg close for next bar
      this.lastClose[barIdentifier] = bar.close
    }

    this.lastBar[barIdentifier] = bar

    return this.lastBar[barIdentifier]
  }

  /**
   * Import the trades to influx db
   *
   * @param {Trade[]} trades
   * @param {string} identifier ex bitmex:XBTUSD
   * @returns {Promise<{
      from: number,
      to: number,
      pairs: string[],
      exchanges: string[]
    }>}
   * @memberof InfluxStorage
   */
  import(trades, identifier) {
    /**
     * Current bars
     * @type {{[identifier: string]: Bar}}
     */
    const activeBars = {}

    /**
     * closed bars
     * @type {Bar[]}
     */
    const closedBars = []

    /**
     * liquidations
     * @type {Trade[]}
     */
    const liquidations = []

    /**
     * Total range of import
     * @type {TimeRange}
     */
    const totalRange = {
      from: Infinity,
      to: 0,
      pairs: [],
      exchanges: [],
    }

    for (let i = 0; i <= trades.length; i++) {
      const trade = trades[i]

      let tradeIdentifier
      let tradeFlooredTime

      if (!trade) {
        // end of loop reached = close all bars
        for (let barIdentifier in activeBars) {
          closedBars.push(this.closeBar(activeBars[barIdentifier]))

          delete activeBars[barIdentifier]
        }

        break
      } else {
        tradeIdentifier = trade.exchange + ':' + trade.pair
        tradeFlooredTime = Math.floor(trade.timestamp / this.options.influxTimeframe) * this.options.influxTimeframe

        if (!activeBars[tradeIdentifier] || activeBars[tradeIdentifier].time < tradeFlooredTime) {
          if (activeBars[tradeIdentifier]) {
            // close bar required

            closedBars.push(this.closeBar(activeBars[tradeIdentifier]))

            delete activeBars[tradeIdentifier]
          }

          if (trade) {
            // create bar required

            if (this.lastBar[tradeIdentifier] && this.lastBar[tradeIdentifier].time === tradeFlooredTime) {
              // trades passed in save() contains some of the last batch (trade time = last bar time)
              // recover exchange point of lastbar
              activeBars[tradeIdentifier] = this.lastBar[tradeIdentifier]
            } else {
              // create new bar
              activeBars[tradeIdentifier] = {
                time: tradeFlooredTime,
                pair: trade.pair,
                exchange: trade.exchange,
                cbuy: 0,
                csell: 0,
                vbuy: 0,
                vsell: 0,
                lbuy: 0,
                lsell: 0,
                open: null,
                high: null,
                low: null,
                close: null,
              }

              if (typeof this.lastClose[tradeIdentifier] === 'number') {
                // this bar open = last bar close (from last save or getReferencePoint on startup)
                activeBars[tradeIdentifier].open = activeBars[tradeIdentifier].high = activeBars[tradeIdentifier].low = activeBars[
                  tradeIdentifier
                ].close = this.lastClose[tradeIdentifier]
              }
            }
          }
        }
      }

      if (trade.liquidation) {
        liquidations.push(trade)
      }

      if (trade.liquidation) {
        // trade is a liquidation
        activeBars[tradeIdentifier]['l' + trade.side] += trade.price * trade.size
      } else {
        if (activeBars[tradeIdentifier].open === null) {
          // new bar without close in db, should only happen once
          console.log(`[storage/${this.name}] register new serie ${tradeIdentifier}`)
          activeBars[tradeIdentifier].open = activeBars[tradeIdentifier].high = activeBars[tradeIdentifier].low = activeBars[
            tradeIdentifier
          ].close = +trade.price
        }

        activeBars[tradeIdentifier].high = Math.max(activeBars[tradeIdentifier].high, +trade.price)
        activeBars[tradeIdentifier].low = Math.min(activeBars[tradeIdentifier].low, +trade.price)
        activeBars[tradeIdentifier].close = +trade.price

        activeBars[tradeIdentifier]['c' + trade.side]++
        activeBars[tradeIdentifier]['v' + trade.side] += trade.price * trade.size
      }

      totalRange.from = Math.min(tradeFlooredTime, totalRange.from)
      totalRange.to = Math.max(tradeFlooredTime, totalRange.to)

      if (totalRange.pairs.indexOf(trade.pair) === -1) {
        totalRange.pairs.push(trade.pair)
      }

      if (totalRange.exchanges.indexOf(trade.exchange) === -1) {
        totalRange.exchanges.push(trade.exchange)
      }
    }

    const promises = []

    if (closedBars.length) {
      promises.push(
        this.influx.writePoints(
          closedBars.map((bar, index) => {
            const fields = {
              cbuy: bar.cbuy,
              csell: bar.csell,
              vbuy: bar.vbuy,
              vsell: bar.vsell,
              lbuy: bar.lbuy,
              lsell: bar.lsell,
            }

            if (bar.close !== null) {
              ;(fields.open = bar.open), (fields.high = bar.high), (fields.low = bar.low), (fields.close = bar.close)
            }

            return {
              measurement: 'trades' + (this.options.influxTimeframe ? '_' + getHms(this.options.influxTimeframe) : ''),
              tags: {
                market: bar.exchange + ':' + bar.pair,
              },
              fields: fields,
              timestamp: +bar.time,
            }
          }),
          {
            precision: 'ms',
          }
        )
      )
    }

    if (liquidations.length) {
      promises.push(
        this.influx
          .writePoints(
            liquidations.map((trade, index) => {
              const fields = {
                size: +trade.price,
                price: +trade.size,
                side: trade[4] == 1 ? 'buy' : 'sell',
              }

              return {
                measurement: 'liquidations',
                tags: {
                  market: trade.exchange + ':' + trade.pair,
                },
                fields: fields,
                timestamp: +trade.timestamp,
              }
            })
          )
          .catch((err) => {
            console.error('influx write failed (liquidation)', err.message)
          })
      )
    }

    return Promise.all(promises).then(() => ({
      ...totalRange,
    }))
  }

  fetch({ from, to, timeframe = 60000, markets = [] }) {
    const timeframeText = getHms(timeframe)

    let query = `SELECT * FROM "${this.options.influxDatabase}"."autogen"."trades_${timeframeText}" WHERE time >= ${from}ms AND time < ${to}ms`

    if (markets.length) {
      query += ` AND market =~ /${markets.join('|').replace(/\//g, '\\/')}/`
    }

    console.log(query)

    return this.influx
      .query(query, {
        precision: 's',
      })
      .catch((err) => {
        console.error(
          `[storage/${this.name}] failed to retrieves trades between ${from} and ${to} with timeframe ${timeframe}\n\t`,
          err.message
        )
      })
  }
}

module.exports = InfluxStorage
