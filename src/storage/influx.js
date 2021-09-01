const Influx = require('influx')
const { getHms, sleep } = require('../helper')

require('../typedef')

class InfluxStorage {
  constructor(options) {
    this.name = this.constructor.name
    this.format = 'point'

    /**
     * @type {{[identifier: string]: number}}
     */
    this.lastClose = {}

    /**
     * @type {{[identifier: string]: Bar}}
     */
    this.lastBar = {}

    /**
     * @type {{[identifier: string]: Bar[]}}
     */
    this.pendingBars = {}

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

    try {
      this.influx = new Influx.InfluxDB({
        host: host || 'localhost',
        port: port || '8086',
        database: this.options.influxDatabase,
      })

      const databases = await this.influx.getDatabaseNames()

      if (!databases.includes(this.options.influxDatabase)) {
        await this.influx.createDatabase(this.options.influxDatabase)
      }

      await this.ensureRetentionPolicies()
      await this.getPreviousCloses()
    } catch (error) {
      console.error(`[storage/${this.name}] ${error.message}... retrying in 1s`)

      await sleep()

      return this.connect()
    }
  }

  /**
   *
   */
  async ensureRetentionPolicies() {
    const retentionsPolicies = (await this.influx.showRetentionPolicies()).reduce((output, retentionPolicy) => {
      output[retentionPolicy.name] = retentionPolicy.duration
      return output
    }, {})

    const timeframes = [this.options.influxTimeframe].concat(this.options.influxResampleTo)

    for (let timeframe of timeframes) {
      const rpDuration = timeframe * this.options.influxRetentionPerTimeframe // 5000 bars
      const rpDurationLitteral = getHms(rpDuration, true)
      const rpName = this.options.influxRetentionPrefix + getHms(timeframe)

      if (!retentionsPolicies[rpName]) {
        console.log(`[storage/${this.name}] create retention policy ${rpName} (duration ${rpDurationLitteral})`)
        await this.influx.createRetentionPolicy(rpName, {
          database: this.options.influxDatabase,
          duration: rpDurationLitteral,
          replication: 1,
        })
      }

      delete retentionsPolicies[rpName]
    }

    for (let rpName in retentionsPolicies) {
      if (rpName.indexOf(this.options.influxRetentionPrefix) === 0) {
        console.warn(`[storage/${this.name}] unused retention policy ? (${rpName})`)
        // await this.influx.dropRetentionPolicy(rpName, this.options.influxDatabase)
        // just warning now because of multiple instances of aggr-server running with different RPs
      }
    }

    this.baseRp = this.options.influxRetentionPrefix + getHms(this.options.influxTimeframe)
  }

  /**
   * Create continuous queries if not exists
   *
   * @memberof InfluxStorage
   */
  async resample(range) {
    let sourceTimeframeLitteral
    let destinationTimeframeLitteral

    this.options.influxResampleTo.sort((a, b) => a - b)

    for (let timeframe of this.options.influxResampleTo) {
      const flooredRange = {
        from: Math.floor(range.from / timeframe) * timeframe,
        to: Math.floor(range.to / timeframe) * timeframe + timeframe,
      }

      for (let i = this.options.influxResampleTo.indexOf(timeframe); i >= 0; i--) {
        if (timeframe <= this.options.influxResampleTo[i] || timeframe % this.options.influxResampleTo[i] !== 0) {
          if (i === 0) {
            sourceTimeframeLitteral = getHms(this.options.influxTimeframe)
          }
          continue
        }

        sourceTimeframeLitteral = getHms(this.options.influxResampleTo[i])
        break
      }

      destinationTimeframeLitteral = getHms(timeframe)

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

      const query_from = `${this.options.influxDatabase}.${this.options.influxRetentionPrefix}${sourceTimeframeLitteral}.${this.options.influxMeasurement}_${sourceTimeframeLitteral}`
      const query_into = `${this.options.influxDatabase}.${this.options.influxRetentionPrefix}${destinationTimeframeLitteral}.${this.options.influxMeasurement}_${destinationTimeframeLitteral}`
      const coverage = `WHERE time >= ${flooredRange.from}ms AND time < ${flooredRange.to}ms AND market =~ /${range.markets
        .join('|')
        .replace(/\//g, '\\/')}/`
      const group = `GROUP BY time(${destinationTimeframeLitteral}), market fill(none)`

      await this.executeQuery(`${query} INTO ${query_into} FROM ${query_from} ${coverage} ${group}`)
    }
  }

  getPreviousCloses() {
    const timeframeLitteral = getHms(this.options.influxTimeframe)

    this.influx
      .query(
        `SELECT close FROM ${this.options.influxRetentionPrefix}${timeframeLitteral}.${this.options.influxMeasurement}${
          '_' + timeframeLitteral
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
  async save(trades, forceImport) {
    if (!trades || !trades.length) {
      return Promise.resolve()
    }

    this.processTrades(trades)

    const now = +new Date()
    const timeBackupFloored = Math.floor(now / this.options.backupInterval) * this.options.backupInterval
    const timeMinuteFloored = Math.floor(now / (this.options.influxResampleInterval)) * (this.options.influxResampleInterval)

    if (forceImport || timeBackupFloored === timeMinuteFloored) {
      const resampleRange = await this.importPendingBars()
      await this.resample(resampleRange)
    }
  }

  /**
   * close a bar (register close + reference for next bar)
   * @param {Bar} bar
   */
  closeBar(bar) {
    if (typeof bar.close === 'number') {
      // reg close for next bar
      this.lastClose[bar.market] = bar.close
    }

    this.lastBar[bar.market] = bar

    return this.lastBar[bar.market]
  }

  /**
   * Import and clear pending bars
   *
   * @returns {Promise<{
      from: number,
      to: number,
      markets: string[],
    }>}
   * @memberof InfluxStorage
   */
  async importPendingBars() {
    /**
     * closed bars
     * @type {Bar[]}
     */
    const barsToImport = []

    /**
     * Total range of import
     * @type {TimeRange}
     */
    const importedRange = {
      from: Infinity,
      to: 0,
      markets: [],
    }

    for (const identifier in this.pendingBars) {
      for (let i = 0; i < this.pendingBars[identifier].length; i++) {
        const bar = this.pendingBars[identifier][i]

        importedRange.from = Math.min(bar.time, importedRange.from)
        importedRange.to = Math.max(bar.time, importedRange.to)

        if (importedRange.markets.indexOf(identifier) === -1) {
          importedRange.markets.push(identifier)
        }

        barsToImport.push(this.pendingBars[identifier].shift())
        i--
      }
    }

    this.pendingBars = {}

    console.log('import ' + barsToImport.length + ', no more pending bars')

    if (barsToImport.length) {
      await this.writePoints(
        barsToImport.map((bar, index) => {
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
            measurement: 'trades_' + getHms(this.options.influxTimeframe),
            tags: {
              market: bar.market,
            },
            fields: fields,
            timestamp: +bar.time,
          }
        }),
        {
          precision: 'ms',
          retentionPolicy: this.baseRp,
        }
      )
    }

    return importedRange
  }

  /**
   * Trades into bars (pending bars)
   *
   * @param {Trade[]} trades
   * @returns {Promise<{
      from: number,
      to: number,
      markets: string[],
    }>}
   * @memberof InfluxStorage
   */
  async processTrades(trades) {
    /**
     * Current bars
     * @type {{[identifier: string]: Bar}}
     */
    const activeBars = {}

    for (let i = 0; i <= trades.length; i++) {
      const trade = trades[i]

      let tradeIdentifier
      let tradeFlooredTime

      if (!trade) {
        // end of loop reached = close all bars
        for (let barIdentifier in activeBars) {
          this.closeBar(activeBars[barIdentifier])

          delete activeBars[barIdentifier]
        }

        break
      } else {
        tradeIdentifier = trade.exchange + ':' + trade.pair

        tradeFlooredTime = Math.floor(trade.timestamp / this.options.influxTimeframe) * this.options.influxTimeframe

        if (!activeBars[tradeIdentifier] || activeBars[tradeIdentifier].time < tradeFlooredTime) {
          if (activeBars[tradeIdentifier]) {
            // close bar required
            this.closeBar(activeBars[tradeIdentifier])

            delete activeBars[tradeIdentifier]
          }

          if (trade) {
            // create bar required

            if (!this.pendingBars[tradeIdentifier]) {
              this.pendingBars[tradeIdentifier] = []
            }

            if (
              this.pendingBars[tradeIdentifier].length &&
              this.pendingBars[tradeIdentifier][this.pendingBars[tradeIdentifier].length - 1].time === tradeFlooredTime
            ) {
              activeBars[tradeIdentifier] = this.pendingBars[tradeIdentifier][this.pendingBars[tradeIdentifier].length - 1]
            } else if (this.lastBar[tradeIdentifier] && this.lastBar[tradeIdentifier].time === tradeFlooredTime) {
              // trades passed in save() contains some of the last batch (trade time = last bar time)
              // recover exchange point of lastbar
              this.pendingBars[tradeIdentifier].push(this.lastBar[tradeIdentifier])
              activeBars[tradeIdentifier] = this.pendingBars[tradeIdentifier][this.pendingBars[tradeIdentifier].length - 1]
            } else {
              // create new bar
              this.pendingBars[tradeIdentifier].push({
                time: tradeFlooredTime,
                market: tradeIdentifier,
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
              })

              activeBars[tradeIdentifier] = this.pendingBars[tradeIdentifier][this.pendingBars[tradeIdentifier].length - 1]

              if (typeof this.lastClose[tradeIdentifier] === 'number') {
                // this bar open = last bar close (from last save or getReferencePoint on startup)
                activeBars[tradeIdentifier].open =
                  activeBars[tradeIdentifier].high =
                  activeBars[tradeIdentifier].low =
                  activeBars[tradeIdentifier].close =
                    this.lastClose[tradeIdentifier]
              }
            }
          }
        }
      }

      if (trade.liquidation) {
        // trade is a liquidation
        activeBars[tradeIdentifier]['l' + trade.side] += trade.price * trade.size
      } else {
        if (activeBars[tradeIdentifier].open === null) {
          // new bar without close in db, should only happen once
          console.log(`[storage/${this.name}] register new serie ${tradeIdentifier}`)
          activeBars[tradeIdentifier].open =
            activeBars[tradeIdentifier].high =
            activeBars[tradeIdentifier].low =
            activeBars[tradeIdentifier].close =
              +trade.price
        }

        activeBars[tradeIdentifier].high = Math.max(activeBars[tradeIdentifier].high, +trade.price)
        activeBars[tradeIdentifier].low = Math.min(activeBars[tradeIdentifier].low, +trade.price)
        activeBars[tradeIdentifier].close = +trade.price

        activeBars[tradeIdentifier]['c' + trade.side]++
        activeBars[tradeIdentifier]['v' + trade.side] += trade.price * trade.size
      }
    }
  }

  async writePoints(points, options, attempt = 0) {
    if (!points.length) {
      return
    }

    const measurement = points[0].measurement
    const from = points[0].timestamp
    const to = points[points.length - 1].timestamp

    try {
      await this.influx.writePoints(points, options)

      if (attempt > 0) {
        console.debug(`[storage/${this.name}] successfully wrote points after ${attempt} attempt(s)`)
      }
    } catch (error) {
      attempt++

      console.error(
        `[storage/${this.name}] write points failed (${attempt}${
          attempt === 1 ? 'st' : attempt === 2 ? 'nd' : attempt === 3 ? 'rd' : 'th'
        } attempt)`,
        error.message
      )

      if (attempt > 5) {
        console.error(
          `too many attemps at writing points\n\n${measurement}, ${new Date(from).toUTCString()} to ${new Date(
            to
          ).toUTCString()}\n\t-> abort`
        )
        throw error
      }

      await sleep(500)

      return this.writePoints(points, options, attempt)
    }
  }

  async executeQuery(query, attempt = 0) {
    try {
      await this.influx.query(query)

      if (attempt > 0) {
        console.debug(`[storage/${this.name}] successfully executed query ${attempt} attempt(s)`)
      }
    } catch (error) {
      attempt++

      console.error(
        `[storage/${this.name}] query failed (${attempt}${
          attempt === 1 ? 'st' : attempt === 2 ? 'nd' : attempt === 3 ? 'rd' : 'th'
        } attempt)`,
        error.message
      )

      if (attempt > 5) {
        console.error(`too many attemps at executing query\n\n${query}\n\t-> abort`)
        throw error
      }

      await sleep(500)

      return this.executeQuery(query, attempt)
    }
  }

  fetch({ from, to, timeframe = 60000, markets = [] }) {
    const timeframeLitteral = getHms(timeframe)

    let query = `SELECT * FROM "${this.options.influxDatabase}"."${this.options.influxRetentionPrefix}${timeframeLitteral}"."trades_${timeframeLitteral}" WHERE time >= ${from}ms AND time < ${to}ms`

    if (markets.length) {
      query += ` AND market =~ /${markets.join('|').replace(/\//g, '\\/')}/`
    }

    return this.influx
      .queryRaw(query, {
        precision: 's',
        epoch: 's',
      })
      .then((results) => {
        let injectedPendingBars = []

        for (const market of markets) {
          if (this.pendingBars[market] && this.pendingBars[market].length) {
            for (const bar of this.pendingBars[market]) {
              if (bar.time >= from && bar.time <= to) {
                injectedPendingBars.push(bar)
              }
            }
          }
        }

        injectedPendingBars = injectedPendingBars.sort((a, b) => a.time - b.time)

        if (!results.results[0].series) {
          return []
        }

        return results.results[0].series[0].values.concat(injectedPendingBars)
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
