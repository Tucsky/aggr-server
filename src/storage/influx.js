const Influx = require('influx')
const { getHms, sleep, ID } = require('../helper')
const net = require('net')
const { statSync, unlinkSync, cp } = require('fs')
const webPush = require('web-push')
const persistence = require('../persistence')

require('../typedef')

const DAY = 1000 * 60 * 60 * 24

class InfluxStorage {
  constructor(options) {
    this.name = this.constructor.name
    this.format = 'point'

    /**
     * @type {net.Socket}
     */
    this.clusterSocket = null

    /**
     * @type {net.Server}
     */
    this.serverSocket = null

    /**
     * @type {net.Socket[]}
     */
    this.clusteredCollectors = []

    /**
     * @type {{[pendingBarsRequestId: string]: (bars: Bar[]) => void}}
     */
    this.promisesOfPendingBars = {}

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

    /**
     * @type {{[identifier: string]: any[]}}
     */
    this.alerts = {}

    /**
     * @type {{[endpoint: string]: any}}
     */
    this.alertEndpoints = {}

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

    console.log(`[storage/influx] connecting to ${host}:${port}`)

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

      if (this.options.collect) {
        await this.ensureRetentionPolicies()
        await this.getPreviousCloses()
      }
    } catch (error) {
      console.error(`[storage/influx] ${error.message}... retrying in 1s`)

      await sleep()

      return this.connect()
    } finally {
      if (this.options.influxCollectors) {
        if (this.options.api && !this.options.collect) {
          // CLUSTER NODE (node is dedicated to serving data)
          this.createCluster()

          return
        } else if (!this.options.api && this.options.collect) {
          // COLLECTOR NODE (node is just collecting + storing data)

          if (this.options.privateVapidKey && (typeof this.options.id === 'undefined' || this.options.id === null)) {
            console.error(`[storage/alerts] push subscriptions won't be persisted (NEED ID COLLECTORS)`)
          }

          this.connectToCluster()
        }
      }

      this.getAlerts()
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
      const rpDuration = timeframe * this.options.influxRetentionPerTimeframe
      const rpDurationLitteral = getHms(rpDuration, true)
      const rpName = this.options.influxRetentionPrefix + getHms(timeframe)

      if (!retentionsPolicies[rpName]) {
        console.log(`[storage/influx] create retention policy ${rpName} (duration ${rpDurationLitteral})`)
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
        console.warn(`[storage/influx] unused retention policy ? (${rpName})`)
        // await this.influx.dropRetentionPolicy(rpName, this.options.influxDatabase)
        // just warning now because of multiple instances of aggr-server running with different RPs
      }
    }

    this.baseRp = this.options.influxRetentionPrefix + getHms(this.options.influxTimeframe)
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
   * Process the trades into bars of minimum tf
   * And occasionaly writes into db
   * Triggered every options.backupInterval
   *
   * @param {Trade[]} trades
   * @param {boolean} isExiting
   * @returns
   */
  async save(trades, isExiting) {
    if (isExiting) {
      await this.peristAlerts(true)

      if (this.clusterSocket) {
        console.log('[storage/influx/collector] closing cluster connection')
        await new Promise((resolve) => {
          this.clusterSocket.end(() => {
            console.log('[storage/influx/collector] successfully closed cluster connection')
            resolve()
          })
        })
      }
    }

    if (!trades || !trades.length) {
      return Promise.resolve()
    }

    // convert the trades into bars (bars tf = minimum tf)
    this.processTrades(trades)

    if (isExiting) {
      // always write when exiting
      return this.import()
    }

    if (!this.clusterSocket) {
      // here the node decide when to write in db
      // otherwise cluster will send a command for that (to balance write tasks between collectors nodes)

      const now = Date.now()
      const timeBackupFloored = Math.floor(now / this.options.backupInterval) * this.options.backupInterval
      const timeMinuteFloored = Math.floor(now / this.options.influxResampleInterval) * this.options.influxResampleInterval

      if (timeBackupFloored === timeMinuteFloored) {
        return this.import()
      }
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

    let debugPush = {
      count: 0,
      market: null,
      lastTime: null,
    }

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

            const debugWrite = /BINANCE_FUTURES.*btcusdt/.test(tradeIdentifier)

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
              if (debugWrite) {
                debugPush.count++
                debugPush.market = tradeIdentifier
                debugPush.lastTime = tradeFlooredTime
              }
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
          console.log(`[storage/influx] register new serie ${tradeIdentifier} (last close was unknown)`)
          activeBars[tradeIdentifier].open =
            activeBars[tradeIdentifier].high =
            activeBars[tradeIdentifier].low =
            activeBars[tradeIdentifier].close =
              +trade.price
        }

        activeBars[tradeIdentifier].high = Math.max(activeBars[tradeIdentifier].high, +trade.price)
        activeBars[tradeIdentifier].low = Math.min(activeBars[tradeIdentifier].low, +trade.price)
        activeBars[tradeIdentifier].close = +trade.price

        activeBars[tradeIdentifier]['c' + trade.side] += trade.count || 1
        activeBars[tradeIdentifier]['v' + trade.side] += trade.price * trade.size
      }
    }

    if (debugPush) {
      console.log(
        `[storage/${this.name}] push ${debugPush.count} ${getHms(this.options.influxTimeframe)} bar for market ${
          debugPush.market
        } (last time ${new Date(debugPush.lastTime).toISOString().split('T').pop()})`
      )
    }
  }

  /**
   * Import pending bars (minimum tf bars) and resample into bigger timeframes
   */
  async import() {
    let now = Date.now()
    let before = now

    console.log(`[storage/influx/import] import start`)

    const resampleRange = await this.importPendingBars()

    if (resampleRange.to - resampleRange.from >= 0) {
      await this.resample(resampleRange)
    }

    now = Date.now()
    console.log(`[storage/influx/import] import end (took ${getHms(now - before, true)})`)
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
    const now = Date.now()

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

    // free up realtime bars
    this.pendingBars = {}

    const pricesRanges = {}

    let debugImport = {
      count: 0,
      market: null,
      lastTime: null,
    }

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
            if (this.alerts[bar.market]) {
              if (!pricesRanges[bar.market]) {
                pricesRanges[bar.market] = {
                  high: -Infinity,
                  low: Infinity,
                }

                pricesRanges[bar.market].open = bar.open
              }

              pricesRanges[bar.market].high = Math.max(bar.high, pricesRanges[bar.market].high)
              pricesRanges[bar.market].low = Math.min(bar.low, pricesRanges[bar.market].low)
              pricesRanges[bar.market].close = bar.close
            }

            ;(fields.open = bar.open), (fields.high = bar.high), (fields.low = bar.low), (fields.close = bar.close)
          }

          if (/BINANCE_FUTURES.*btcusdt/.test(bar.market)) {
            debugImport.count++
            debugImport.lastTime = bar.time
            debugImport.market = bar.market
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

    if (debugImport.count) {
      console.log(
        `[storage/${this.name}] import ${debugImport.count} ${getHms(this.options.influxTimeframe)} bar for market ${
          debugImport.market
        } (last time ${new Date(debugImport.lastTime).toISOString().split('T').pop()})`
      )
    }

    for (const market in pricesRanges) {
      const { high, low } = pricesRanges[market]

      let expired = 0

      for (let i = 0; i < this.alerts[market].length; i++) {
        const alert = this.alerts[market][i]

        if (now - alert.timestamp < this.options.influxResampleInterval) {
          continue
        }

        const isTriggered = alert.price <= high && alert.price >= low
        const isExpired = !isTriggered && alert.timestamp + this.options.alertExpiresAfter < now

        if (isTriggered || isExpired) {
          if (isTriggered) {
            this.sendAlert(alert, alert.close > alert.open, now - alert.timestamp)
          } else {
            expired++
          }

          if (this.unregisterAlert(alert, true)) {
            i--
          }
        }
      }

      if (expired) {
        console.log(`[alert] removed ${expired} expired alerts on ${market}`)
      }
    }

    return importedRange
  }

  /**
   * Wrapper for write
   * Write points into db
   * Called from importPendingBars
   *
   * @param {Influx.IPoint[]} points
   * @param {Influx.IWriteOptions} options
   * @param {number?} attempt no of attempt starting at 0 (abort if too much failed attempts)
   * @returns
   */
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
        console.debug(`[storage/influx] successfully wrote points after ${attempt} attempt(s)`)
      }
    } catch (error) {
      attempt++

      console.error(
        `[storage/influx] write points failed (${attempt}${
          attempt === 1 ? 'st' : attempt === 2 ? 'nd' : attempt === 3 ? 'rd' : 'th'
        } attempt)`,
        error.message
      )

      if (attempt >= 5) {
        console.error(
          `too many attemps at writing points\n\n${measurement}, ${new Date(from).toUTCString()} to ${new Date(
            to
          ).toUTCString()}\n\t-> abort`
        )
        throw error.message
      }

      await sleep(500)

      return this.writePoints(points, options, attempt)
    }
  }

  /**
   * Start from minimum tf (influxTimeframe) and update all timeframes above it (influxResampleTo)
   * 10s into 30s, 30s into 1m, 1m into 3m, 1m into 5m, 5m into 15m, 3m into 21m, 15m into 30m etc
   *
   * @memberof InfluxStorage
   */
  async resample(range) {
    let sourceTimeframeLitteral
    let destinationTimeframeLitteral

    let now = Date.now()
    let before = now

    console.debug(`[storage/influx/resample] resampling ${range.markets.length} markets`)

    this.options.influxResampleTo.sort((a, b) => a - b)

    let bars = 0

    for (let timeframe of this.options.influxResampleTo) {
      const isOddTimeframe = DAY % timeframe !== 0 && timeframe < DAY

      let flooredRange

      if (isOddTimeframe) {
        const dayOpen = Math.floor(range.from / DAY) * DAY
        flooredRange = {
          from: dayOpen + Math.floor((range.from - dayOpen) / timeframe) * timeframe,
          to: dayOpen + Math.floor((range.to - dayOpen) / timeframe) * timeframe + timeframe,
        }
      } else {
        flooredRange = {
          from: Math.floor(range.from / timeframe) * timeframe,
          to: Math.floor(range.to / timeframe) * timeframe + timeframe,
        }
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

      let coverage = `WHERE time >= ${flooredRange.from}ms AND time < ${flooredRange.to}ms`
      coverage += ` AND (${range.markets.map((market) => `market = '${market}'`).join(' OR ')})`

      const group = `GROUP BY time(${destinationTimeframeLitteral}${
        isOddTimeframe ? ', ' + getHms(flooredRange.from % timeframe) : ''
      }), market fill(none)`

      bars += (flooredRange.to - flooredRange.from) / timeframe

      await this.executeQuery(`${query} INTO ${query_into} FROM ${query_from} ${coverage} ${group}`)
    }

    now = Date.now()

    console.debug(
      `[storage/influx/resample] done resampling ${parseInt((now - before) / bars)}ms per bar (${parseInt(
        now - before
      )}ms for ${bars} bars)`
    )
  }

  /**
   * Wrapper for query
   * Query the db
   * Called from resample
   *
   * @param {string} query
   * @param {number?} attempt no of attempt starting at 0 (abort if too much failed attempts)
   * @returns
   */
  async executeQuery(query, attempt = 0) {
    try {
      await this.influx.query(query)

      if (attempt > 0) {
        console.debug(`[storage/influx] successfully executed query ${attempt} attempt(s)`)
      }
    } catch (error) {
      attempt++

      console.error(
        `[storage/influx] query failed (${attempt}${attempt === 1 ? 'st' : attempt === 2 ? 'nd' : attempt === 3 ? 'rd' : 'th'} attempt)`,
        error.message
      )

      if (attempt >= 5) {
        console.error(`too many attemps at executing query\n\n${query}\n\t-> abort`)
        throw error.message
      }

      await sleep(500)

      return this.executeQuery(query, attempt)
    }
  }

  /**
   * Called from main
   * API user called method
   *
   * @returns
   */
  fetch({ from, to, timeframe = 60000, markets = [] }) {
    const timeframeLitteral = getHms(timeframe)

    let query = `SELECT * FROM "${this.options.influxDatabase}"."${this.options.influxRetentionPrefix}${timeframeLitteral}"."trades_${timeframeLitteral}" WHERE time >= ${from}ms AND time < ${to}ms`

    if (markets.length) {
      query += ` AND (${markets.map((market) => `market = '${market}'`).join(' OR ')})`
    }

    return this.influx
      .queryRaw(query, {
        precision: 's',
        epoch: 's',
      })
      .then((results) => {
        if (to > +new Date() - this.options.influxResampleInterval) {
          return this.concatWithPendingBars(results.results[0].series ? results.results[0].series[0].values : [], markets, from, to)
        } else if (results.results[0].series) {
          return results.results[0].series[0].values
        } else {
          return []
        }
      })
      .catch((err) => {
        console.error(`[storage/influx] failed to retrieves trades between ${from} and ${to} with timeframe ${timeframe}\n\t`, err.message)
      })
  }

  /**
   * Concat given results of bars with realtime bars (pending bars)
   * If clustering enabled, use collectors as source of pending bars
   * Otherwise current node pending bars will be used
   * @param {number[][]} bars
   * @param {string[]} markets
   * @param {number} from
   * @param {number} to
   * @returns
   */
  concatWithPendingBars(bars, markets, from, to) {
    if (this.options.influxCollectors && this.clusteredCollectors.length) {
      // use collectors nodes pending bars
      return this.requestPendingBars(markets, from, to).then((pendingBars) => {
        return bars.concat(pendingBars)
      })
    } else {
      // use current node pending bars
      let injectedPendingBars = []

      for (const market of markets) {
        if (this.pendingBars[market] && this.pendingBars[market].length) {
          for (const bar of this.pendingBars[market]) {
            if (bar.time >= from && bar.time < to) {
              injectedPendingBars.push(bar)
            }
          }
        }
      }

      injectedPendingBars = injectedPendingBars.sort((a, b) => a.time - b.time)

      return bars.concat(injectedPendingBars)
    }
  }

  /**
   * Called from a collector node
   * Connect current collector node to cluster node
   * WILL try to reconnect if it fails
   * @returns {void}
   */
  connectToCluster() {
    if (this.clusterSocket) {
      console.warn('[storage/influx/collector] already connected (aborting)')
      return
    }

    console.debug('[storage/influx/collector] connecting to cluster..')

    this.clusterSocket = net.createConnection(this.options.influxCollectorsClusterSocketPath)

    this.clusterSocket.on('connect', () => {
      console.log('[storage/influx/collector] successfully connected to cluster')
      this.clusterSocket.write(JSON.stringify(this.options.pairs) + '#')
    })

    // store current incoming to be filled by potentialy partial chunks
    this.pendingSocketData = ''

    this.clusterSocket
      .on(
        'data',
        this.parseSocketData.bind(this, (data) => {
          if (data.pendingBarsRequestId) {
            // this is a request for pending bars
            this.emitPendingBars(data.pendingBarsRequestId, data.markets, data.from, data.to)
          } else if (data.op === 'import') {
            // cluster is asking collector to write
            this.import().finally(() => {
              if (this.clusterSocket) {
                this.clusterSocket.write(
                  JSON.stringify({
                    op: 'import',
                  }) + '#'
                )
              }
            })
          } else if (data.op === 'toggleAlert') {
            this.toggleAlert(data.alert, true)
          }
        })
      )
      .on('close', () => {
        // collector never close connection with cluster by itself
        console.error('[storage/influx/collector] cluster closed (unexpectedly)')

        // schedule reconnection
        this.reconnectCluster()
      })
      .on('error', (error) => {
        // the close even destroy the previous strem and may trigger error
        // reconnect in this situation as well
        this.reconnectCluster()
      })
  }

  /**
   * Handle connectToCluster failure and unexpected close
   * @returns {void}
   */
  reconnectCluster() {
    if (this.clusterSocket) {
      // ensure previous stream is donezo
      this.clusterSocket.destroy()
      this.clusterSocket = null
    }

    if (this._clusterConnectionTimeout) {
      clearTimeout(this._clusterConnectionTimeout)
    } else {
      console.log(`[storage/influx/collector] schedule reconnect to cluster (${this.options.influxCollectorsReconnectionDelay / 1000}s)`)
    }

    this._clusterConnectionTimeout = setTimeout(() => {
      this._clusterConnectionTimeout = null

      this.connectToCluster()
    }, this.options.influxCollectorsReconnectionDelay)
  }

  /**
   * Create cluster unix socket
   * And listen for collectors joining
   *
   * Only called once
   */
  createCluster() {
    try {
      if (statSync(this.options.influxCollectorsClusterSocketPath)) {
        console.debug(`[storage/influx/cluster] unix socket was not closed properly last time`)
        unlinkSync(this.options.influxCollectorsClusterSocketPath)
      }
    } catch (error) {}

    this.serverSocket = net.createServer((socket) => {
      console.log('[storage/influx/cluster] collector connected successfully')

      socket.on('end', () => {
        console.log('[storage/influx/cluster] collector disconnected (unexpectedly)')

        const index = this.clusteredCollectors.indexOf(socket)

        if (index !== -1) {
          this.clusteredCollectors.splice(index, 1)
        } else {
          console.error(`[storage/influx/cluster] disconnected collector WAS NOT found in collectors array`)
        }

        socket.destroy()
      })

      // store current incoming to be filled by potentialy partial chunks
      this.pendingSocketData = ''

      socket.on(
        'data',
        this.parseSocketData.bind(this, (data) => {
          if (!socket.markets) {
            // this is our welcome message
            socket.markets = data

            console.log('[storage/influx/cluster] registered collector with markets', socket.markets.join(', '))

            this.clusteredCollectors.push(socket)
          } else if (data.pendingBarsRequestId) {
            // this is a pending data response to cluster

            if (this.promisesOfPendingBars[data.pendingBarsRequestId]) {
              this.promisesOfPendingBars[data.pendingBarsRequestId](data.results)
            } else {
              console.error('there was no promisesOfPendingBars with given pendingBarsRequestId', data.pendingBarsRequestId)
            }
          } else if (data.op === 'import') {
            // this is a import response to cluster

            if (this.promiseOfImport) {
              this.promiseOfImport() // trigger next import (if any)
            }
          }
        })
      )
    })

    this.serverSocket.on('error', (error) => {
      console.error(`[storage/influx/cluster] server socket error`, error)
    })

    this.serverSocket.listen(this.options.influxCollectorsClusterSocketPath)
    setTimeout(this.importCollectors.bind(this), this.options.influxResampleInterval)
  }

  parseSocketData(callback, data) {
    // data is a stringified json inside a buffer
    // BUT it can also be a part of a json, or contain multiple

    // convert to string
    const stringData = data.toString()

    // complete data has a # char at it's end
    const incompleteData = stringData[stringData.length - 1] !== '#'

    if (stringData.indexOf('#') !== -1) {
      // data has delimiter

      // split chunks using given delimiter
      const chunks = stringData.split('#')

      for (let i = 0; i < chunks.length; i++) {
        if (!chunks[i].length) {
          // chunk is empty (last one can be as # used as divider:
          // partial_chunk#complete_chunk#*empty_chunk <-)
          // complete_chunk#complete_chunk#*empty_chunk <-)
          // partial_chunk#*empty_chunk <-)
          // complete_chunk#*empty_chunk <-)
          continue
        }

        // add to already existing incoming data (if i not last: this is a end of chunk)
        this.pendingSocketData += chunks[i]

        if (i === chunks.length - 1 && incompleteData) {
          // last chunk and incomplete
          // wait for next data event
          continue
        }

        // this is a complete chunk either because i < last OR last and # at this end of the total stringData
        let json

        try {
          json = JSON.parse(this.pendingSocketData)
        } catch (error) {
          console.error('[storage/influx] failed to parse socket data', error.message, this.pendingSocketData)
        }

        if (json) {
          try {
            callback(json)
          } catch (error) {
            console.error('[storage/influx] failed to execute callback data', error.message, json)
          }
        }

        // flush incoming data for next chunk
        this.pendingSocketData = ''
      }
    } else {
      // no delimiter in payload so this *has* to be incomplete data
      this.pendingSocketData += stringData
    }
  }

  async importCollectors() {
    for (const collector of this.clusteredCollectors) {
      await new Promise((resolve) => {
        let importTimeout = setTimeout(() => {
          console.error('[storage/influx/cluster] collector import was resolved early (5s timeout fired)')
          importTimeout = null
          resolve()
        }, 5000)

        this.promiseOfImport = () => {
          if (importTimeout) {
            clearTimeout(importTimeout)
            resolve()
          }
        }

        collector.write(JSON.stringify({ op: 'import' }) + '#')
      })
    }

    setTimeout(this.importCollectors.bind(this), this.options.influxResampleInterval)
  }

  /**
   * Called from the cluster node
   * Return array of realtime bars matching the given criteras (markets, start time & end time)
   * This WILL query all nodes responsible of collecting trades for given markets
   * @param {net.Socket} markets
   * @param {number} from
   * @param {number} to
   */
  async requestPendingBars(markets, from, to) {
    const collectors = []

    for (let i = 0; i < markets.length; i++) {
      for (let j = 0; j < this.clusteredCollectors.length; j++) {
        if (collectors.indexOf(this.clusteredCollectors[j]) === -1 && this.clusteredCollectors[j].markets.indexOf(markets[i]) !== -1) {
          collectors.push(this.clusteredCollectors[j])
        }
      }
    }

    const promisesOfBars = []

    for (const collector of collectors) {
      promisesOfBars.push(this.requestCollectorPendingBars(collector, markets, from, to))
    }

    return [].concat.apply([], await Promise.all(promisesOfBars)).sort((a, b) => a.time - b.time)
  }

  /**
   * Called from the cluster node
   * Query specific collector node (socket) for realtime bars matching given criteras
   * @param {net.Socket} collector
   * @param {string[]} markets
   * @param {number} from
   * @param {number} to
   */
  async requestCollectorPendingBars(collector, markets, from, to) {
    return new Promise((resolve) => {
      const pendingBarsRequestId = ID()

      let promiseOfPendingBarsTimeout = setTimeout(() => {
        console.error('[storage/influx/cluster] promise of realtime bar timeout fired (pendingBarsRequestId: ' + pendingBarsRequestId + ')')

        // response empty array as we didn't got the expected bars...
        this.promisesOfPendingBars[pendingBarsRequestId]([])

        // invalidate timeout
        promiseOfPendingBarsTimeout = null
      }, 5000)

      // register promise
      this.promisesOfPendingBars[pendingBarsRequestId] = (pendingBars) => {
        if (promiseOfPendingBarsTimeout) {
          clearTimeout(promiseOfPendingBarsTimeout)
        }

        // unregister promise
        delete this.promisesOfPendingBars[pendingBarsRequestId]

        resolve(pendingBars)
      }

      collector.write(
        JSON.stringify({
          pendingBarsRequestId,
          markets,
          from,
          to,
        }) + '#'
      )
    })
  }

  emitPendingBars(pendingBarsRequestId, markets, from, to) {
    const results = []

    for (const market of markets) {
      if (this.pendingBars[market] && this.pendingBars[market].length) {
        for (const bar of this.pendingBars[market]) {
          if (bar.time >= from && bar.time <= to) {
            results.push(bar)
          }
        }
      }
    }

    this.clusterSocket.write(
      JSON.stringify({
        pendingBarsRequestId,
        results,
      }) + '#'
    )
  }

  toggleAlert(alert, fromCluster = false) {
    if (!fromCluster && this.options.influxCollectors && this.clusteredCollectors.length) {
      const collector = this.getCollectorByMarket(alert.market)

      if (!collector) {
        throw new Error('unsupported market')
      }

      collector.write(
        JSON.stringify({
          op: 'toggleAlert',
          alert: alert,
        }) + '#'
      )
    } else {
      if (!fromCluster && this.options.pairs.indexOf(alert.market) === -1) {
        throw new Error('unsupported market')
      }
      const activeAlert = this.getActiveAlert(alert)

      if (activeAlert) {
        activeAlert.user = alert.user

        if (typeof alert.newPrice === 'number') {
          this.moveAlert(activeAlert, alert.newPrice)
        } else if (alert.unsubscribe) {
          this.unregisterAlert(activeAlert)
        }

        return
      } else if (!alert.unsubscribe) {
        this.registerAlert(alert)
      }
    }
  }

  getActiveAlert(alert) {
    if (!this.alerts[alert.market]) {
      return null
    }

    return this.alerts[alert.market].find((activeAlert) => activeAlert.endpoint === alert.endpoint && activeAlert.price === alert.price)
  }

  registerAlert(alert) {
    if (!this.alerts[alert.market]) {
      this.alerts[alert.market] = []
    }

    if (!this.alertEndpoints[alert.endpoint]) {
      this.alertEndpoints[alert.endpoint] = {
        user: alert.user,
        endpoint: alert.endpoint,
        keys: alert.keys,
        count: 0,
      }
    }

    if (typeof alert.newPrice === 'number') {
      alert.price = alert.newPrice
    }

    this.alertEndpoints[alert.endpoint].count++

    console.debug(
      `[alert/${alert.user}] create alert ${alert.market} @ ${alert.price} (user now have ${
        this.alertEndpoints[alert.endpoint].count
      } active alerts)`
    )

    this.alerts[alert.market].push({
      endpoint: alert.endpoint,
      market: alert.market,
      price: alert.price,
      origin: alert.origin,
      timestamp: Date.now(),
    })

    return true
  }

  unregisterAlert(alert, wasAutomatic) {
    const index = this.alerts[alert.market].indexOf(alert)

    this.scheduleAlertsCleanup(alert)

    if (index !== -1) {
      let user
      let count = 0
      if (this.alertEndpoints[alert.endpoint]) {
        this.alertEndpoints[alert.endpoint].count--
        user = this.alertEndpoints[alert.endpoint].user
        count = this.alertEndpoints[alert.endpoint].count
      }

      if (wasAutomatic) {
        console.debug(`[alert/${user || 'unknown'}] server removed ${alert.market} @ ${alert.price} (user now have ${count} active alerts)`)
      } else {
        console.debug(
          `[alert/${user || 'unknown'}] user removed alert ${alert.market} @ ${alert.price} (user now have ${count} active alerts)`
        )
      }

      this.alerts[alert.market].splice(index, 1)

      return true
    }

    return false
  }

  moveAlert(alert, newPrice) {
    const index = this.alerts[alert.market].indexOf(alert)

    if (index !== -1) {
      this.alerts[alert.market].splice(index, 1)

      let count = 0

      if (this.alertEndpoints[alert.endpoint]) {
        count = this.alertEndpoints[alert.endpoint].count
      }

      console.debug(`[alert/${alert.user}] move alert on ${alert.market} @ ${alert.price} (user now have ${count} active alerts)`)
      
      const now = Date.now()

      alert.price = newPrice
      alert.timestamp = now

      this.alerts[alert.market].push(alert)
    }
  }

  async scheduleAlertsCleanup(alert) {
    setTimeout(() => {
      if (this.alerts[alert.market] && !this.alerts[alert.market].length) {
        delete this.alerts[alert.market]
      }

      if (this.alertEndpoints[alert.endpoint] && !this.alertEndpoints[alert.endpoint].count) {
        delete this.alertEndpoints[alert.endpoint]
      }
    }, 100)
  }

  async getAlerts() {
    const now = Date.now()
    this.alertEndpoints = (await persistence.get(this.options.id + '-alerts-endpoints')) || {}

    console.debug(`[alert] this node handle ${Object.keys(this.alertEndpoints).length} alert users`)

    let totalCount = 0
    let pairsCount = 0

    for (const pair of this.options.pairs) {
      const marketAlerts = await persistence.get(this.options.id + '-alerts-' + pair)

      if (marketAlerts) {
        pairsCount++

        for (let i = 0; i < marketAlerts.length; i++) {
          const alert = marketAlerts[i]
          if (!this.alertEndpoints[alert.endpoint]) {
            marketAlerts.splice(i--, 1)
            continue
          } else {
            const subscription = this.alertEndpoints[alert.endpoint]

            if (!subscription.found) {
              subscription.found = 1
            } else {
              subscription.found++
            }
          }
        }

        console.debug(`[alert] ${pair} has ${marketAlerts.length} alerts`)

        totalCount += marketAlerts.length

        this.alerts[pair] = marketAlerts
      }
    }

    for (const endpoint in this.alertEndpoints) {
      const subscription = this.alertEndpoints[endpoint]

      if (!subscription.found) {
        delete this.alertEndpoints[endpoint]
      }
    }

    console.log(`[alert] total ${totalCount} alerts across ${pairsCount} pairs`)

    this._persistAlertsTimeout = setTimeout(this.peristAlerts.bind(this), 1000 * 60 * 30 + Math.random() * 1000 * 60 * 30)
  }

  async peristAlerts(isExiting = false) {
    clearTimeout(this._persistAlertsTimeout)

    try {
      await persistence.set(this.options.id + '-alerts-endpoints', this.alertEndpoints)
    } catch (error) {
      console.error('[alert] persistence error (saving endpoints)', error.message)
    }

    let totalCount = 0
    let pairsCount = 0

    for (const market in this.alerts) {
      const count = this.alerts[market].length

      if (count) {
        totalCount += count
        pairsCount++
      }

      try {
        await persistence.set(this.options.id + '-alerts-' + market, this.alerts[market])
      } catch (error) {
        console.error('[alert] persistence error (saving alerts)', error.message)
      }
    }

    console.log(`[alert] save alerts in persistence (${totalCount} alerts across ${pairsCount} pairs)`)

    if (!isExiting) {
      this._persistAlertsTimeout = setTimeout(this.peristAlerts.bind(this), 1000 * 60 * 30 + Math.random() * 1000 * 60 * 30)
    }
  }

  sendAlert(alert, isGreen, elapsedTime) {
    if (!this.alertEndpoints[alert.endpoint]) {
      console.error(`[alert] attempted to send alert without matching endpoint`, alert)
      return
    }

    console.debug(
      `[alert/${this.alertEndpoints[alert.endpoint].user}] send alert ${alert.market} @ ${alert.price} (${getHms(elapsedTime)} after)`
    )

    alert.triggered = true

    const payload = JSON.stringify({
      title: `${alert.market}`,
      body: `Price crossed ${alert.price}`,
      origin: alert.origin,
      price: alert.price,
      market: alert.market,
    })

    return webPush
      .sendNotification(this.alertEndpoints[alert.endpoint], payload, {
        TTL: 60,
        vapidDetails: {
          subject: 'mailto: contact@aggr.trade',
          publicKey: this.options.publicVapidKey,
          privateKey: this.options.privateVapidKey,
        },
        contentEncoding: 'aes128gcm',
      })
      .catch((err) => {
        console.error(`[alert] push notification failure\n\t`, err.message, '| payload :')
        console.error(JSON.parse(payload))
      })
  }

  getCollectorByMarket(market) {
    for (let j = 0; j < this.clusteredCollectors.length; j++) {
      if (this.clusteredCollectors[j].markets.indexOf(market) !== -1) {
        return this.clusteredCollectors[j]
      }
    }
  }
}

module.exports = InfluxStorage
