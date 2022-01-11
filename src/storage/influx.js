const { InfluxDB, Point, FluxTableMetaData } = require('@influxdata/influxdb-client')
const { OrgsAPI, BucketsAPI } = require('@influxdata/influxdb-client-apis')
const { getHms, sleep, ID } = require('../helper')
const net = require('net')
const config = require('../config')
const socketService = require('../services/socket')
const alertService = require('../services/alert')
const { connections, updateIndexes } = require('../services/connections')

require('../typedef')

const DAY = 1000 * 60 * 60 * 24

class InfluxStorage {
  constructor() {
    this.name = this.constructor.name
    this.format = 'point'

    /**
     * @type {import('@influxdata/influxdb-client').QueryApi}
     */
    this.queryAPI = null

    /**
     * @type {{[identifier: string]: number}}
     */
    this.lastClose = {}

    /**
     * @type {{[pendingBarsRequestId: string]: (bars: Bar[]) => void}}
     */
    this.promisesOfPendingBars = {}

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
  }

  async connect() {
    if (/\-/.test(config.influxDatabase)) {
      throw new Error('dashes not allowed inside influxdb database')
    }

    console.log(`[storage/influx] connecting to ${this.options.influxUrl}`)

    try {
      this.influx = new InfluxDB({ url: this.options.influxUrl, token: this.options.influxToken })
      this.queryAPI = this.influx.getQueryApi(this.options.influxOrg)

      if (this.options.collect) {
        await this.prepareBuckets()
        await this.getPreviousCloses()
      }
    } catch (error) {
      console.error(`[storage/influx] ${error.message}... retrying in 1s`)

      await sleep()

      return this.connect()
    } finally {
      if (config.influxCollectors) {
        if (config.api) {
          this.bindCollectorsEvents()
        } else if (config.collect) {
          this.bindClusterEvents()
        }

        if (config.api && !config.collect) {
          // schedule import of all collectors every influxResampleInterval until the scripts die
          setTimeout(this.importCollectors.bind(this), config.influxResampleInterval)
        }
      }

      if (alertService) {
        this.bindAlertsEvents()
      }
    }
  }

  /**
   * listen for responses from collector node
   */
  bindCollectorsEvents() {
    socketService
      .on('import', () => {
        // response from import request

        if (this.promiseOfImport) {
          this.promiseOfImport() // trigger next import (if any)
        }
      })
      .on('requestPendingBars', (data) => {
        // response from pending bars request

        if (this.promisesOfPendingBars[data.pendingBarsRequestId]) {
          this.promisesOfPendingBars[data.pendingBarsRequestId](data.results)
        } else {
          console.error('[influx/cluster] there was no promisesOfPendingBars with given pendingBarsRequestId', data.pendingBarsRequestId)
        }
      })
  }

  /**
   * listen for request from cluster node
   */
  bindClusterEvents() {
    socketService
      .on('requestPendingBars', (data) => {
        // this is a request for pending bars from cluster
        const payload = {
          pendingBarsRequestId: data.pendingBarsRequestId,
          results: this.getPendingBars(data.markets, data.from, data.to),
        }

        socketService.clusterSocket.write(
          JSON.stringify({
            op: 'requestPendingBars',
            data: payload,
          }) + '#'
        )
      })
      .on('import', () => {
        // this is a request to import pending data

        this.import().finally(() => {
          if (socketService.clusterSocket) {
            socketService.clusterSocket.write(
              JSON.stringify({
                op: 'import',
              }) + '#'
            )
          }
        })
      })
  }

  /**
   * Listen for alerts change
   */
  bindAlertsEvents() {
    alertService.on('change', ({ market, price, user, type }) => {
      console.log('[influx/alert]', market, price, user, type)
      this.writePoints(
        [
          {
            measurement: 'alerts',
            tags: {
              market,
            },
            fields: {
              price,
              user,
              type,
            },
            timestamp: Date.now(),
          },
        ],
        {
          precision: 'ms',
        }
      )
    })
  }

  /**
   *
   * @param {BucketsAPI} bucketsAPI
   * @param {string} orgID
   * @param {string} name
   * @param {number} rpDuration in seconds
   * @returns
   */
  async createBucket(bucketsAPI, orgID, name, rpDuration) {
    const [, rp] = name.split('/')

    try {
      const buckets = await bucketsAPI.getBuckets({ orgID, name })

      if (buckets && buckets.buckets && buckets.buckets.length) {
        console.error(`[storage/influx/bucket] bucket ${name} already exists`)
        return
      }
    } catch (e) {
      if (!(e instanceof HttpError && e.statusCode == 404)) {
        throw e
      }
    }

    console.log(`[storage/influx/bucket] create bucket ${name}`)

    await bucketsAPI.postBuckets({
      body: {
        orgID,
        name,
        rp,
        retentionRules: [
          {
            type: 'expire',
            everySeconds: rpDuration,
          },
        ],
      },
    })
  }

  /**
   *
   */
  async prepareBuckets() {
    const orgsAPI = new OrgsAPI(this.influx)
    const organizations = await orgsAPI.getOrgs({ org: this.options.influxOrg })

    if (!organizations || !organizations.orgs || !organizations.orgs.length) {
      console.error(`No organization named "${this.options.influxOrg}" found!`)
    }

    const orgID = organizations.orgs[0].id
    console.log(`Using organization "${this.options.influxOrg}" identified by "${orgID}"`)

    console.log('*** Get buckets by name ***')
    const bucketsAPI = new BucketsAPI(this.influx)
    const existingBuckets = (await bucketsAPI.getBuckets({ orgID }))
      .buckets.map((a) => a.name)
      .filter((a) => a.indexOf('/') !== -1)

    const timeframes = [this.options.influxTimeframe].concat(this.options.influxResampleTo)

    for (let timeframe of timeframes) {
      const rpDuration = (timeframe * this.options.influxRetentionPerTimeframe) / 1000
      const bucketName = this.getBucketName(timeframe)

      const bucketIndex = existingBuckets.indexOf(bucketName)

      if (bucketIndex === -1) {
        await this.createBucket(bucketsAPI, orgID, bucketName, rpDuration)
      } else {
        existingBuckets.splice(bucketIndex, 1)
      }
    }

    for (const bucketName of existingBuckets) {
      console.warn(`[storage/influx] unused bucket ? (${bucketName})`)
    }

    this.baseBucket = this.getBucketName(this.options.influxTimeframe)
  }

  getBucketName(timeframe) {
    return this.options.influxDatabase + '/' + this.options.influxRetentionPrefix + getHms(timeframe)
  }

  async getPreviousCloses() {
    const bars = await this.queryAPI.collectLines(`
      from(bucket:"${this.baseBucket}") 
      |> range(start: -1d)
      |> filter(fn: (r) => r["_measurement"] == "trades_10s")
      |> filter(fn: (r) => r["_field"] == "close")
      |> last(column: "_time")
      |> map(fn: (r) => ({ market: r.market, close: r._value }))
    `)

    for (let i = 4; i < bars.length; i++) {
      const [, , , close, market] = bars[i].split(',')
      this.lastClose[market] = +close
    }
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
    if (!trades || !trades.length) {
      return Promise.resolve()
    }

    // convert the trades into bars (bars tf = minimum tf)
    this.processTrades(trades)

    if (isExiting) {
      // always write when exiting
      return this.import()
    }

    if (!socketService.clusterSocket) {
      // here the cluster node decide when to write in db
      // otherwise cluster will send a command for that (to balance write tasks between collectors nodes)

      const now = Date.now()
      const timeBackupFloored = Math.floor(now / config.backupInterval) * config.backupInterval
      const timeMinuteFloored = Math.floor(now / config.influxResampleInterval) * config.influxResampleInterval

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

      // reg range for index
      connections[bar.market].high = Math.max(connections[bar.market].high, bar.high)
      connections[bar.market].low = Math.min(connections[bar.market].low, bar.low)
    }

    connections[bar.market].bar = bar

    return connections[bar.market].bar
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

      let market
      let tradeFlooredTime

      if (!trade) {
        // end of loop reached = close all bars
        for (let barIdentifier in activeBars) {
          this.closeBar(activeBars[barIdentifier])

          delete activeBars[barIdentifier]
        }

        break
      } else {
        market = trade.exchange + ':' + trade.pair

        tradeFlooredTime = Math.floor(trade.timestamp / config.influxTimeframe) * config.influxTimeframe

        if (!activeBars[market] || activeBars[market].time < tradeFlooredTime) {
          if (activeBars[market]) {
            // close bar required
            this.closeBar(activeBars[market])

            delete activeBars[market]
          } else {
            connections[market].high = -Infinity
            connections[market].low = Infinity
          }

          // create bar required
          if (!this.pendingBars[market]) {
            this.pendingBars[market] = []
          }

          const debugWrite = /BINANCE_FUTURES.*btcusdt/.test(market)

          if (this.pendingBars[market].length && this.pendingBars[market][this.pendingBars[market].length - 1].time === tradeFlooredTime) {
            activeBars[market] = this.pendingBars[market][this.pendingBars[market].length - 1]
          } else if (connections[market].bar && connections[market].bar.time === tradeFlooredTime) {
            // trades passed in save() contains some of the last batch (trade time = last bar time)
            // recover exchange point of lastbar
            this.pendingBars[market].push(connections[market].bar)
            activeBars[market] = this.pendingBars[market][this.pendingBars[market].length - 1]
          } else {
            // create new bar
            if (debugWrite) {
              debugPush.count++
              debugPush.market = market
              debugPush.lastTime = tradeFlooredTime
            }
            this.pendingBars[market].push({
              time: tradeFlooredTime,
              market: market,
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

            activeBars[market] = this.pendingBars[market][this.pendingBars[market].length - 1]

            if (typeof this.lastClose[market] === 'number') {
              // this bar open = last bar close (from last save or getReferencePoint on startup)
              activeBars[market].open = activeBars[market].high = activeBars[market].low = activeBars[market].close = this.lastClose[market]
            }
          }
        }
      }

      if (trade.liquidation) {
        // trade is a liquidation
        activeBars[market]['l' + trade.side] += trade.price * trade.size
      } else {
        if (activeBars[market].open === null) {
          // new bar without close in db, should only happen once
          console.log(`[storage/influx] register new serie ${market} (last close was unknown)`)
          activeBars[market].open = activeBars[market].high = activeBars[market].low = activeBars[market].close = +trade.price
        }

        activeBars[market].high = Math.max(activeBars[market].high, +trade.price)
        activeBars[market].low = Math.min(activeBars[market].low, +trade.price)
        activeBars[market].close = +trade.price

        activeBars[market]['c' + trade.side] += trade.count || 1
        activeBars[market]['v' + trade.side] += trade.price * trade.size
      }
    }

    if (config.pairs.indexOf('BINANCE_FUTURES:btcusdt') !== -1) {
      console.log(
        `[storage/${this.name}] push ${debugPush.count} ${getHms(config.influxTimeframe)} bar for market ${
          debugPush.market
        } (last time ${new Date(debugPush.lastTime).toISOString().split('T').pop()})`
      )
    }

    updateIndexes((index, high, low) => {
      alertService.checkPriceCrossover(index, high, low)
    })
  }

  /**
   * Import pending bars (minimum tf bars) and resample into bigger timeframes
   */
  async import() {
    /*let now = Date.now()
    let before = now

    console.log(`[storage/influx/import] import start`)

    const resampleRange = await this.importPendingBars()

    if (resampleRange.to - resampleRange.from >= 0) {
      await this.resample(resampleRange)
    }

    now = Date.now()
    console.log(`[storage/influx/import] import end (took ${getHms(now - before, true)})`)*/
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
     * Total range of import
     * @type {TimeRange}
     */
    const importedRange = {
      from: Infinity,
      to: 0,
      markets: [],
    }

    if (!Object.keys(this.pendingBars).length) {
      return importedRange
    }

    const writeAPI = this.influx.getWriteApi(this.options.influxOrg, this.baseBucket, 'ms')
    console.log(`import pending bar (cuurent time: ${new Date().toISOString()})`)

    for (const identifier in this.pendingBars) {
      for (let i = 0; i < this.pendingBars[identifier].length; i++) {
        const bar = this.pendingBars[identifier][i]

        importedRange.from = Math.min(bar.time, importedRange.from)
        importedRange.to = Math.max(bar.time, importedRange.to)

        if (importedRange.markets.indexOf(identifier) === -1) {
          importedRange.markets.push(identifier)
        }

        const volumePoint = new Point('volume')
          .timestamp(bar.time)
          .tag('market', bar.market)
          .intField('cbuy', bar.cbuy)
          .intField('csell', bar.csell)
          .floatField('vbuy', bar.vbuy)
          .floatField('vsell', bar.vsell)
          .floatField('lbuy', bar.lbuy)
          .floatField('lsell', bar.lsell)

        console.log(`${new Date(bar.time).toISOString()} ${volumePoint.toLineProtocol(writeAPI)}`)

        writeAPI.writePoint(volumePoint)

        if (bar.close !== null) {
          const ohlcPoint = new Point('ohlc')
            .timestamp(bar.time)
            .tag('market', bar.market)
            .floatField('open', bar.open)
            .floatField('high', bar.high)
            .floatField('low', bar.low)
            .floatField('close', bar.close)
          writeAPI.writePoint(ohlcPoint)
        }
      }
    }

    // bars are now in writeAPI
    this.pendingBars = {}

    return writeAPI
      .close()
      .then(() => {
        now = Date.now()

        console.log(`[storage/influx/import] done importing pending bars (took ${now - before}ms)`)

        return importedRange
      })
      .catch((error) => {
        console.error(e)
        if (e instanceof HttpError && e.statusCode === 401) {
          console.log('Run ./onboarding.js to setup a new InfluxDB database.')
        }
        console.error(`[storage/influx/import] failed to write points`, error.message)
      })
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

    config.influxResampleTo.sort((a, b) => a - b)

    let bars = 0

    for (let timeframe of config.influxResampleTo) {
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

      for (let i = config.influxResampleTo.indexOf(timeframe); i >= 0; i--) {
        if (timeframe <= config.influxResampleTo[i] || timeframe % config.influxResampleTo[i] !== 0) {
          if (i === 0) {
            sourceTimeframeLitteral = getHms(config.influxTimeframe)
          }
          continue
        }

        sourceTimeframeLitteral = getHms(config.influxResampleTo[i])
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

      const query_from = `${config.influxDatabase}.${config.influxRetentionPrefix}${sourceTimeframeLitteral}.${config.influxMeasurement}_${sourceTimeframeLitteral}`
      const query_into = `${config.influxDatabase}.${config.influxRetentionPrefix}${destinationTimeframeLitteral}.${config.influxMeasurement}_${destinationTimeframeLitteral}`

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
    let filters = `|> filter(fn: (r) => r["_measurement"] == "ohlc")`

    if (markets.length) {
      filters += ` |> filter(fn: (r) => ${markets.map((market) => `r["market"] == "${market}"`).join(' or ')})`
    }

    const query = `from(bucket: "${this.getBucketName(timeframe)}")
    |> range(start: ${from}, stop: ${to})
    ${filters}
    |> pivot(
      rowKey:["_time"],
      columnKey: ["_field"],
      valueColumn: "_value"
    )`
      console.log(query);
    return new Promise((resolve, reject) => {
      this.queryAPI.queryRows(query, {
        next(data) {
          debugger
          resolve([])
        },
        error(error) {
          reject(error.message)
          debugger
        },
        complete() {
          resolve([])
        }
      })
    })

    return this.influx
      .queryRaw(query, {
        precision: 's',
        epoch: 's',
      })
      .then((results) => {
        if (to > +new Date() - config.influxResampleInterval) {
          return this.appendPendingBarsToResponse(results.results[0].series ? results.results[0].series[0].values : [], markets, from, to)
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
  appendPendingBarsToResponse(bars, markets, from, to) {
    if (config.influxCollectors && socketService.clusteredCollectors.length) {
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
  async importCollectors() {
    for (const collector of socketService.clusteredCollectors) {
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

    setTimeout(this.importCollectors.bind(this), config.influxResampleInterval)
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
      for (let j = 0; j < socketService.clusteredCollectors.length; j++) {
        if (
          collectors.indexOf(socketService.clusteredCollectors[j]) === -1 &&
          socketService.clusteredCollectors[j].markets.indexOf(markets[i]) !== -1
        ) {
          collectors.push(socketService.clusteredCollectors[j])
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
          op: 'requestPendingBars',
          data: {
            pendingBarsRequestId,
            markets,
            from,
            to,
          },
        }) + '#'
      )
    })
  }

  getPendingBars(markets, from, to) {
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

    return results
  }
}

module.exports = InfluxStorage
