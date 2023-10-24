const { getHms, sleep, ID } = require('../helper')
const net = require('net')
const config = require('../config')
const socketService = require('../services/socket')
const alertService = require('../services/alert')
const { updateIndexes } = require('../services/connections')

const DAY = 1000 * 60 * 60 * 24

/**
 * Official implementation example for Node.JS:
 * https://docs.timescale.com/quick-start/latest/node/#nodejs-quick-start
 */

class TimescaleStorage {
  constructor() {
    this.name = this.constructor.name
    this.format = 'point'

    /**
     * @type {{[pendingBarsRequestId: string]: (bars: Bar[]) => void}}
     */
    this.promisesOfPendingBars = {}

    /**
     * @type {{[identifier: string]: Bar[]}}
     */
    this.recentlyClosedBars = {}

    /**
     * @type {{[identifier: string]: {[timestamp: number]: Bar}}}
     */
    this.pendingBars = {}

    /**
     * @type {{[identifier: string]: {[timestamp: number]: Bar}}}
     */
    this.archivedBars = {}

    /**
     * @type {number}
     */
    this.influxTimeframeRetentionDuration = null
  }

  async connect() {
    if (/\-/.test(config.timescaleDatabase)) {
      throw new Error('dashes not allowed inside timescale database name')
    }

    let host = config.timescaleHost
    let port = config.timescalePort

    if (typeof config.timescaleUrl === 'string' && config.timescaleUrl.length) {
      ;[host, port] = config.timescaleUrl.split(':')
    }

    console.log(
      `[storage/timescale] connecting to ${host}:${port} on db "${config.timescaleDatabase}"`
    )

    this.client = new Client({
      host: host || 'localhost',
      port: port || '5432',
      database: config.timescaleDatabase,
      user: config.timescaleUser, // Assuming you have a username in config
      password: config.timescalePassword // Assuming you have a password in config
    })

    try {
      await this.client.connect()

      // Check if the database exists
      const res = await this.client.query(
        'SELECT 1 FROM pg_database WHERE datname=$1',
        [config.timescaleDatabase]
      )
      if (res.rows.length === 0) {
        // Create the database - Note: You might need a superuser to create a new database
        await this.client.query(`CREATE DATABASE ${config.timescaleDatabase}`)
      }

      if (config.collect) {
        await this.ensureRetentionPolicies()
        await this.getPreviousBars()
      }
    } catch (error) {
      console.error(
        [
          `[storage/timescale] Error: ${error.message}... retrying in 1s`,
          `Please ensure that the environment variable TIMESCALE_HOST is correctly set or that TimescaleDB is running.`,
          `Refer to the README.md file for more instructions.`
        ].join('\n')
      )

      await sleep()

      return this.connect()
    } finally {
      // Rest of your logic...
      // ...

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
      .on('requestPendingBars', ({ data }) => {
        // response from pending bars request

        if (this.promisesOfPendingBars[data.pendingBarsRequestId]) {
          this.promisesOfPendingBars[data.pendingBarsRequestId](data.results)
        } else {
          // console.error('[influx/cluster] there was no promisesOfPendingBars with given pendingBarsRequestId', data.pendingBarsRequestId)
        }
      })
  }

  /**
   * listen for request from cluster node
   */
  bindClusterEvents() {
    socketService
      .on('requestPendingBars', ({ data }) => {
        // this is a request for pending bars from cluster
        const payload = {
          pendingBarsRequestId: data.pendingBarsRequestId,
          results: this.getPendingBars(data.markets, data.from, data.to)
        }
        socketService.clusterSocket.write(
          JSON.stringify({
            opId: 'requestPendingBars',
            data: payload
          }) + '#'
        )
      })
      .on('import', () => {
        // this is a request to import pending data

        this.import().finally(() => {
          if (socketService.clusterSocket) {
            socketService.clusterSocket.write(
              JSON.stringify({
                opId: 'import'
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
    alertService.on(
      'change',
      ({ market, price, user, type, previousPrice }) => {
        const fields = {
          price,
          user,
          type
        }

        if (typeof previousPrice !== 'undefined') {
          fields.previousPrice = previousPrice
        }

        this.writePoints(
          [
            {
              measurement: 'alerts',
              tags: {
                market
              },
              fields,
              timestamp: Date.now()
            }
          ],
          {
            precision: 'ms'
          }
        )
      }
    )
  }

  /**
   *
   */
  async ensureRetentionPolicies() {
    // 1. Ensure the Hypertable
    let res = await this.client.query("SELECT * FROM timescaledb_information.hypertables WHERE table_name=$1", [config.timescaleTable]);
    
    if (res.rows.length === 0) {
        await this.client.query(`CREATE TABLE ${config.timescaleTable} (
          timestamp TIMESTAMPTZ NOT NULL,
          market TEXT NOT NULL,
          open DOUBLE PRECISION,
          high DOUBLE PRECISION,
          low DOUBLE PRECISION,
          close DOUBLE PRECISION
        );
                                 SELECT create_hypertable('${config.timescaleTable}', 'timestamp', 'market');`);
    }

    // 2. Continuous Aggregates
    const baseTimeframe = config.influxTimeframe
    const derivedTimeframes = config.influxResampleTo

    for (let timeframe of derivedTimeframes) {
      // construct the name of the view based on your naming convention
      const caggName = `cagg_${getHms(timeframe).replace(/[\s,]/g, '')}`
      let caggRes = await this.client.query(
        'SELECT * FROM timescaledb_information.continuous_aggregates WHERE view_name=$1',
        [caggName]
      )

      if (caggRes.rows.length === 0) {
        await this.client
          .query(`CREATE VIEW ${caggName} WITH (timescaledb.continuous)
                                     AS
                                     SELECT /* your aggregation logic here based on baseTimeframe and timeframe */
                                     FROM ${config.timescaleTable}
                                     GROUP BY time_bucket(INTERVAL 'baseTimeframe', time_column_name), /* other group by columns */;`);
        }
    }

    // 3. Data Retention - for base hypertable and continuous aggregates
    const retentionInterval = getHms(baseTimeframe * config.influxRetentionPerTimeframe).replace(/[\s,]/g, ''); 
    await this.client.query(`SELECT drop_chunks(interval '${retentionInterval}', '${config.timescaleTable}');`);
    for (let timeframe of derivedTimeframes) {
      const caggName = `cagg_${getHms(timeframe).replace(/[\s,]/g, '')}`
      await this.client.query(
        `SELECT drop_chunks(interval '${retentionInterval}', '${caggName}');`
      )
    }
  }

  async getPreviousBars() {
    const timeframeLitteral = getHms(config.influxTimeframe) // this will still be used for filtering
    const now = +new Date()

    // Use parameterized query for better safety against SQL injections
    let query = `
        SELECT * 
        FROM ${config.timescaleTable}
        WHERE market = ANY($1) 
        AND timestamp >= NOW() - INTERVAL '${timeframeLitteral}'
        GROUP BY market 
        ORDER BY timestamp DESC 
        LIMIT 1
    `

    try {
      // Assuming you have a client named 'timescale' or some appropriate name
      const data = await timescale.query(query, [config.pairs])

      for (let bar of data.rows) {
        if (now - Date.parse(bar.timestamp) > config.influxResampleInterval) {
            // can't use lastBar because it is too old anyway
            continue;
        }

        let originalBar

        if (!this.pendingBars[bar.market]) {
          this.pendingBars[bar.market] = {}
        }

        if (
          this.pendingBars[bar.market] &&
          (originalBar = this.pendingBars[bar.market][Date.parse(bar.time)])
        ) {
          this.sumBar(originalBar, bar)
        } else {
          this.pendingBars[bar.market][Date.parse(bar.time)] = this.sumBar(
            {},
            bar
          )
        }
      }
    } catch (error) {
      console.error(`Error while fetching previous bars: ${error.message}`)
    }
  }

  sumBar(barToMutate, barToAdd) {
    const props = Object.keys(barToMutate)
      .concat(Object.keys(barToAdd))
      .filter((x, i, a) => a.indexOf(x) == i)

    for (let i = 0; i < props.length; i++) {
      const prop = props[i]

      const value = isNaN(barToAdd[prop]) ? barToAdd[prop] : +barToAdd[prop]

      if (typeof barToMutate[prop] === 'undefined') {
        barToMutate[prop] = value
        continue
      }

      if (typeof barToMutate[prop] === 'number') {
        barToMutate[props] += value
      }
    }

    if (
      !barToMutate.open &&
      !barToMutate.high &&
      !barToMutate.low &&
      !barToMutate.close
    ) {
      barToMutate.open =
        barToMutate.high =
        barToMutate.low =
        barToMutate.close =
          null
    }

    return barToMutate
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
      const timeBackupFloored =
        Math.floor(now / config.backupInterval) * config.backupInterval
      const timeMinuteFloored =
        Math.floor(now / config.influxResampleInterval) *
        config.influxResampleInterval

      if (timeBackupFloored === timeMinuteFloored) {
        return this.import()
      }
    }
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
    const bars = {}

    /**
     * Processed ranges by market
     * @type {{[identifier: string]: {
     *  high: number,
     *  low: number
     * }}}
     */
    const ranges = {}

    for (let i = 0; i < trades.length; i++) {
      const trade = trades[i]
      const market = trade.exchange + ':' + trade.pair
      const tradeFlooredTime =
        Math.floor(trade.timestamp / config.influxTimeframe) *
        config.influxTimeframe

      if (!trade.liquidation) {
        if (!ranges[market]) {
          ranges[market] = {
            low: trade.price,
            high: trade.price,
            close: trade.price
          }
        } else {
          ranges[market].low = Math.min(ranges[market].low, trade.price)
          ranges[market].high = Math.max(ranges[market].high, trade.price)
          ranges[market].close = trade.price
        }
      }

      if (!bars[market] || bars[market].timestamp !== tradeFlooredTime) {
        //bars[market] && console.log(`${market} timestamp is !==, resolve bar or create`)
        // need to create bar OR recover bar either from pending bar / last saved bar

        if (!this.pendingBars[market]) {
          //console.log(`create container for pending bars of market ${market}`)
          this.pendingBars[market] = {}
        }

        if (
          this.pendingBars[market] &&
          this.pendingBars[market][tradeFlooredTime]
        ) {
          bars[market] = this.pendingBars[market][tradeFlooredTime]
        } else if (
          this.archivedBars[market] &&
          this.archivedBars[market][tradeFlooredTime]
        ) {
          bars[market] = this.pendingBars[market][tradeFlooredTime] =
            this.archivedBars[market][tradeFlooredTime]
        } else {
          bars[market] = this.pendingBars[market][tradeFlooredTime] = {
            timestamp: tradeFlooredTime,
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
            close: null
          }
          //console.log(`\tcreate new bar (timestamp ${new Date(bars[market].timestamp).toISOString().split('T').pop().replace(/\..*/, '')})`)
        }
      }

      if (trade.liquidation) {
        // trade is a liquidation
        bars[market]['l' + trade.side] += trade.price * trade.size
      } else {
        if (bars[market].open === null) {
          bars[market].open =
            bars[market].high =
            bars[market].low =
            bars[market].close =
              +trade.price
        } else {
          bars[market].high = Math.max(bars[market].high, +trade.price)
          bars[market].low = Math.min(bars[market].low, +trade.price)
          bars[market].close = +trade.price
        }

        bars[market]['c' + trade.side] += trade.count || 1
        bars[market]['v' + trade.side] += trade.price * trade.size
      }
    }

    await updateIndexes(ranges, async (index, high, low, direction) => {
      await alertService.checkPriceCrossover(index, high, low, direction)
    })
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
      markets: []
    }

    for (const identifier in this.pendingBars) {
      if (importedRange.markets.indexOf(identifier) === -1) {
        importedRange.markets.push(identifier)
      }

      for (const timestamp in this.pendingBars[identifier]) {
        if (timestamp < now - this.influxTimeframeRetentionDuration) {
          continue
        }

        const bar = this.pendingBars[identifier][timestamp]

        importedRange.from = Math.min(bar.timestamp, importedRange.from)
        importedRange.to = Math.max(bar.timestamp, importedRange.to)

        barsToImport.push(bar)

        if (!this.archivedBars[identifier]) {
          this.archivedBars[identifier] = {}
        }

        this.archivedBars[identifier][timestamp] =
          this.pendingBars[identifier][timestamp]
      }
    }

    // free up pending bars memory
    this.pendingBars = {}

    if (barsToImport.length) {
      // console.log(`[storage/influx] importing ${barsToImport.length} bars`)

      // Write the bars to TimescaleDB hypertable
      await this.writeBarsToTimescaleDB(barsToImport)
    }

    return importedRange
  }

  async writeBarsToTimescaleDB(bars) {
    const query = `
        INSERT INTO ohlcv_base (time, market, open, high, low, close, vbuy, vsell, cbuy, csell, lbuy, lsell)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (time, market) 
        DO UPDATE SET 
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            vbuy = EXCLUDED.vbuy,
            vsell = EXCLUDED.vsell,
            cbuy = EXCLUDED.cbuy,
            csell = EXCLUDED.csell,
            lbuy = EXCLUDED.lbuy,
            lsell = EXCLUDED.lsell;
    `

    for (let bar of bars) {
      const values = [
        bar.time,
        bar.market,
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.vbuy,
        bar.vsell,
        bar.cbuy,
        bar.csell,
        bar.lbuy,
        bar.lsell
      ]

      // Assuming you have a client set up to interact with TimescaleDB
      await timescale.query(query, values)
    }
  }

  async writeBarsToTimescaleDB(bars) {
    // Base query for inserting data
    const baseQuery = `
        INSERT INTO ohlcv_base (time, market, open, high, low, close, vbuy, vsell, cbuy, csell, lbuy, lsell)
        VALUES 
    `

    // Placeholder for all bar values
    const allValues = []

    // Placeholder for the parameterized query parts for each bar
    const barsQueryParts = []

    bars.forEach((bar, index) => {
      const offset = index * 12 // 12 because there are 12 fields for each bar
      barsQueryParts.push(
        `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${
          offset + 5
        }, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${
          offset + 10
        }, $${offset + 11}, $${offset + 12})`
      )
      allValues.push(
        bar.time,
        bar.market,
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.vbuy,
        bar.vsell,
        bar.cbuy,
        bar.csell,
        bar.lbuy,
        bar.lsell
      )
    })

    const query =
      baseQuery +
      barsQueryParts.join(', ') +
      `
        ON CONFLICT (time, market) 
        DO UPDATE SET 
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            vbuy = EXCLUDED.vbuy,
            vsell = EXCLUDED.vsell,
            cbuy = EXCLUDED.cbuy,
            csell = EXCLUDED.csell,
            lbuy = EXCLUDED.lbuy,
            lsell = EXCLUDED.lsell;
    `

    await timescale.query(query, allValues)
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
        console.log(
          `[storage/influx] successfully wrote points after ${attempt} attempt(s)`
        )
      }
    } catch (error) {
      attempt++

      console.error(
        `[storage/influx] write points failed (${attempt}${
          attempt === 1
            ? 'st'
            : attempt === 2
            ? 'nd'
            : attempt === 3
            ? 'rd'
            : 'th'
        } attempt)`,
        error.message
      )

      if (attempt >= 5) {
        console.error(
          `too many attemps at writing points\n\n${measurement}, ${new Date(
            from
          ).toUTCString()} to ${new Date(to).toUTCString()}\n\t-> abort`
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
  async resample(range, fromTimeframe, toTimeframe = null) {
    let sourceTimeframeLitteral
    let destinationTimeframeLitteral

    let now = Date.now()
    let before = now

    console.log(
      `[storage/influx/resample] resampling ${range.markets.length} markets`
    )

    let minimumTimeframe
    let timeframes
    if (fromTimeframe) {
      minimumTimeframe = Math.max(fromTimeframe, config.influxTimeframe)
      timeframes = config.influxResampleTo.filter(a => a > fromTimeframe)
    } else {
      minimumTimeframe = config.influxTimeframe
      timeframes = config.influxResampleTo
    }

    let bars = 0

    for (let timeframe of timeframes) {
      const isOddTimeframe = DAY % timeframe !== 0 && timeframe < DAY

      if (toTimeframe && timeframe !== toTimeframe) {
        continue
      }

      let flooredRange

      if (isOddTimeframe) {
        const dayOpen = Math.floor(range.from / DAY) * DAY
        flooredRange = {
          from:
            dayOpen +
            Math.floor((range.from - dayOpen) / timeframe) * timeframe,
          to:
            dayOpen +
            Math.floor((range.to - dayOpen) / timeframe) * timeframe +
            timeframe
        }
      } else {
        flooredRange = {
          from: Math.floor(range.from / timeframe) * timeframe,
          to: Math.floor(range.to / timeframe) * timeframe + timeframe
        }
      }

      for (let i = timeframes.indexOf(timeframe); i >= 0; i--) {
        if (timeframe <= timeframes[i] || timeframe % timeframes[i] !== 0) {
          if (i === 0) {
            sourceTimeframeLitteral = getHms(minimumTimeframe)
          }
          continue
        }

        sourceTimeframeLitteral = getHms(timeframes[i])
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
      coverage += ` AND (${range.markets
        .map(market => `market = '${market}'`)
        .join(' OR ')})`

      const group = `GROUP BY time(${destinationTimeframeLitteral}${
        isOddTimeframe ? ', ' + getHms(flooredRange.from % timeframe) : ''
      }), market fill(none)`

      bars += (flooredRange.to - flooredRange.from) / timeframe

      await this.executeQuery(
        `${query} INTO ${query_into} FROM ${query_from} ${coverage} ${group}`
      )
    }

    now = Date.now()

    const elapsedOp = now - before

    console.log(
      `[storage/influx/resample] done resampling ${parseInt(
        (now - before) / bars
      )}ms per bar (${parseInt(elapsedOp)}ms for ${bars} bars)`
    )

    if (elapsedOp > 10000) {
      console.log(
        `[storage/influx/resample] resample range ${new Date(
          range.from
        ).toISOString()} to ${new Date(range.to).toISOString()} (${
          range.markets.length
        } markets)`
      )
    }
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
        console.log(
          `[storage/influx] successfully executed query ${attempt} attempt(s)`
        )
      }
    } catch (error) {
      attempt++

      console.error(
        `[storage/influx] query failed (${attempt}${
          attempt === 1
            ? 'st'
            : attempt === 2
            ? 'nd'
            : attempt === 3
            ? 'rd'
            : 'th'
        } attempt)`,
        error.message
      )

      if (attempt >= 5) {
        console.error(
          `too many attemps at executing query\n\n${query}\n\t-> abort`
        )
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
  async fetch({ from, to, timeframe = 60000, markets = [] }) {
    // Determine if you should use the base hypertable or a continuous aggregate view
    const isBaseTimeframe = timeframe === 10000; // 10s in ms
    const tableName = isBaseTimeframe ? config.timescaleTable : `"${config.timescaleRetentionPrefix}${getHms(timeframe)}"`;
    
    const marketConditions = markets.length ? 
        `AND market IN (${markets.map((_, index) => `$${index + 3}`).join(', ')})` : 
        "";

    const query = `
        SELECT * 
        FROM ${tableName}
        WHERE time >= $1 AND time < $2 ${marketConditions};
    `

    try {
      const values = [from, to, ...markets]
      const result = await this.pgClient.query(query, values)

      // Process the results and return the appropriate structure
      const output = {
        format: this.format,
        columns: Object.keys(result.rows[0]),
        results: result.rows
      }

      // If there might be pending bars in the application's memory, merge them
      if (to > Date.now() - config.influxResampleInterval) {
        const bars = await this.appendPendingBarsToResponse(
          output.results,
          markets,
          from,
          to,
          timeframe
        )
        output.results = bars
      }

      return output
    } catch (err) {
      console.error(
        `[storage/timescale] failed to retrieve bars between ${from} and ${to} with timeframe ${timeframe}\n\t`,
        err.message
      )
      throw err
    }
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
  appendPendingBarsToResponse(bars, markets, from, to, timeframe) {
    if (config.influxCollectors && socketService.clusteredCollectors.length) {
      // use collectors nodes pending bars
      return this.requestPendingBars(markets, from, to, timeframe).then(
        pendingBars => {
          return bars.concat(pendingBars)
        }
      )
    } else {
      // use current node pending bars
      const injectedPendingBars = this.getPendingBars(markets, from, to).sort(
        (a, b) => a.time - b.time
      )

      return Promise.resolve(bars.concat(injectedPendingBars))
    }
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
    return new Promise(resolve => {
      const pendingBarsRequestId = ID()

      let promiseOfPendingBarsTimeout = setTimeout(() => {
        console.error(
          '[storage/influx/cluster] promise of realtime bar timeout fired (pendingBarsRequestId: ' +
            pendingBarsRequestId +
            ')'
        )

        // response empty array as we didn't got the expected bars...
        this.promisesOfPendingBars[pendingBarsRequestId]([])

        // invalidate timeout
        promiseOfPendingBarsTimeout = null
      }, 5000)

      // register promise
      this.promisesOfPendingBars[pendingBarsRequestId] = pendingBars => {
        if (promiseOfPendingBarsTimeout) {
          clearTimeout(promiseOfPendingBarsTimeout)
        }

        // unregister promise
        delete this.promisesOfPendingBars[pendingBarsRequestId]

        resolve(pendingBars)
      }

      collector.write(
        JSON.stringify({
          opId: 'requestPendingBars',
          data: {
            pendingBarsRequestId,
            markets,
            from,
            to
          }
        }) + '#'
      )
    })
  }
}

module.exports = TimescaleStorage
