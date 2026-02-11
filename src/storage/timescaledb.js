const { Pool } = require('pg')
const { getHms, sleep } = require('../helper')
const config = require('../config')
const alertService = require('../services/alert')
const { updateIndexes } = require('../services/connections')

require('../typedef')

class TimescaleDbStorage {
  constructor() {
    this.name = this.constructor.name
    this.format = 'point'

    /** @type {{[identifier: string]: {[timestamp: number]: Bar}}} */
    this.pendingBars = {}

    /** @type {{[identifier: string]: {[timestamp: number]: Bar}}} */
    this.archivedBars = {}

    /** @type {number} */
    this.baseTimeframe = config.influxTimeframe

    /** @type {number[]} */
    this.timeframes = [config.influxTimeframe].concat(config.influxResampleTo || [])

    this.schema = config.timescaleSchema || 'public'
    this.table = config.timescaleTable || 'aggr_bars'

    this.tablePath =
      this.quoteIdentifier(this.schema) + '.' + this.quoteIdentifier(this.table)
  }

  quoteIdentifier(identifier) {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(identifier)) {
      throw new Error(`Invalid SQL identifier "${identifier}"`)
    }

    return '"' + identifier + '"'
  }

  normalizeSide(side) {
    if (side === 'buy' || side === 1 || side === '1') {
      return 'buy'
    }

    if (side === 'sell' || side === 2 || side === '2') {
      return 'sell'
    }

    return String(side || '').toLowerCase() === 'sell' ? 'sell' : 'buy'
  }

  async connect() {
    const sslEnabled =
      config.timescaleSsl === true ||
      config.timescaleSsl === 'true' ||
      config.timescaleSsl === 1 ||
      config.timescaleSsl === '1'

    const poolConfig = {
      host: config.timescaleHost || 'localhost',
      port: Number(config.timescalePort || 5432),
      database: config.timescaleDatabase || 'aggr',
      user: config.timescaleUser || 'postgres',
      password:
        typeof config.timescalePassword === 'string'
          ? config.timescalePassword
          : undefined,
      max: Number(config.timescalePoolMax || 20),
      application_name: config.timescaleApplicationName || 'aggr-server'
    }

    if (sslEnabled) {
      poolConfig.ssl = {
        rejectUnauthorized: !(
          config.timescaleSslRejectUnauthorized === false ||
          config.timescaleSslRejectUnauthorized === 'false' ||
          config.timescaleSslRejectUnauthorized === 0 ||
          config.timescaleSslRejectUnauthorized === '0'
        )
      }
    }

    if (config.timescaleStatementTimeout) {
      poolConfig.statement_timeout = Number(config.timescaleStatementTimeout)
    }

    console.log(
      `[storage/timescaledb] connecting to ${poolConfig.host}:${poolConfig.port} on db "${poolConfig.database}"`
    )

    this.pool = new Pool(poolConfig)

    try {
      await this.pool.query('SELECT 1')
    } catch (error) {
      console.error(
        `[storage/timescaledb] Error: ${error.message}... retrying in 1s`
      )
      await sleep(1000)
      return this.connect()
    }

    console.log('[storage/timescaledb] connection ready')
  }

  async save(trades, isExiting) {
    if (!trades || !trades.length) {
      return Promise.resolve()
    }

    await this.processTrades(trades)

    const now = Date.now()
    const timeBackupFloored =
      Math.floor(now / config.backupInterval) * config.backupInterval
    const timeMinuteFloored =
      Math.floor(now / config.influxResampleInterval) *
      config.influxResampleInterval

    if (isExiting || timeBackupFloored === timeMinuteFloored) {
      await this.flush(isExiting)
    }
  }

  async processTrades(trades) {
    const ranges = {}

    for (let i = 0; i < trades.length; i++) {
      const trade = trades[i]
      const market = trade.exchange + ':' + trade.pair
      const tradeFlooredTime =
        Math.floor(trade.timestamp / this.baseTimeframe) * this.baseTimeframe

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

      if (!this.pendingBars[market]) {
        this.pendingBars[market] = {}
      }

      let bar

      if (this.pendingBars[market][tradeFlooredTime]) {
        bar = this.pendingBars[market][tradeFlooredTime]
      } else if (
        this.archivedBars[market] &&
        this.archivedBars[market][tradeFlooredTime]
      ) {
        bar = this.pendingBars[market][tradeFlooredTime] = {
          ...this.archivedBars[market][tradeFlooredTime]
        }
      } else {
        const persistedBar = await this.getPersistedBar(market, tradeFlooredTime)

        if (persistedBar) {
          bar = this.pendingBars[market][tradeFlooredTime] = persistedBar
        } else {
          bar = this.pendingBars[market][tradeFlooredTime] = {
            time: tradeFlooredTime,
            market,
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
        }
      }

      const side = this.normalizeSide(trade.side)

      if (trade.liquidation) {
        bar['l' + side] += trade.price * trade.size
      } else {
        if (bar.open === null) {
          bar.open = bar.high = bar.low = bar.close = +trade.price
        } else {
          bar.high = Math.max(bar.high, +trade.price)
          bar.low = Math.min(bar.low, +trade.price)
          bar.close = +trade.price
        }

        bar['c' + side] += trade.count || 1
        bar['v' + side] += trade.price * trade.size
      }
    }

    updateIndexes(ranges, async (index, high, low, direction) => {
      if (alertService && alertService.checkPriceCrossover) {
        await alertService.checkPriceCrossover(index, high, low, direction)
      }
    })
  }

  async getPersistedBar(market, timestamp) {
    const query = `
      SELECT
        EXTRACT(EPOCH FROM bucket)::bigint AS ts,
        open, high, low, close,
        vbuy, vsell, cbuy, csell, lbuy, lsell
      FROM ${this.tablePath}
      WHERE timeframe_ms = $1
        AND market = $2
        AND bucket = to_timestamp($3 / 1000.0)
      LIMIT 1
    `

    try {
      const result = await this.pool.query(query, [
        this.baseTimeframe,
        market,
        timestamp
      ])

      if (!result.rows.length) {
        return null
      }

      const row = result.rows[0]

      return {
        time: Number(row.ts) * 1000,
        market,
        open: Number(row.open),
        high: Number(row.high),
        low: Number(row.low),
        close: Number(row.close),
        vbuy: Number(row.vbuy),
        vsell: Number(row.vsell),
        cbuy: Number(row.cbuy),
        csell: Number(row.csell),
        lbuy: Number(row.lbuy),
        lsell: Number(row.lsell)
      }
    } catch (error) {
      console.error(
        `[storage/timescaledb] failed to load persisted bar ${market}@${new Date(
          timestamp
        ).toISOString()}`,
        error.message
      )
      return null
    }
  }

  cleanArchivedBars(market) {
    const barsMap = this.archivedBars[market]

    if (!barsMap) {
      return
    }

    const keepWindow = this.baseTimeframe * 100
    const now = Date.now()
    const cutoff =
      Math.floor(now / this.baseTimeframe) * this.baseTimeframe - keepWindow

    for (const ts in barsMap) {
      if (Number(ts) < cutoff) {
        delete barsMap[ts]
      }
    }
  }

  async flush(isExiting) {
    const now = Date.now()
    const closedThreshold =
      Math.floor(now / this.baseTimeframe) * this.baseTimeframe

    for (const market in this.pendingBars) {
      const barsMap = this.pendingBars[market]
      const closedBars = []

      for (const ts in barsMap) {
        const barTime = Number(ts)
        const alignedBarTime =
          Math.floor(barTime / this.baseTimeframe) * this.baseTimeframe

        if (isExiting || alignedBarTime < closedThreshold) {
          const bar = barsMap[ts]

          if (bar.close !== null) {
            bar.time = alignedBarTime
            closedBars.push(bar)
          }

          delete barsMap[ts]
        }
      }

      if (!closedBars.length) {
        this.cleanArchivedBars(market)
        continue
      }

      closedBars.sort((a, b) => a.time - b.time)

      const writeResult = await this.upsertBars(
        this.baseTimeframe,
        market,
        closedBars
      )

      if (!this.archivedBars[market]) {
        this.archivedBars[market] = {}
      }

      for (const bar of closedBars) {
        this.archivedBars[market][bar.time] = { ...bar }
      }

      this.cleanArchivedBars(market)

      if (writeResult.fromTsWritten !== null) {
        await this.resampleToHigherTimeframes(
          market,
          writeResult.fromTsWritten,
          writeResult.toTsWritten
        )
      }
    }
  }

  async upsertBars(timeframe, market, bars) {
    if (!bars.length) {
      return {
        fromTsWritten: null,
        toTsWritten: null
      }
    }

    const MAX_ROWS_PER_BATCH = 1000

    for (let cursor = 0; cursor < bars.length; cursor += MAX_ROWS_PER_BATCH) {
      const batch = bars.slice(cursor, cursor + MAX_ROWS_PER_BATCH)

      const values = []
      const params = []

      for (let i = 0; i < batch.length; i++) {
        const bar = batch[i]
        const base = i * 13

        values.push(
          `(
            to_timestamp($${base + 1} / 1000.0),
            $${base + 2},
            $${base + 3},
            $${base + 4},
            $${base + 5},
            $${base + 6},
            $${base + 7},
            $${base + 8},
            $${base + 9},
            $${base + 10},
            $${base + 11},
            $${base + 12},
            $${base + 13}
          )`
        )

        params.push(
          bar.time,
          timeframe,
          market,
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
      }

      const query = `
        INSERT INTO ${this.tablePath}
          (
            bucket,
            timeframe_ms,
            market,
            open,
            high,
            low,
            close,
            vbuy,
            vsell,
            cbuy,
            csell,
            lbuy,
            lsell
          )
        VALUES ${values.join(',')}
        ON CONFLICT (timeframe_ms, market, bucket)
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
          lsell = EXCLUDED.lsell
      `

      await this.pool.query(query, params)
    }

    return {
      fromTsWritten: bars[0].time,
      toTsWritten: bars[bars.length - 1].time
    }
  }

  async resampleToHigherTimeframes(market, fromTs, toTs) {
    const higherTimeframes = this.timeframes.filter(tf => tf > this.baseTimeframe)

    if (!higherTimeframes.length) {
      return
    }

    for (const targetTimeframe of higherTimeframes) {
      await this.resampleRangeToTimeframe(market, fromTs, toTs, targetTimeframe)
    }
  }

  async resampleRangeToTimeframe(market, fromTs, toTs, targetTimeframe) {
    const fromAligned = Math.floor(fromTs / targetTimeframe) * targetTimeframe
    const toAligned =
      Math.ceil((toTs + this.baseTimeframe) / targetTimeframe) * targetTimeframe

    if (fromAligned >= toAligned) {
      return
    }

    const query = `
      INSERT INTO ${this.tablePath}
        (
          bucket,
          timeframe_ms,
          market,
          open,
          high,
          low,
          close,
          vbuy,
          vsell,
          cbuy,
          csell,
          lbuy,
          lsell
        )
      SELECT
        grouped.bucket,
        $2::integer AS timeframe_ms,
        grouped.market,
        grouped.open,
        grouped.high,
        grouped.low,
        grouped.close,
        grouped.vbuy,
        grouped.vsell,
        grouped.cbuy,
        grouped.csell,
        grouped.lbuy,
        grouped.lsell
      FROM (
        SELECT
          time_bucket($1::interval, bucket) AS bucket,
          market,
          (array_agg(open ORDER BY bucket ASC))[1] AS open,
          MAX(high) AS high,
          MIN(low) AS low,
          (array_agg(close ORDER BY bucket DESC))[1] AS close,
          SUM(vbuy)::double precision AS vbuy,
          SUM(vsell)::double precision AS vsell,
          SUM(cbuy)::integer AS cbuy,
          SUM(csell)::integer AS csell,
          SUM(lbuy)::double precision AS lbuy,
          SUM(lsell)::double precision AS lsell
        FROM ${this.tablePath}
        WHERE timeframe_ms = $3
          AND market = $4
          AND bucket >= to_timestamp($5 / 1000.0)
          AND bucket < to_timestamp($6 / 1000.0)
        GROUP BY 1, 2
      ) grouped
      ON CONFLICT (timeframe_ms, market, bucket)
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
        lsell = EXCLUDED.lsell
    `

    await this.pool.query(query, [
      `${targetTimeframe} milliseconds`,
      targetTimeframe,
      this.baseTimeframe,
      market,
      fromAligned,
      toAligned
    ])
  }

  fetch({ from, to, timeframe = 60000, markets = [] }) {
    if (this.timeframes.indexOf(timeframe) === -1) {
      return Promise.reject(new Error(`Unsupported timeframe: ${getHms(timeframe)}`))
    }

    const params = [timeframe, from, to]

    let query = `
      SELECT
        EXTRACT(EPOCH FROM bucket)::bigint AS time,
        market,
        open,
        high,
        low,
        close,
        vbuy,
        vsell,
        cbuy,
        csell,
        lbuy,
        lsell
      FROM ${this.tablePath}
      WHERE timeframe_ms = $1
        AND bucket >= to_timestamp($2 / 1000.0)
        AND bucket < to_timestamp($3 / 1000.0)
    `

    if (markets.length) {
      params.push(markets)
      query += ` AND market = ANY($${params.length})`
    }

    query += ' ORDER BY bucket ASC'

    return this.pool
      .query(query, params)
      .then(result => {
        const output = {
          format: this.format,
          columns: {
            time: 0,
            market: 1,
            open: 2,
            high: 3,
            low: 4,
            close: 5,
            vbuy: 6,
            vsell: 7,
            cbuy: 8,
            csell: 9,
            lbuy: 10,
            lsell: 11
          },
          results: result.rows.map(row => [
            Number(row.time),
            row.market,
            Number(row.open),
            Number(row.high),
            Number(row.low),
            Number(row.close),
            Number(row.vbuy),
            Number(row.vsell),
            Number(row.cbuy),
            Number(row.csell),
            Number(row.lbuy),
            Number(row.lsell)
          ])
        }

        if (to > Date.now() - config.influxResampleInterval) {
          const pendingBars = this.getPendingBars(markets, from, to)
          output.results = output.results.concat(pendingBars).sort((a, b) => a[0] - b[0])
        }

        return output
      })
      .catch(err => {
        console.error(
          `[storage/timescaledb] failed to retrieve bars between ${from} and ${to} with timeframe ${timeframe}`,
          err.message
        )

        throw err
      })
  }

  getPendingBars(markets, from, to) {
    const results = []

    const selectedMarkets = markets.length
      ? markets
      : Object.keys(this.pendingBars)

    for (const market of selectedMarkets) {
      const barsMap = this.pendingBars[market]

      if (!barsMap) {
        continue
      }

      for (const ts in barsMap) {
        const bar = barsMap[ts]

        if (bar.close === null) {
          continue
        }

        const barTime = Number(ts)

        if (barTime < from || barTime >= to) {
          continue
        }

        results.push([
          Math.floor(barTime / 1000),
          market,
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
        ])
      }
    }

    return results
  }
}

module.exports = TimescaleDbStorage
