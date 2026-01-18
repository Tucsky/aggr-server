const fs = require('fs')
const config = require('../../config')
const alertService = require('../../services/alert')
const { updateIndexes } = require('../../services/connections')
const { getHms } = require('../../helper')

const { RECORD_SIZE, getSegmentStartTs, getSegmentSpanMs } = require('./constants')
const { getSegmentPaths, parseMarket, readRecord, isNullRecord, readMeta, readSingleRecordAtTs, openFdCached } = require('./io')
const { upsertBars } = require('./write')
const { resampleToHigherTimeframes } = require('./resample')

/**
 * Binary storage engine for OHLCV data.
 * 
 * Stores aggregated trade data in dense binary files with fixed-size records.
 * Each market (exchange:symbol) has separate files per timeframe.
 * 
 * Key features:
 * - Dense indexing: records are stored contiguously, with null bars filling gaps
 * - Upsert semantics: can overwrite existing records or append new ones
 * - Automatic resampling: base timeframe data is aggregated to higher timeframes
 * - Backfill support: older trades can be merged into existing buckets
 * 
 * Data flow:
 * 1. Trades arrive via save() → processTrades() aggregates into pendingBars
 * 2. On flush interval, closed bars are persisted via upsertBars()
 * 3. After base TF write, higher TFs are resampled via resampleToHigherTimeframes()
 * 
 * @class
 */
class BinariesStorage {
  /**
   * Creates a new BinariesStorage instance.
   * Initializes in-memory caches and ensures the data directory exists.
   */
  constructor() {
    /** @type {string} */
    this.name = this.constructor.name

    /** @type {string} */
    this.format = 'point'

    /**
     * In-memory cache of bars not yet persisted (current bucket and recent unflushed).
     * Structure: { [market: string]: { [timestamp: number]: Bar } }
     * @type {Object<string, Object<number, import('./constants').Bar>>}
     */
    this.pendingBars = {}

    /**
     * Cache of recently persisted bars for backfill merging.
     * Allows late-arriving trades to merge into already-flushed buckets.
     * Structure: { [market: string]: { [timestamp: number]: Bar } }
     * @type {Object<string, Object<number, import('./constants').Bar>>}
     */
    this.archivedBars = {}

    /**
     * Base timeframe in milliseconds (smallest granularity stored).
     * @type {number}
     */
    this.baseTimeframe = config.influxTimeframe

    /**
     * All supported timeframes (base + resampled targets).
     * @type {number[]}
     */
    this.timeframes = [config.influxTimeframe].concat(config.influxResampleTo || [])

    // Ensure the data directory exists
    if (!fs.existsSync(config.filesLocation)) {
      fs.mkdirSync(config.filesLocation, { recursive: true })
    }
  }

  /**
   * Removes old entries from the archivedBars cache for a given market.
   * Keeps only the last (baseTimeframe * 100) milliseconds worth of buckets.
   * This prevents unbounded memory growth while allowing reasonable backfill windows.
   * 
   * @param {string} market - Market identifier (e.g., "COINBASE:BTC-USD")
   */
  cleanArchivedBars(market) {
    const barsMap = this.archivedBars[market]
    if (!barsMap) return

    const keepWindow = this.baseTimeframe * 100
    const now = Date.now()
    const cutoff = Math.floor(now / this.baseTimeframe) * this.baseTimeframe - keepWindow

    for (const ts in barsMap) {
      if (Number(ts) < cutoff) {
        delete barsMap[ts]
      }
    }
  }

  /**
   * Main entry point for saving trades.
   * Aggregates trades into pending bars and flushes to disk on interval.
   * 
   * @param {import('../../typedef').Trade[]} trades - Array of trade objects to process
   * @param {boolean} isExiting - If true, forces immediate flush (used on shutdown)
   * @returns {Promise<void>}
   */
  async save(trades, isExiting) {
    if (!trades || !trades.length) {
      return Promise.resolve()
    }

    // Aggregate trades into pending bars
    this.processTrades(trades)

    // Check if it's time to flush (backup interval aligned with resample interval)
    const now = Date.now()
    const timeBackupFloored = Math.floor(now / config.backupInterval) * config.backupInterval
    const timeMinuteFloored = Math.floor(now / config.influxResampleInterval) * config.influxResampleInterval

    if (isExiting || timeBackupFloored === timeMinuteFloored) {
      await this.flush(isExiting)
    }
  }

  /**
   * Processes an array of trades and aggregates them into pending bars.
   * 
   * Bar lookup priority (for backfill support):
   * 1. pendingBars - already in memory, not yet flushed
   * 2. archivedBars - recently flushed, cached for merging
   * 3. Disk fallback - read single record from binary file
   * 4. Create new empty bar
   * 
   * Merge semantics:
   * - "Sticky open": open price is only set when null (first trade wins)
   * - "Last write wins close": close updates as trades arrive
   * - high/low: extrema of all trades
   * - volumes/counts: cumulative sums
   * 
   * @param {import('../../typedef').Trade[]} trades - Array of trade objects
   */
  processTrades(trades) {
    /** @type {Object<string, {low: number, high: number, close: number}>} */
    const ranges = {}
    const timeframe = this.baseTimeframe

    for (let i = 0; i < trades.length; i++) {
      const trade = trades[i]
      const market = trade.exchange + ':' + trade.pair

      // Track price ranges for alert crossover checks (non-liquidation trades only)
      if (!trade.liquidation) {
        if (!ranges[market]) {
          ranges[market] = { low: trade.price, high: trade.price, close: trade.price }
        } else {
          ranges[market].low = Math.min(ranges[market].low, trade.price)
          ranges[market].high = Math.max(ranges[market].high, trade.price)
          ranges[market].close = trade.price
        }
      }

      // Floor trade timestamp to bucket boundary
      const tradeFlooredTime = Math.floor(trade.timestamp / timeframe) * timeframe

      // Ensure market maps exist
      if (!this.pendingBars[market]) {
        this.pendingBars[market] = {}
      }
      if (!this.archivedBars[market]) {
        this.archivedBars[market] = {}
      }

      // Priority 1: Check pendingBars
      let bar = this.pendingBars[market][tradeFlooredTime]

      // Priority 2: Check archivedBars cache
      if (!bar) {
        bar = this.archivedBars[market][tradeFlooredTime]
        if (bar) {
          // Re-add to pendingBars so it can accumulate more trades
          this.pendingBars[market][tradeFlooredTime] = bar
        }
      }

      // Priority 3: Disk fallback (only for base timeframe)
      if (!bar) {
        // Try disk fallback for base timeframe only
        const parsed = parseMarket(market)
        if (parsed) {
          const diskBar = readSingleRecordAtTs(parsed.exchange, parsed.symbol, timeframe, tradeFlooredTime)
          if (diskBar) {
            bar = {
              time: tradeFlooredTime,
              market: market,
              cbuy: diskBar.cbuy,
              csell: diskBar.csell,
              vbuy: diskBar.vbuy,
              vsell: diskBar.vsell,
              lbuy: diskBar.lbuy,
              lsell: diskBar.lsell,
              open: diskBar.open,
              high: diskBar.high,
              low: diskBar.low,
              close: diskBar.close
            }
            this.archivedBars[market][tradeFlooredTime] = bar
            this.pendingBars[market][tradeFlooredTime] = bar
          }
        }
      }

      if (!bar) {
        bar = this.pendingBars[market][tradeFlooredTime] = {
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
          close: null
        }
      }

      if (trade.liquidation) {
        bar['l' + trade.side] += trade.price * trade.size
      } else {
        // Sticky open: only set if null
        if (bar.open === null) {
          bar.open = bar.high = bar.low = bar.close = +trade.price
        } else {
          // Update high/low extrema and close price
          bar.high = Math.max(bar.high, +trade.price)
          bar.low = Math.min(bar.low, +trade.price)
          bar.close = +trade.price
        }
        // Increment trade count and volume for the appropriate side
        bar['c' + trade.side] += trade.count || 1
        bar['v' + trade.side] += trade.price * trade.size
      }
    }

    // Update price indexes and check for alert crossovers
    updateIndexes(ranges, async (index, high, low, direction) => {
      if (alertService && alertService.checkPriceCrossover) {
        await alertService.checkPriceCrossover(index, high, low, direction)
      }
    })
  }

  /**
   * Flushes closed pending bars to disk.
   * 
   * A bar is "closed" when its bucket time is before the current bucket threshold.
   * Closed bars are:
   * 1. Written to disk via upsertBars()
   * 2. Copied to archivedBars for future backfill merging
   * 3. Removed from pendingBars
   * 
   * After base timeframe write, triggers resampling to higher timeframes.
   * 
   * @param {boolean} isExiting - If true, flushes all bars regardless of close status
   * @returns {Promise<void>}
   */
  async flush(isExiting) {
    const now = Date.now()
    const timeframe = this.baseTimeframe
    // Current bucket start time - bars before this are considered "closed"
    const closedThreshold = Math.floor(now / timeframe) * timeframe

    for (const market in this.pendingBars) {
      const barsMap = this.pendingBars[market]
      const closedBars = []

      // Collect closed bars and remove from pending
      for (const ts in barsMap) {
        const barTime = Number(ts)
        const alignedBarTime = Math.floor(barTime / timeframe) * timeframe

        // Bar is closed if its time is before current bucket (or if exiting)
        if (isExiting || alignedBarTime < closedThreshold) {
          const bar = barsMap[ts]
          // Only persist bars that have actual data (non-null close)
          if (bar.close !== null) {
            bar.time = alignedBarTime
            closedBars.push(bar)
          }
          delete barsMap[ts]
        }
      }

      if (closedBars.length === 0) {
        this.cleanArchivedBars(market)
        continue
      }

      // Sort by time for consistent write order
      closedBars.sort((a, b) => a.time - b.time)

      const parsed = parseMarket(market)
      if (!parsed) {
        this.cleanArchivedBars(market)
        continue
      }

      // Write to disk (may include overwrites for backfilled buckets)
      const writeResult = await upsertBars(parsed.exchange, parsed.symbol, timeframe, closedBars)

      // Copy closed bars to archivedBars for future backfill merging
      // This allows late-arriving trades to merge into recently flushed buckets
      if (!this.archivedBars[market]) {
        this.archivedBars[market] = {}
      }
      for (const bar of closedBars) {
        this.archivedBars[market][bar.time] = bar
      }

      // Clean old archived bars to prevent unbounded memory growth
      this.cleanArchivedBars(market)

      // Resample to higher timeframes if we wrote any data
      if (writeResult && writeResult.fromTsWritten !== null) {
        await resampleToHigherTimeframes(this, parsed.exchange, parsed.symbol, writeResult.fromTsWritten, writeResult.toTsWritten)
      }
    }
  }

  /**
   * Fetches historical OHLCV data for one or more markets.
   * 
   * Returns data in a columnar format suitable for charting.
   * Null bars (gaps) are skipped in the response.
   * Pending bars near "now" are injected to provide real-time data.
   * 
   * Now reads from segmented files: computes which segments intersect [from, to)
   * and reads only the required records from each segment.
   * 
   * @param {Object} params - Query parameters
   * @param {number} params.from - Start timestamp (inclusive)
   * @param {number} params.to - End timestamp (exclusive)
   * @param {number} [params.timeframe=60000] - Timeframe in milliseconds
   * @param {string[]} [params.markets=[]] - Array of market identifiers
   * @returns {Promise<{format: string, columns: Object, results: Array[]}>} Query results
   */
  fetch({ from, to, timeframe = 60000, markets = [] }) {
    // Validate timeframe is supported
    const supported = this.timeframes.indexOf(timeframe) !== -1
    if (!supported) {
      return Promise.reject(new Error(`Unsupported timeframe: ${getHms(timeframe)}`))
    }

    // Column index mapping for result arrays (matches InfluxDB format)
    const columns = {
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
    }

    // Pre-allocate with estimated capacity to reduce array resizing
    const results = []

    // Check if we need pending bars (query extends to near-realtime)
    const now = Date.now()
    const needsPendingBars = to > now - config.influxResampleInterval

    // Compute segment range for the query
    const segmentSpanMs = getSegmentSpanMs(timeframe)
    const firstSegmentStartTs = getSegmentStartTs(from, timeframe)
    const lastSegmentStartTs = getSegmentStartTs(to - 1, timeframe) // to is exclusive

    // Read data from disk for each market
    for (const market of markets) {
      const parsed = parseMarket(market)
      if (!parsed) continue

      // Iterate through all segments that intersect [from, to)
      for (let segmentStartTs = firstSegmentStartTs; segmentStartTs <= lastSegmentStartTs; segmentStartTs += segmentSpanMs) {
        const paths = getSegmentPaths(parsed.exchange, parsed.symbol, timeframe, segmentStartTs)
        const meta = readMeta(paths.json)
        if (!meta) continue

        // Calculate index range within this segment for the requested time window
        const segmentEndTs = segmentStartTs + segmentSpanMs
        const readFromTs = Math.max(from, segmentStartTs)
        const readToTs = Math.min(to, segmentEndTs)

        if (readFromTs >= readToTs) continue

        let startIndex = Math.floor((readFromTs - segmentStartTs) / timeframe)
        let endIndex = Math.ceil((readToTs - segmentStartTs) / timeframe)

        // Clamp to valid range (use meta.records, avoids fstatSync)
        if (startIndex < 0) startIndex = 0
        if (endIndex > meta.records) endIndex = meta.records
        if (startIndex >= endIndex) continue

        const count = endIndex - startIndex
        const byteOffset = startIndex * RECORD_SIZE
        const byteLength = count * RECORD_SIZE

        // Compute max bytes from meta.records (avoids fstatSync)
        const maxBytesFromMeta = meta.records * RECORD_SIZE
        const maxBytes = Math.min(byteLength, maxBytesFromMeta - byteOffset)
        if (maxBytes <= 0) continue

        // Use cached fd (avoids openSync/closeSync per segment)
        const fd = openFdCached(paths.bin, 'r')
        if (fd === null) continue

        let buffer
        try {
          buffer = Buffer.alloc(maxBytes)
          fs.readSync(fd, buffer, 0, maxBytes, byteOffset)
        } catch (_e) {
          continue
        }

        // Decode records and add to results
        const recordCount = Math.floor(buffer.length / RECORD_SIZE)
        for (let i = 0; i < recordCount; i++) {
          const offset = i * RECORD_SIZE

          // Fast null-bar check (only reads 16 bytes, no decode)
          if (isNullRecord(buffer, offset)) continue

          const record = readRecord(buffer, offset, meta)

          const barTime = segmentStartTs + (startIndex + i) * timeframe

          // Filter by time range
          if (barTime < from || barTime >= to) continue

          // Add as array matching column order (includes market)
          // Time is converted to seconds to match InfluxDB format (precision: 's')
          results.push([
            Math.floor(barTime / 1000),
            market,
            record.open,
            record.high,
            record.low,
            record.close,
            record.vbuy,
            record.vsell,
            record.cbuy,
            record.csell,
            record.lbuy,
            record.lsell
          ])
        }
      }
    }

    // Inject pending bars for near-realtime data
    if (needsPendingBars) {
      const pendingBars = this.getPendingBars(markets, from, to)
      for (let i = 0; i < pendingBars.length; i++) {
        const bar = pendingBars[i]
        // Time is converted to seconds to match InfluxDB format (precision: 's')
        results.push([
          Math.floor(bar.time / 1000),
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
        ])
      }
    }

    // Single sort at the end (by time)
    // For large datasets, this is O(n log n) - unavoidable when merging multiple sources
    if (markets.length > 1 || needsPendingBars) {
      results.sort((a, b) => a[0] - b[0])
    }

    return Promise.resolve({
      format: this.format,
      columns,
      results
    })
  }

  /**
   * Gets pending (in-memory, not yet flushed) bars for given markets and time range.
   * 
   * Used by fetch() to inject real-time data for queries near "now".
   * Returns base timeframe bars as-is (no resampling - matches InfluxDB behavior).
   * 
   * @param {string[]} markets - Array of market identifiers to include
   * @param {number} from - Start timestamp (inclusive)
   * @param {number} to - End timestamp (exclusive)
   * @returns {import('./constants').Bar[]} Array of pending bar objects with market field
   */
  getPendingBars(markets, from, to) {
    const results = []

    for (const market of markets) {
      const barsMap = this.pendingBars[market]
      if (!barsMap) continue

      for (const ts in barsMap) {
        const bar = barsMap[ts]

        // Skip empty bars
        if (bar.close === null) continue

        const barTime = Number(ts)

        // Filter by requested time range
        if (barTime < from || barTime >= to) continue

        results.push(bar)
      }
    }

    return results
  }
}

module.exports = BinariesStorage
