const fs = require('fs')
const path = require('path')
const { ensureDirectoryExists, getHms } = require('../helper')
const config = require('../config')
const alertService = require('../services/alert')
const { updateIndexes } = require('../services/connections')

/**
 * Size in bytes of a single OHLCV record in the binary file.
 * Layout (56 bytes total):
 *   - open:  Int32LE  (4 bytes, offset 0)
 *   - high:  Int32LE  (4 bytes, offset 4)
 *   - low:   Int32LE  (4 bytes, offset 8)
 *   - close: Int32LE  (4 bytes, offset 12)
 *   - vbuy:  BigInt64LE (8 bytes, offset 16)
 *   - vsell: BigInt64LE (8 bytes, offset 24)
 *   - cbuy:  UInt32LE (4 bytes, offset 32)
 *   - csell: UInt32LE (4 bytes, offset 36)
 *   - lbuy:  BigInt64LE (8 bytes, offset 40)
 *   - lsell: BigInt64LE (8 bytes, offset 48)
 * @constant {number}
 */
const RECORD_SIZE = 56

/**
 * Scale factor for storing prices as integers.
 * Prices are multiplied by this value before storing and divided when reading.
 * Example: price 45000.1234 becomes 450001234 (stored as Int32).
 * @constant {number}
 */
const PRICE_SCALE = 10000

/**
 * Scale factor for storing volumes as integers.
 * Volumes are multiplied by this value before storing and divided when reading.
 * @constant {number}
 */
const VOLUME_SCALE = 1000000

/**
 * @typedef {Object} BinaryMeta
 * @property {string} exchange - Exchange name
 * @property {string} symbol - Trading pair symbol
 * @property {string} timeframe - Human-readable timeframe label (e.g., "5s", "1m")
 * @property {number} timeframeMs - Timeframe in milliseconds
 * @property {number} startTs - Timestamp of the first record (bucket time)
 * @property {number} endTs - Timestamp just past the last record (startTs + records * timeframeMs)
 * @property {number} priceScale - Price scaling factor used
 * @property {number} volumeScale - Volume scaling factor used
 * @property {number} records - Total number of records in the binary file
 * @property {number} lastInputStartTs - Last input bar timestamp processed
 */

/**
 * @typedef {Object} FilePaths
 * @property {string} bin - Path to the binary data file
 * @property {string} json - Path to the JSON metadata file
 */

/**
 * @typedef {Object} ParsedMarket
 * @property {string} exchange - Exchange name
 * @property {string} symbol - Trading pair symbol
 */

/**
 * @typedef {Object} WriteResult
 * @property {number|null} fromTsWritten - Minimum timestamp written (null if nothing written)
 * @property {number|null} toTsWritten - Maximum timestamp written (null if nothing written)
 */

/**
 * @typedef {Object} RecordData
 * @property {number} open - Open price
 * @property {number} high - High price
 * @property {number} low - Low price
 * @property {number} close - Close price
 * @property {number} vbuy - Buy volume (price * size)
 * @property {number} vsell - Sell volume (price * size)
 * @property {number} cbuy - Buy trade count
 * @property {number} csell - Sell trade count
 * @property {number} lbuy - Buy liquidation volume
 * @property {number} lsell - Sell liquidation volume
 */

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
     * @type {Object<string, Object<number, Bar>>}
     */
    this.pendingBars = {}

    /**
     * Cache of recently persisted bars for backfill merging.
     * Allows late-arriving trades to merge into already-flushed buckets.
     * Structure: { [market: string]: { [timestamp: number]: Bar } }
     * @type {Object<string, Object<number, Bar>>}
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
   * Reads a single record from the binary file at a specific bucket timestamp.
   * Uses positioned read (pread) to read only the needed 56 bytes.
   * 
   * This is used as a disk fallback when processing trades for buckets
   * that exist on disk but not in memory caches.
   * 
   * @param {string} exchange - Exchange name
   * @param {string} symbol - Trading pair symbol
   * @param {number} timeframe - Timeframe in milliseconds
   * @param {number} bucketTs - Bucket timestamp to read
   * @returns {Bar|null} Bar data if found and non-null, otherwise null
   */
  readSingleRecordAtTs(exchange, symbol, timeframe, bucketTs) {
    const paths = this.getFilePath(exchange, symbol, timeframe)
    const meta = this.readMeta(paths.json)
    if (!meta) return null
    if (!fs.existsSync(paths.bin)) return null

    // Check if bucketTs is within the file's range
    if (bucketTs < meta.startTs || bucketTs >= meta.endTs) return null

    // Calculate record index and byte offset
    const index = Math.floor((bucketTs - meta.startTs) / meta.timeframeMs)
    if (index < 0 || index >= meta.records) return null

    const byteOffset = index * RECORD_SIZE
    const buffer = Buffer.alloc(RECORD_SIZE)

    let fd
    try {
      fd = fs.openSync(paths.bin, 'r')
      const stat = fs.fstatSync(fd)
      if (byteOffset + RECORD_SIZE > stat.size) {
        fs.closeSync(fd)
        return null
      }
      // Positioned read - only reads the single record we need
      fs.readSync(fd, buffer, 0, RECORD_SIZE, byteOffset)
      fs.closeSync(fd)
    } catch (_e) {
      if (fd !== undefined) {
        try { fs.closeSync(fd) } catch (_) { /* ignored */ }
      }
      return null
    }

    const record = this.readRecord(buffer, 0, meta)

    // Check if this is a null record (all OHLC are zero)
    if (record.open === 0 && record.high === 0 && record.low === 0 && record.close === 0) {
      return null
    }

    return {
      time: bucketTs,
      open: record.open,
      high: record.high,
      low: record.low,
      close: record.close,
      vbuy: record.vbuy,
      vsell: record.vsell,
      cbuy: record.cbuy,
      csell: record.csell,
      lbuy: record.lbuy,
      lsell: record.lsell
    }
  }

  /**
   * Generates file paths for a market's binary and metadata files.
   * 
   * @param {string} exchange - Exchange name
   * @param {string} symbol - Trading pair symbol (will be sanitized)
   * @param {number} timeframe - Timeframe in milliseconds
   * @returns {FilePaths} Object with bin and json file paths
   */
  getFilePath(exchange, symbol, timeframe) {
    // Sanitize symbol to create valid filename (replace / and : with -)
    const sanitizedSymbol = symbol.replace(/[/:]/g, '-')
    const tfLabel = getHms(timeframe)
    const dir = path.join(config.filesLocation, exchange, sanitizedSymbol)
    return {
      bin: path.join(dir, `${tfLabel}.bin`),
      json: path.join(dir, `${tfLabel}.json`)
    }
  }

  /**
   * Parses a market string into exchange and symbol components.
   * 
   * @param {string} market - Market identifier (e.g., "COINBASE:BTC-USD")
   * @returns {ParsedMarket|null} Parsed components or null if invalid format
   */
  parseMarket(market) {
    const match = market.match(/([^:]*):(.*)/)
    if (!match) return null
    return { exchange: match[1], symbol: match[2] }
  }

  /**
   * Main entry point for saving trades.
   * Aggregates trades into pending bars and flushes to disk on interval.
   * 
   * @param {Trade[]} trades - Array of trade objects to process
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
   * @param {Trade[]} trades - Array of trade objects
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
        const parsed = this.parseMarket(market)
        if (parsed) {
          const diskBar = this.readSingleRecordAtTs(parsed.exchange, parsed.symbol, timeframe, tradeFlooredTime)
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

      const parsed = this.parseMarket(market)
      if (!parsed) {
        this.cleanArchivedBars(market)
        continue
      }

      // Write to disk (may include overwrites for backfilled buckets)
      const writeResult = await this.upsertBars(parsed.exchange, parsed.symbol, timeframe, closedBars)

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
        await this.resampleToHigherTimeframes(parsed.exchange, parsed.symbol, writeResult.fromTsWritten, writeResult.toTsWritten)
      }
    }
  }

  /**
   * Triggers resampling from base timeframe to all configured higher timeframes.
   * 
   * @param {string} exchange - Exchange name
   * @param {string} symbol - Trading pair symbol
   * @param {number} fromTs - Start of dirty range (earliest modified bucket)
   * @param {number} toTs - End of dirty range (latest modified bucket)
   * @returns {Promise<void>}
   */
  async resampleToHigherTimeframes(exchange, symbol, fromTs, toTs) {
    const baseTimeframe = this.baseTimeframe
    const higherTimeframes = this.timeframes.filter(tf => tf > baseTimeframe)

    if (higherTimeframes.length === 0) return

    const basePaths = this.getFilePath(exchange, symbol, baseTimeframe)
    const baseMeta = this.readMeta(basePaths.json)
    if (!baseMeta || !fs.existsSync(basePaths.bin)) return

    // Resample each higher timeframe
    for (const targetTimeframe of higherTimeframes) {
      await this.resampleRangeToTimeframe(exchange, symbol, baseMeta, basePaths.bin, fromTs, toTs, targetTimeframe)
    }
  }

  /**
   * Resamples a range of base timeframe data into a higher target timeframe.
   * 
   * Unlike append-only resampling, this supports upsert semantics:
   * - Overwrites existing target buckets that were affected by base changes
   * - Appends new target buckets as needed
   * - Maintains dense indexing (null bars fill gaps)
   * 
   * Aggregation rules:
   * - open: first non-null value in bucket (by base bar time order)
   * - close: last non-null value in bucket
   * - high/low: extrema across all base bars
   * - volumes/counts: cumulative sums
   * 
   * @param {string} exchange - Exchange name
   * @param {string} symbol - Trading pair symbol
   * @param {BinaryMeta} baseMeta - Metadata for base timeframe file
   * @param {string} baseBinPath - Path to base timeframe binary file
   * @param {number} fromTs - Start of dirty range (base bucket time)
   * @param {number} toTs - End of dirty range (base bucket time)
   * @param {number} targetTimeframe - Target timeframe in milliseconds
   * @returns {Promise<void>}
   */
  async resampleRangeToTimeframe(exchange, symbol, baseMeta, baseBinPath, fromTs, toTs, targetTimeframe) {
    const baseTimeframe = baseMeta.timeframeMs
    const paths = this.getFilePath(exchange, symbol, targetTimeframe)
    await ensureDirectoryExists(paths.bin)

    let meta = this.readMeta(paths.json)

    // Compute dirty target bucket range (inclusive)
    // A target bucket is dirty if any of its contributing base buckets were modified
    const firstTargetBucket = Math.floor(fromTs / targetTimeframe) * targetTimeframe
    const lastTargetBucket = Math.floor(toTs / targetTimeframe) * targetTimeframe

    // Initialize metadata if this is a new file
    if (!meta) {
      meta = {
        exchange: exchange,
        symbol: symbol,
        timeframe: getHms(targetTimeframe),
        timeframeMs: targetTimeframe,
        startTs: firstTargetBucket,
        endTs: firstTargetBucket,
        priceScale: PRICE_SCALE,
        volumeScale: VOLUME_SCALE,
        records: 0,
        lastInputStartTs: firstTargetBucket
      }
    }

    // Skip buckets before meta.startTs (no prepend allowed)
    const startBucket = Math.max(firstTargetBucket, meta.startTs)
    if (startBucket > lastTargetBucket) return

    // Calculate base record index range covering the dirty target buckets
    // Need to read all base bars that contribute to [startBucket, lastTargetBucket + targetTimeframe)
    const baseStartIndex = Math.floor((startBucket - baseMeta.startTs) / baseTimeframe)
    const baseEndIndex = Math.ceil((lastTargetBucket + targetTimeframe - baseMeta.startTs) / baseTimeframe)

    // Clamp to valid index range within the base file
    const clampedStartIndex = Math.max(0, baseStartIndex)
    const clampedEndIndex = Math.min(baseMeta.records, baseEndIndex)

    // Aggregate base bars into target buckets
    // Key: target bucket timestamp, Value: aggregated bar data
    const aggregated = {}

    if (clampedStartIndex < clampedEndIndex) {
      const count = clampedEndIndex - clampedStartIndex
      const byteOffset = clampedStartIndex * RECORD_SIZE
      const byteLength = count * RECORD_SIZE

      let buffer
      try {
        const fd = fs.openSync(baseBinPath, 'r')
        const stat = fs.fstatSync(fd)
        const maxBytes = Math.min(byteLength, stat.size - byteOffset)
        if (maxBytes > 0) {
          buffer = Buffer.alloc(maxBytes)
          fs.readSync(fd, buffer, 0, maxBytes, byteOffset)
        }
        fs.closeSync(fd)
      } catch (_e) {
        buffer = null
      }

      if (buffer) {
        const recordCount = Math.floor(buffer.length / RECORD_SIZE)

        // Process each base record and aggregate into target buckets
        for (let i = 0; i < recordCount; i++) {
          const record = this.readRecord(buffer, i * RECORD_SIZE, baseMeta)
          const barTime = baseMeta.startTs + (clampedStartIndex + i) * baseTimeframe

          // Skip null records (gaps in base data)
          if (record.open === 0 && record.high === 0 && record.low === 0 && record.close === 0) continue

          // Determine which target bucket this base bar contributes to
          const targetBucketTime = Math.floor(barTime / targetTimeframe) * targetTimeframe
          if (targetBucketTime < startBucket || targetBucketTime > lastTargetBucket) continue

          if (!aggregated[targetBucketTime]) {
            // First contributing base bar for this target bucket
            aggregated[targetBucketTime] = {
              time: targetBucketTime,
              open: record.open,
              high: record.high,
              low: record.low,
              close: record.close,
              vbuy: record.vbuy,
              vsell: record.vsell,
              cbuy: record.cbuy,
              csell: record.csell,
              lbuy: record.lbuy,
              lsell: record.lsell,
              _firstBarTime: barTime // Track earliest base bar for open determination
            }
          } else {
            // Merge subsequent base bars into the aggregate
            const agg = aggregated[targetBucketTime]

            // Open comes from the earliest base bar (by time, not arrival order)
            if (barTime < agg._firstBarTime) {
              agg.open = record.open
              agg._firstBarTime = barTime
            }

            // High/low are extrema, close comes from the last bar processed
            agg.high = Math.max(agg.high, record.high)
            agg.low = Math.min(agg.low, record.low)
            agg.close = record.close

            // Volumes and counts are summed
            agg.vbuy += record.vbuy
            agg.vsell += record.vsell
            agg.cbuy += record.cbuy
            agg.csell += record.csell
            agg.lbuy += record.lbuy
            agg.lsell += record.lsell
          }
        }
      }
    }

    // Build complete list of bars to upsert (dense: one per target bucket in range)
    const barsToUpsert = []
    for (let bucketTs = startBucket; bucketTs <= lastTargetBucket; bucketTs += targetTimeframe) {
      if (aggregated[bucketTs]) {
        barsToUpsert.push(aggregated[bucketTs])
      } else {
        // No contributing base bars - write null bar for dense indexing
        barsToUpsert.push(this.createNullBar(bucketTs))
      }
    }

    if (barsToUpsert.length === 0) return

    // Separate bars into overwrites (existing records) vs appends (new records)
    const overwrites = []
    const appends = []

    for (const bar of barsToUpsert) {
      const index = Math.floor((bar.time - meta.startTs) / targetTimeframe)
      if (index < 0) continue // No prepend allowed
      if (index < meta.records) {
        overwrites.push({ index, bar })
      } else {
        appends.push({ index, bar })
      }
    }

    // Handle overwrites (positioned writes to existing records)
    if (overwrites.length > 0) {
      let fd = null
      try {
        // Open in read-write mode for in-place updates
        fd = fs.openSync(paths.bin, 'r+')

        // Sort by index for sequential access and batch contiguous writes
        overwrites.sort((a, b) => a.index - b.index)
        let groupStart = 0

        // Group contiguous indices for efficient batch writes
        while (groupStart < overwrites.length) {
          let groupEnd = groupStart

          // Find end of contiguous run
          while (groupEnd + 1 < overwrites.length && 
                 overwrites[groupEnd + 1].index === overwrites[groupEnd].index + 1) {
            groupEnd++
          }

          // Write contiguous group in single syscall
          const groupSize = groupEnd - groupStart + 1
          const buffer = Buffer.alloc(groupSize * RECORD_SIZE)
          let bufferOffset = 0

          for (let i = groupStart; i <= groupEnd; i++) {
            this.writeRecord(buffer, bufferOffset, overwrites[i].bar, meta)
            bufferOffset += RECORD_SIZE
          }

          const fileOffset = overwrites[groupStart].index * RECORD_SIZE
          fs.writeSync(fd, buffer, 0, buffer.length, fileOffset)

          groupStart = groupEnd + 1
        }
      } finally {
        if (fd !== null) {
          fs.closeSync(fd)
        }
      }
    }

    // Handle appends (extend file with new records)
    if (appends.length > 0) {
      appends.sort((a, b) => a.index - b.index)
      const lastAppendIndex = appends[appends.length - 1].index

      // Build lookup map for sparse appends
      const appendMap = {}
      for (const a of appends) {
        appendMap[a.index] = a.bar
      }

      const BATCH_SIZE = 10000
      let currentIndex = meta.records
      let fd = null

      try {
        // Open in append mode
        fd = fs.openSync(paths.bin, 'a')

        // Write records from current end to last append index (dense, filling gaps with null)
        while (currentIndex <= lastAppendIndex) {
          const batchRecords = []
          const batchEndIndex = Math.min(currentIndex + BATCH_SIZE, lastAppendIndex + 1)

          while (currentIndex < batchEndIndex) {
            if (appendMap[currentIndex]) {
              batchRecords.push(appendMap[currentIndex])
            } else {
              // Fill gap with null bar
              batchRecords.push(this.createNullBar(meta.startTs + currentIndex * targetTimeframe))
            }
            currentIndex++
          }

          if (batchRecords.length > 0) {
            const buffer = Buffer.alloc(batchRecords.length * RECORD_SIZE)
            let offset = 0

            for (const bar of batchRecords) {
              this.writeRecord(buffer, offset, bar, meta)
              offset += RECORD_SIZE
            }

            fs.writeSync(fd, buffer)
            meta.records += batchRecords.length
          }
        }
      } finally {
        if (fd !== null) {
          fs.closeSync(fd)
        }
      }

      // Update endTs after extending file
      meta.endTs = meta.startTs + meta.records * targetTimeframe
    }

    meta.lastInputStartTs = lastTargetBucket
    this.writeMeta(paths.json, meta)
  }

  /**
   * Appends bars to a binary file (append-only, no overwrites).
   * 
   * @deprecated Use upsertBars() instead for backfill support
   * @param {string} exchange - Exchange name
   * @param {string} symbol - Trading pair symbol
   * @param {number} timeframe - Timeframe in milliseconds
   * @param {Bar[]} bars - Array of bars to append
   * @returns {Promise<WriteResult>} Timestamps of written data range
   */
  async appendBars(exchange, symbol, timeframe, bars) {
    const paths = this.getFilePath(exchange, symbol, timeframe)
    await ensureDirectoryExists(paths.bin)

    let meta = this.readMeta(paths.json)

    // Get time bounds of input bars
    const firstBarTime = Math.floor(bars[0].time / timeframe) * timeframe
    const lastBarTime = Math.floor(bars[bars.length - 1].time / timeframe) * timeframe

    // Initialize metadata if new file
    if (!meta) {
      meta = {
        exchange: exchange,
        symbol: symbol,
        timeframe: getHms(timeframe),
        timeframeMs: timeframe,
        startTs: firstBarTime,
        endTs: firstBarTime,
        priceScale: PRICE_SCALE,
        volumeScale: VOLUME_SCALE,
        records: 0,
        lastInputStartTs: firstBarTime
      }
    }

    // Filter to only bars that can be appended (>= endTs, non-null close)
    const barsToWriteMap = {}
    for (const bar of bars) {
      const alignedTime = Math.floor(bar.time / timeframe) * timeframe
      if (alignedTime < meta.endTs) continue // Skip existing records
      if (bar.close === null) continue // Skip empty bars
      barsToWriteMap[alignedTime] = bar
    }

    const timestamps = Object.keys(barsToWriteMap).map(Number).sort((a, b) => a - b)
    if (timestamps.length === 0) return { fromTsWritten: null, toTsWritten: null }

    const lastNewBarTime = timestamps[timestamps.length - 1]
    const fromTsWritten = meta.endTs

    const BATCH_SIZE = 10000
    let currentTs = meta.endTs
    let fd = null

    try {
      fd = fs.openSync(paths.bin, 'a')

      // Write records from endTs to lastNewBarTime (dense, with gap filling)
      while (currentTs <= lastNewBarTime) {
        const batchRecords = []
        const batchEnd = Math.min(currentTs + BATCH_SIZE * timeframe, lastNewBarTime + timeframe)

        while (currentTs < batchEnd && currentTs <= lastNewBarTime) {
          if (barsToWriteMap[currentTs]) {
            batchRecords.push(barsToWriteMap[currentTs])
          } else {
            // Fill gap with null bar
            batchRecords.push(this.createNullBar(currentTs))
          }
          currentTs += timeframe
        }

        if (batchRecords.length > 0) {
          const buffer = Buffer.alloc(batchRecords.length * RECORD_SIZE)
          let offset = 0

          for (const bar of batchRecords) {
            this.writeRecord(buffer, offset, bar, meta)
            offset += RECORD_SIZE
          }

          fs.writeSync(fd, buffer)
          meta.records += batchRecords.length
        }
      }
    } finally {
      if (fd !== null) {
        fs.closeSync(fd)
      }
    }

    // Update metadata
    meta.endTs = meta.startTs + meta.records * timeframe
    meta.lastInputStartTs = lastBarTime

    this.writeMeta(paths.json, meta)

    return { fromTsWritten, toTsWritten: lastNewBarTime }
  }

  /**
   * Upserts bars to a binary file with support for both overwrites and appends.
   * 
   * This is the primary write method that supports backfill scenarios:
   * - Bars with timestamps >= endTs are appended (with gap filling)
   * - Bars with timestamps < endTs (but >= startTs) overwrite existing records
   * - Bars with timestamps < startTs are ignored (no prepend)
   * 
   * @param {string} exchange - Exchange name
   * @param {string} symbol - Trading pair symbol
   * @param {number} timeframe - Timeframe in milliseconds
   * @param {Bar[]} bars - Array of bars to upsert
   * @returns {Promise<WriteResult>} Timestamps of written data range (includes overwrites)
   */
  async upsertBars(exchange, symbol, timeframe, bars) {
    const paths = this.getFilePath(exchange, symbol, timeframe)
    await ensureDirectoryExists(paths.bin)

    let meta = this.readMeta(paths.json)

    // Filter out bars with null close (empty bars)
    const validBars = bars.filter(b => b.close !== null)
    if (validBars.length === 0) return { fromTsWritten: null, toTsWritten: null }

    // Build map of aligned timestamps to bars
    const barsMap = {}
    for (const bar of validBars) {
      const alignedTime = Math.floor(bar.time / timeframe) * timeframe
      barsMap[alignedTime] = bar
    }

    const timestamps = Object.keys(barsMap).map(Number).sort((a, b) => a - b)
    if (timestamps.length === 0) return { fromTsWritten: null, toTsWritten: null }

    const firstBarTime = timestamps[0]
    const _lastBarTime = timestamps[timestamps.length - 1]

    // Initialize metadata if new file
    if (!meta) {
      meta = {
        exchange: exchange,
        symbol: symbol,
        timeframe: getHms(timeframe),
        timeframeMs: timeframe,
        startTs: firstBarTime,
        endTs: firstBarTime,
        priceScale: PRICE_SCALE,
        volumeScale: VOLUME_SCALE,
        records: 0,
        lastInputStartTs: firstBarTime
      }
    }

    // Filter out bars before startTs (no prepend allowed)
    const filteredTimestamps = timestamps.filter(ts => ts >= meta.startTs)
    if (filteredTimestamps.length === 0) return { fromTsWritten: null, toTsWritten: null }

    // Track the actual dirty range (for resampling)
    const minTs = filteredTimestamps[0]
    const maxTs = filteredTimestamps[filteredTimestamps.length - 1]

    // Categorize bars as overwrites or appends based on their index
    const overwrites = [] // { index, ts, bar } - existing records to update
    const appends = []    // { index, ts, bar } - new records to add

    for (const ts of filteredTimestamps) {
      const index = Math.floor((ts - meta.startTs) / timeframe)
      if (index < meta.records) {
        // Index exists in file - this is an overwrite
        overwrites.push({ index, ts, bar: barsMap[ts] })
      } else {
        // Index beyond current file - this is an append
        appends.push({ index, ts, bar: barsMap[ts] })
      }
    }

    // Handle overwrites (positioned writes to existing records)
    if (overwrites.length > 0) {
      let fd = null
      try {
        // Open in read-write mode for in-place updates
        fd = fs.openSync(paths.bin, 'r+')

        // Sort and group contiguous indices for efficient batch writes
        overwrites.sort((a, b) => a.index - b.index)
        let groupStart = 0

        while (groupStart < overwrites.length) {
          let groupEnd = groupStart

          // Find end of contiguous run
          while (groupEnd + 1 < overwrites.length && 
                 overwrites[groupEnd + 1].index === overwrites[groupEnd].index + 1) {
            groupEnd++
          }

          // Write contiguous group in single syscall
          const groupSize = groupEnd - groupStart + 1
          const buffer = Buffer.alloc(groupSize * RECORD_SIZE)
          let bufferOffset = 0

          for (let i = groupStart; i <= groupEnd; i++) {
            this.writeRecord(buffer, bufferOffset, overwrites[i].bar, meta)
            bufferOffset += RECORD_SIZE
          }

          const fileOffset = overwrites[groupStart].index * RECORD_SIZE
          fs.writeSync(fd, buffer, 0, buffer.length, fileOffset)

          groupStart = groupEnd + 1
        }
      } finally {
        if (fd !== null) {
          fs.closeSync(fd)
        }
      }
    }

    // Handle appends (extend file with new records)
    if (appends.length > 0) {
      appends.sort((a, b) => a.index - b.index)
      const _firstAppendIndex = appends[0].index
      const lastAppendIndex = appends[appends.length - 1].index

      // Build lookup map for sparse appends
      const appendMap = {}
      for (const a of appends) {
        appendMap[a.index] = a.bar
      }

      const BATCH_SIZE = 10000
      let currentIndex = meta.records
      let fd = null

      try {
        // Open in append mode
        fd = fs.openSync(paths.bin, 'a')

        // Write records from current end to last append index (dense, filling gaps)
        while (currentIndex <= lastAppendIndex) {
          const batchRecords = []
          const batchEndIndex = Math.min(currentIndex + BATCH_SIZE, lastAppendIndex + 1)

          while (currentIndex < batchEndIndex) {
            if (appendMap[currentIndex]) {
              batchRecords.push(appendMap[currentIndex])
            } else {
              // Fill gap with null bar for dense indexing
              batchRecords.push(this.createNullBar(meta.startTs + currentIndex * timeframe))
            }
            currentIndex++
          }

          if (batchRecords.length > 0) {
            const buffer = Buffer.alloc(batchRecords.length * RECORD_SIZE)
            let offset = 0

            for (const bar of batchRecords) {
              this.writeRecord(buffer, offset, bar, meta)
              offset += RECORD_SIZE
            }

            fs.writeSync(fd, buffer)
            meta.records += batchRecords.length
          }
        }
      } finally {
        if (fd !== null) {
          fs.closeSync(fd)
        }
      }

      // Update endTs after extending file
      meta.endTs = meta.startTs + meta.records * timeframe
    }

    meta.lastInputStartTs = maxTs
    this.writeMeta(paths.json, meta)

    // Return the full dirty range (includes overwrites, not just appends)
    return { fromTsWritten: minTs, toTsWritten: maxTs }
  }

  /**
   * Creates an empty (null) bar for gap filling.
   * Null bars have all OHLC set to null and all volumes/counts set to 0.
   * On disk, these are stored as all zeros and skipped when reading.
   * 
   * @param {number} time - Bucket timestamp
   * @returns {Object} Null bar object
   */
  createNullBar(time) {
    return {
      time,
      open: null,
      high: null,
      low: null,
      close: null,
      vbuy: 0,
      vsell: 0,
      cbuy: 0,
      csell: 0,
      lbuy: 0,
      lsell: 0
    }
  }

  /**
   * Writes a bar record to a buffer at the specified offset.
   * 
   * Binary layout (56 bytes):
   * - Bytes 0-3:   open (Int32LE, scaled by priceScale)
   * - Bytes 4-7:   high (Int32LE, scaled)
   * - Bytes 8-11:  low (Int32LE, scaled)
   * - Bytes 12-15: close (Int32LE, scaled)
   * - Bytes 16-23: vbuy (BigInt64LE, scaled by volumeScale)
   * - Bytes 24-31: vsell (BigInt64LE, scaled)
   * - Bytes 32-35: cbuy (UInt32LE, trade count)
   * - Bytes 36-39: csell (UInt32LE, trade count)
   * - Bytes 40-47: lbuy (BigInt64LE, liquidation volume, scaled)
   * - Bytes 48-55: lsell (BigInt64LE, liquidation volume, scaled)
   * 
   * @param {Buffer} buffer - Target buffer
   * @param {number} offset - Byte offset in buffer
   * @param {Object} bar - Bar data to write
   * @param {BinaryMeta} meta - Metadata with scale factors
   */
  writeRecord(buffer, offset, bar, meta) {
    // Scale and round prices (null becomes 0)
    const open = bar.open !== null ? Math.round(bar.open * meta.priceScale) : 0
    const high = bar.high !== null ? Math.round(bar.high * meta.priceScale) : 0
    const low = bar.low !== null ? Math.round(bar.low * meta.priceScale) : 0
    const close = bar.close !== null ? Math.round(bar.close * meta.priceScale) : 0

    // Write OHLC
    buffer.writeInt32LE(open, offset)
    buffer.writeInt32LE(high, offset + 4)
    buffer.writeInt32LE(low, offset + 8)
    buffer.writeInt32LE(close, offset + 12)

    // Scale and write volumes as BigInt
    const vbuy = BigInt(Math.round((bar.vbuy || 0) * meta.volumeScale))
    const vsell = BigInt(Math.round((bar.vsell || 0) * meta.volumeScale))
    buffer.writeBigInt64LE(vbuy, offset + 16)
    buffer.writeBigInt64LE(vsell, offset + 24)

    // Write trade counts
    buffer.writeUInt32LE(bar.cbuy || 0, offset + 32)
    buffer.writeUInt32LE(bar.csell || 0, offset + 36)

    // Scale and write liquidation volumes
    const lbuy = BigInt(Math.round((bar.lbuy || 0) * meta.volumeScale))
    const lsell = BigInt(Math.round((bar.lsell || 0) * meta.volumeScale))
    buffer.writeBigInt64LE(lbuy, offset + 40)
    buffer.writeBigInt64LE(lsell, offset + 48)
  }

  /**
   * Reads a bar record from a buffer at the specified offset.
   * 
   * @param {Buffer} buffer - Source buffer
   * @param {number} offset - Byte offset in buffer
   * @param {BinaryMeta} meta - Metadata with scale factors
   * @returns {RecordData} Decoded bar data
   */
  readRecord(buffer, offset, meta) {
    // Read and descale OHLC
    const open = buffer.readInt32LE(offset) / meta.priceScale
    const high = buffer.readInt32LE(offset + 4) / meta.priceScale
    const low = buffer.readInt32LE(offset + 8) / meta.priceScale
    const close = buffer.readInt32LE(offset + 12) / meta.priceScale

    // Read and descale volumes
    const vbuy = Number(buffer.readBigInt64LE(offset + 16)) / meta.volumeScale
    const vsell = Number(buffer.readBigInt64LE(offset + 24)) / meta.volumeScale

    // Read trade counts
    const cbuy = buffer.readUInt32LE(offset + 32)
    const csell = buffer.readUInt32LE(offset + 36)

    // Read and descale liquidation volumes
    const lbuy = Number(buffer.readBigInt64LE(offset + 40)) / meta.volumeScale
    const lsell = Number(buffer.readBigInt64LE(offset + 48)) / meta.volumeScale

    return { open, high, low, close, vbuy, vsell, cbuy, csell, lbuy, lsell }
  }

  /**
   * Reads metadata JSON file for a market/timeframe.
   * 
   * @param {string} jsonPath - Path to the JSON metadata file
   * @returns {BinaryMeta|null} Parsed metadata or null if not found
   */
  readMeta(jsonPath) {
    try {
      if (!fs.existsSync(jsonPath)) return null
      const content = fs.readFileSync(jsonPath, 'utf8')
      return JSON.parse(content)
    } catch (_e) {
      return null
    }
  }

  /**
   * Writes metadata JSON file atomically (write to temp, then rename).
   * 
   * @param {string} jsonPath - Path to the JSON metadata file
   * @param {BinaryMeta} meta - Metadata to write
   */
  writeMeta(jsonPath, meta) {
    const tmpPath = jsonPath + '.tmp'
    fs.writeFileSync(tmpPath, JSON.stringify(meta, null, 2))
    fs.renameSync(tmpPath, jsonPath)
  }

  /**
   * Fetches historical OHLCV data for one or more markets.
   * 
   * Returns data in a columnar format suitable for charting.
   * Null bars (gaps) are skipped in the response.
   * Pending bars near "now" are injected to provide real-time data.
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

    // Read data from disk for each market
    for (const market of markets) {
      const parsed = this.parseMarket(market)
      if (!parsed) continue

      const paths = this.getFilePath(parsed.exchange, parsed.symbol, timeframe)
      const meta = this.readMeta(paths.json)
      if (!meta) continue

      if (!fs.existsSync(paths.bin)) continue

      // Calculate index range for requested time window
      let startIndex = Math.floor((from - meta.startTs) / meta.timeframeMs)
      let endIndex = Math.ceil((to - meta.startTs) / meta.timeframeMs)

      // Clamp to valid range
      if (startIndex < 0) startIndex = 0
      if (endIndex > meta.records) endIndex = meta.records
      if (startIndex >= endIndex) continue

      const count = endIndex - startIndex
      const byteOffset = startIndex * RECORD_SIZE
      const byteLength = count * RECORD_SIZE

      // Read the range from disk
      let buffer
      let fd
      try {
        fd = fs.openSync(paths.bin, 'r')
        const stat = fs.fstatSync(fd)
        const maxBytes = Math.min(byteLength, stat.size - byteOffset)
        if (maxBytes <= 0) {
          fs.closeSync(fd)
          continue
        }
        buffer = Buffer.alloc(maxBytes)
        fs.readSync(fd, buffer, 0, maxBytes, byteOffset)
        fs.closeSync(fd)
      } catch (_e) {
        if (fd !== undefined) {
          try { fs.closeSync(fd) } catch (_) { /* ignored */ }
        }
        continue
      }

      // Decode records and add to results
      const recordCount = Math.floor(buffer.length / RECORD_SIZE)
      for (let i = 0; i < recordCount; i++) {
        const record = this.readRecord(buffer, i * RECORD_SIZE, meta)

        // Skip null bars (gaps) - check before computing barTime for efficiency
        if (record.open === 0 && record.high === 0 && record.low === 0 && record.close === 0) continue

        const barTime = meta.startTs + (startIndex + i) * meta.timeframeMs

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
   * @returns {Object[]} Array of pending bar objects with market field
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
