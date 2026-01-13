const fs = require('fs')
const { ensureDirectoryExists, getHms } = require('../../helper')
const { RECORD_SIZE, PRICE_SCALE, VOLUME_SCALE } = require('./constants')
const { getFilePath, readRecord, writeRecord, readMeta, writeMeta } = require('./io')
const { createNullBar } = require('./write')

/**
 * Triggers resampling from base timeframe to all configured higher timeframes.
 * 
 * @param {Object} storage - BinariesStorage instance
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol
 * @param {number} fromTs - Start of dirty range (earliest modified bucket)
 * @param {number} toTs - End of dirty range (latest modified bucket)
 * @returns {Promise<void>}
 */
async function resampleToHigherTimeframes(storage, exchange, symbol, fromTs, toTs) {
  const baseTimeframe = storage.baseTimeframe
  const higherTimeframes = storage.timeframes.filter(tf => tf > baseTimeframe)

  if (higherTimeframes.length === 0) return

  const basePaths = getFilePath(exchange, symbol, baseTimeframe)
  const baseMeta = readMeta(basePaths.json)
  if (!baseMeta || !fs.existsSync(basePaths.bin)) return

  // Resample each higher timeframe
  for (const targetTimeframe of higherTimeframes) {
    await resampleRangeToTimeframe(exchange, symbol, baseMeta, basePaths.bin, fromTs, toTs, targetTimeframe)
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
 * @param {import('./constants').BinaryMeta} baseMeta - Metadata for base timeframe file
 * @param {string} baseBinPath - Path to base timeframe binary file
 * @param {number} fromTs - Start of dirty range (base bucket time)
 * @param {number} toTs - End of dirty range (base bucket time)
 * @param {number} targetTimeframe - Target timeframe in milliseconds
 * @returns {Promise<void>}
 */
async function resampleRangeToTimeframe(exchange, symbol, baseMeta, baseBinPath, fromTs, toTs, targetTimeframe) {
  const baseTimeframe = baseMeta.timeframeMs
  const paths = getFilePath(exchange, symbol, targetTimeframe)
  await ensureDirectoryExists(paths.bin)

  let meta = readMeta(paths.json)

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
        const record = readRecord(buffer, i * RECORD_SIZE, baseMeta)
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
      barsToUpsert.push(createNullBar(bucketTs))
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
          writeRecord(buffer, bufferOffset, overwrites[i].bar, meta)
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
            batchRecords.push(createNullBar(meta.startTs + currentIndex * targetTimeframe))
          }
          currentIndex++
        }

        if (batchRecords.length > 0) {
          const buffer = Buffer.alloc(batchRecords.length * RECORD_SIZE)
          let offset = 0

          for (const bar of batchRecords) {
            writeRecord(buffer, offset, bar, meta)
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
  writeMeta(paths.json, meta)
}

module.exports = {
  resampleToHigherTimeframes,
  resampleRangeToTimeframe
}
