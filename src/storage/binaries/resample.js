const fs = require('fs')
const { ensureDirectoryExists, getHms } = require('../../helper')
const { RECORD_SIZE, PRICE_SCALE, VOLUME_SCALE, getSegmentStartTs, getSegmentSpanMs, getSegmentRecords } = require('./constants')
const { getFilePath, getSegmentPaths, readRecord, writeRecord, readMeta, writeMeta } = require('./io')
const { createNullBar, upsertBarsToSegment } = require('./write')

/**
 * Triggers resampling from base timeframe to all configured higher timeframes.
 * Now reads from segmented base timeframe files and writes to segmented target files.
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

  // Resample each higher timeframe
  for (const targetTimeframe of higherTimeframes) {
    await resampleRangeToTimeframe(exchange, symbol, baseTimeframe, fromTs, toTs, targetTimeframe)
  }
}

/**
 * Resamples a range of base timeframe data into a higher target timeframe.
 * Now reads from segmented base files and writes to segmented target files.
 * 
 * Aggregation rules:
 * - open: first non-null value in bucket (by base bar time order)
 * - close: last non-null value in bucket
 * - high/low: extrema across all base bars
 * - volumes/counts: cumulative sums
 * 
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol
 * @param {number} baseTimeframe - Base timeframe in milliseconds
 * @param {number} fromTs - Start of dirty range (base bucket time)
 * @param {number} toTs - End of dirty range (base bucket time)
 * @param {number} targetTimeframe - Target timeframe in milliseconds
 * @returns {Promise<void>}
 */
async function resampleRangeToTimeframe(exchange, symbol, baseTimeframe, fromTs, toTs, targetTimeframe) {
  // Compute dirty target bucket range (inclusive)
  const firstTargetBucket = Math.floor(fromTs / targetTimeframe) * targetTimeframe
  const lastTargetBucket = Math.floor(toTs / targetTimeframe) * targetTimeframe

  // Determine which base segments need to be read
  // We need to read all base bars that contribute to [firstTargetBucket, lastTargetBucket + targetTimeframe)
  const baseReadFrom = firstTargetBucket
  const baseReadTo = lastTargetBucket + targetTimeframe - 1 // Last base bar we might need

  // Collect all base segments that intersect this range
  const baseSegmentSpanMs = getSegmentSpanMs(baseTimeframe)
  const firstBaseSegment = getSegmentStartTs(baseReadFrom, baseTimeframe)
  const lastBaseSegment = getSegmentStartTs(baseReadTo, baseTimeframe)

  // Read base bars from all relevant segments
  const baseBarsByTime = {} // ts -> record

  for (let segmentStartTs = firstBaseSegment; segmentStartTs <= lastBaseSegment; segmentStartTs += baseSegmentSpanMs) {
    const paths = getSegmentPaths(exchange, symbol, baseTimeframe, segmentStartTs)
    const meta = readMeta(paths.json)
    if (!meta || !fs.existsSync(paths.bin)) continue

    // Calculate which records from this segment we need
    const segmentEndTs = segmentStartTs + baseSegmentSpanMs
    const readFromTs = Math.max(baseReadFrom, segmentStartTs)
    const readToTs = Math.min(baseReadTo, segmentEndTs - 1)

    if (readFromTs > readToTs) continue

    const startIndex = Math.floor((readFromTs - segmentStartTs) / baseTimeframe)
    const endIndex = Math.min(
      Math.ceil((readToTs + 1 - segmentStartTs) / baseTimeframe),
      meta.records
    )

    if (startIndex >= endIndex) continue

    const count = endIndex - startIndex
    const byteOffset = startIndex * RECORD_SIZE
    const byteLength = count * RECORD_SIZE

    let buffer
    try {
      const fd = fs.openSync(paths.bin, 'r')
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
      for (let i = 0; i < recordCount; i++) {
        const record = readRecord(buffer, i * RECORD_SIZE, meta)
        const barTime = segmentStartTs + (startIndex + i) * baseTimeframe

        // Skip null records
        if (record.open === 0 && record.high === 0 && record.low === 0 && record.close === 0) continue

        baseBarsByTime[barTime] = record
      }
    }
  }

  // Aggregate base bars into target buckets
  const aggregated = {}

  for (const barTimeStr in baseBarsByTime) {
    const barTime = Number(barTimeStr)
    const record = baseBarsByTime[barTime]
    const targetBucketTime = Math.floor(barTime / targetTimeframe) * targetTimeframe

    if (targetBucketTime < firstTargetBucket || targetBucketTime > lastTargetBucket) continue

    if (!aggregated[targetBucketTime]) {
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
        _firstBarTime: barTime
      }
    } else {
      const agg = aggregated[targetBucketTime]
      if (barTime < agg._firstBarTime) {
        agg.open = record.open
        agg._firstBarTime = barTime
      }
      agg.high = Math.max(agg.high, record.high)
      agg.low = Math.min(agg.low, record.low)
      agg.close = record.close
      agg.vbuy += record.vbuy
      agg.vsell += record.vsell
      agg.cbuy += record.cbuy
      agg.csell += record.csell
      agg.lbuy += record.lbuy
      agg.lsell += record.lsell
    }
  }

  // Build list of bars to write (fill gaps with null bars)
  const barsToWrite = []
  for (let bucketTs = firstTargetBucket; bucketTs <= lastTargetBucket; bucketTs += targetTimeframe) {
    if (aggregated[bucketTs]) {
      const bar = aggregated[bucketTs]
      delete bar._firstBarTime // Remove internal tracking field
      barsToWrite.push(bar)
    } else {
      barsToWrite.push(createNullBar(bucketTs))
    }
  }

  if (barsToWrite.length === 0) return

  // Group bars by target segment and upsert each segment
  const targetSegmentSpanMs = getSegmentSpanMs(targetTimeframe)
  const barsBySegment = {}

  for (const bar of barsToWrite) {
    const segmentStartTs = getSegmentStartTs(bar.time, targetTimeframe)
    if (!barsBySegment[segmentStartTs]) {
      barsBySegment[segmentStartTs] = []
    }
    barsBySegment[segmentStartTs].push(bar)
  }

  // Upsert each target segment
  for (const segmentStartTsStr in barsBySegment) {
    const segmentStartTs = Number(segmentStartTsStr)
    const segmentBars = barsBySegment[segmentStartTs]
    
    // Filter out null-only bar sets (don't create segment files just for nulls)
    const hasNonNull = segmentBars.some(b => b.close !== null)
    
    // For resampling, we need to write even null bars to maintain consistency
    // But only if there's existing data or non-null bars
    const paths = getSegmentPaths(exchange, symbol, targetTimeframe, segmentStartTs)
    const existingMeta = readMeta(paths.json)
    
    if (hasNonNull || existingMeta) {
      await upsertBarsToSegmentForResample(exchange, symbol, targetTimeframe, segmentStartTs, segmentBars)
    }
  }
}

/**
 * Upserts bars to a segment for resampling purposes.
 * Unlike regular upsert, this handles null bars for dense indexing in resampled data.
 * 
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol
 * @param {number} timeframe - Timeframe in milliseconds
 * @param {number} segmentStartTs - Segment start timestamp
 * @param {import('./constants').Bar[]} bars - Array of bars (may include null bars)
 * @returns {Promise<void>}
 */
async function upsertBarsToSegmentForResample(exchange, symbol, timeframe, segmentStartTs, bars) {
  const paths = getSegmentPaths(exchange, symbol, timeframe, segmentStartTs)
  await ensureDirectoryExists(paths.bin)

  let meta = readMeta(paths.json)
  const segmentRecords = getSegmentRecords(timeframe)
  const segmentSpanMs = getSegmentSpanMs(timeframe)
  const segmentEndTs = segmentStartTs + segmentSpanMs

  // Initialize segment metadata if new file
  if (!meta) {
    meta = {
      exchange: exchange,
      symbol: symbol,
      timeframe: getHms(timeframe),
      timeframeMs: timeframe,
      segmentStartTs: segmentStartTs,
      segmentEndTs: segmentEndTs,
      segmentSpanMs: segmentSpanMs,
      segmentRecords: segmentRecords,
      priceScale: PRICE_SCALE,
      volumeScale: VOLUME_SCALE,
      records: 0,
      lastInputStartTs: segmentStartTs
    }
  }

  // Build map of index to bar
  const barsMap = {}
  let maxBarTime = segmentStartTs

  for (const bar of bars) {
    const alignedTime = Math.floor(bar.time / timeframe) * timeframe
    if (alignedTime < segmentStartTs || alignedTime >= segmentEndTs) continue
    const index = Math.floor((alignedTime - segmentStartTs) / timeframe)
    if (index < 0 || index >= segmentRecords) continue
    barsMap[index] = { ...bar, time: alignedTime }
    if (alignedTime > maxBarTime) maxBarTime = alignedTime
  }

  const indices = Object.keys(barsMap).map(Number).sort((a, b) => a - b)
  if (indices.length === 0) return

  // Categorize as overwrites or appends
  const overwrites = []
  const appends = []

  for (const index of indices) {
    if (index < meta.records) {
      overwrites.push({ index, bar: barsMap[index] })
    } else {
      appends.push({ index, bar: barsMap[index] })
    }
  }

  // Handle overwrites
  if (overwrites.length > 0) {
    let fd = null
    try {
      fd = fs.openSync(paths.bin, 'r+')
      overwrites.sort((a, b) => a.index - b.index)
      let groupStart = 0

      while (groupStart < overwrites.length) {
        let groupEnd = groupStart
        while (groupEnd + 1 < overwrites.length && 
               overwrites[groupEnd + 1].index === overwrites[groupEnd].index + 1) {
          groupEnd++
        }

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
      if (fd !== null) fs.closeSync(fd)
    }
  }

  // Handle appends
  if (appends.length > 0) {
    appends.sort((a, b) => a.index - b.index)
    const lastAppendIndex = Math.min(appends[appends.length - 1].index, segmentRecords - 1)

    const appendMap = {}
    for (const a of appends) {
      if (a.index <= lastAppendIndex) {
        appendMap[a.index] = a.bar
      }
    }

    const BATCH_SIZE = 10000
    let currentIndex = meta.records
    let fd = null

    try {
      fd = fs.openSync(paths.bin, 'a')

      while (currentIndex <= lastAppendIndex) {
        const batchRecords = []
        const batchEndIndex = Math.min(currentIndex + BATCH_SIZE, lastAppendIndex + 1)

        while (currentIndex < batchEndIndex) {
          if (appendMap[currentIndex]) {
            batchRecords.push(appendMap[currentIndex])
          } else {
            batchRecords.push(createNullBar(segmentStartTs + currentIndex * timeframe))
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
      if (fd !== null) fs.closeSync(fd)
    }
  }

  meta.lastInputStartTs = maxBarTime
  writeMeta(paths.json, meta)
}

module.exports = {
  resampleToHigherTimeframes,
  resampleRangeToTimeframe
}