const fs = require('fs')
const { ensureDirectoryExists, getHms } = require('../../helper')
const { RECORD_SIZE, PRICE_SCALE, VOLUME_SCALE, getSegmentStartTs, getSegmentSpanMs, getSegmentRecords } = require('./constants')
const { getFilePath, getSegmentPaths, writeRecord, readMeta, writeMeta } = require('./io')

/**
 * Creates an empty (null) bar for gap filling.
 * Null bars have all OHLC set to null and all volumes/counts set to 0.
 * On disk, these are stored as all zeros and skipped when reading.
 * 
 * @param {number} time - Bucket timestamp
 * @returns {import('./constants').Bar} Null bar object
 */
function createNullBar(time) {
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
 * Appends bars to a binary file (append-only, no overwrites).
 * 
 * @deprecated Use upsertBars() instead for backfill support
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol
 * @param {number} timeframe - Timeframe in milliseconds
 * @param {import('./constants').Bar[]} bars - Array of bars to append
 * @returns {Promise<import('./constants').WriteResult>} Timestamps of written data range
 */
async function appendBars(exchange, symbol, timeframe, bars) {
  const paths = getFilePath(exchange, symbol, timeframe)
  await ensureDirectoryExists(paths.bin)

  let meta = readMeta(paths.json)

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
          batchRecords.push(createNullBar(currentTs))
        }
        currentTs += timeframe
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

  // Update metadata
  meta.endTs = meta.startTs + meta.records * timeframe
  meta.lastInputStartTs = lastBarTime

  writeMeta(paths.json, meta)

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
 * Now uses segmented storage: bars are split by segment and written to individual segment files.
 * 
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol
 * @param {number} timeframe - Timeframe in milliseconds
 * @param {import('./constants').Bar[]} bars - Array of bars to upsert
 * @returns {Promise<import('./constants').WriteResult>} Timestamps of written data range (includes overwrites)
 */
async function upsertBars(exchange, symbol, timeframe, bars) {
  // Filter out bars with null close (empty bars)
  const validBars = bars.filter(b => b.close !== null)
  if (validBars.length === 0) return { fromTsWritten: null, toTsWritten: null }

  // Group bars by segment
  const barsBySegment = {}
  for (const bar of validBars) {
    const alignedTime = Math.floor(bar.time / timeframe) * timeframe
    const segmentStartTs = getSegmentStartTs(alignedTime, timeframe)
    if (!barsBySegment[segmentStartTs]) {
      barsBySegment[segmentStartTs] = []
    }
    barsBySegment[segmentStartTs].push({ ...bar, time: alignedTime })
  }

  // Track the overall dirty range
  let minTs = null
  let maxTs = null

  // Upsert each segment
  const segmentStartTimestamps = Object.keys(barsBySegment).map(Number).sort((a, b) => a - b)
  for (const segmentStartTs of segmentStartTimestamps) {
    const segmentBars = barsBySegment[segmentStartTs]
    const result = await upsertBarsToSegment(exchange, symbol, timeframe, segmentStartTs, segmentBars)
    
    if (result.fromTsWritten !== null) {
      if (minTs === null || result.fromTsWritten < minTs) minTs = result.fromTsWritten
      if (maxTs === null || result.toTsWritten > maxTs) maxTs = result.toTsWritten
    }
  }

  return { fromTsWritten: minTs, toTsWritten: maxTs }
}

/**
 * Upserts bars to a specific segment file.
 * 
 * Segment characteristics:
 * - Fixed number of records (segmentRecords)
 * - Dense indexing: index = (barTs - segmentStartTs) / timeframe
 * - Index must be in range [0, segmentRecords)
 * 
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol
 * @param {number} timeframe - Timeframe in milliseconds
 * @param {number} segmentStartTs - Segment start timestamp
 * @param {import('./constants').Bar[]} bars - Array of bars to upsert (all must belong to this segment)
 * @returns {Promise<import('./constants').WriteResult>} Timestamps of written data range
 */
async function upsertBarsToSegment(exchange, symbol, timeframe, segmentStartTs, bars) {
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

  // Build map of index to bar, filtering out-of-range bars
  const barsMap = {}
  for (const bar of bars) {
    const alignedTime = Math.floor(bar.time / timeframe) * timeframe
    // Validate bar is within this segment
    if (alignedTime < segmentStartTs || alignedTime >= segmentEndTs) continue
    const index = Math.floor((alignedTime - segmentStartTs) / timeframe)
    if (index < 0 || index >= segmentRecords) continue
    barsMap[index] = { ...bar, time: alignedTime }
  }

  const indices = Object.keys(barsMap).map(Number).sort((a, b) => a - b)
  if (indices.length === 0) return { fromTsWritten: null, toTsWritten: null }

  // Track dirty range
  const minTs = segmentStartTs + indices[0] * timeframe
  const maxTs = segmentStartTs + indices[indices.length - 1] * timeframe

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

  // Handle overwrites (positioned writes to existing records)
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

  // Handle appends (extend file with new records, filling gaps)
  if (appends.length > 0) {
    appends.sort((a, b) => a.index - b.index)
    const lastAppendIndex = appends[appends.length - 1].index

    // Cannot append beyond segment boundary
    const maxAppendIndex = Math.min(lastAppendIndex, segmentRecords - 1)

    const appendMap = {}
    for (const a of appends) {
      if (a.index <= maxAppendIndex) {
        appendMap[a.index] = a.bar
      }
    }

    const BATCH_SIZE = 10000
    let currentIndex = meta.records
    let fd = null

    try {
      fd = fs.openSync(paths.bin, 'a')

      while (currentIndex <= maxAppendIndex) {
        const batchRecords = []
        const batchEndIndex = Math.min(currentIndex + BATCH_SIZE, maxAppendIndex + 1)

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

  meta.lastInputStartTs = maxTs
  writeMeta(paths.json, meta)

  return { fromTsWritten: minTs, toTsWritten: maxTs }
}

module.exports = {
  createNullBar,
  appendBars,
  upsertBars,
  upsertBarsToSegment
}
