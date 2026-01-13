const fs = require('fs')
const { ensureDirectoryExists, getHms } = require('../../helper')
const { RECORD_SIZE, PRICE_SCALE, VOLUME_SCALE } = require('./constants')
const { getFilePath, writeRecord, readMeta, writeMeta } = require('./io')

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
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol
 * @param {number} timeframe - Timeframe in milliseconds
 * @param {import('./constants').Bar[]} bars - Array of bars to upsert
 * @returns {Promise<import('./constants').WriteResult>} Timestamps of written data range (includes overwrites)
 */
async function upsertBars(exchange, symbol, timeframe, bars) {
  const paths = getFilePath(exchange, symbol, timeframe)
  await ensureDirectoryExists(paths.bin)

  let meta = readMeta(paths.json)

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

      // Write records from current end to last append index (dense, filling gaps)
      while (currentIndex <= lastAppendIndex) {
        const batchRecords = []
        const batchEndIndex = Math.min(currentIndex + BATCH_SIZE, lastAppendIndex + 1)

        while (currentIndex < batchEndIndex) {
          if (appendMap[currentIndex]) {
            batchRecords.push(appendMap[currentIndex])
          } else {
            // Fill gap with null bar for dense indexing
            batchRecords.push(createNullBar(meta.startTs + currentIndex * timeframe))
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
    meta.endTs = meta.startTs + meta.records * timeframe
  }

  meta.lastInputStartTs = maxTs
  writeMeta(paths.json, meta)

  // Return the full dirty range (includes overwrites, not just appends)
  return { fromTsWritten: minTs, toTsWritten: maxTs }
}

module.exports = {
  createNullBar,
  appendBars,
  upsertBars
}
