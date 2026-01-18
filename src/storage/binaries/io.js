const fs = require('fs')
const path = require('path')
const config = require('../../config')
const { getHms } = require('../../helper')
const { RECORD_SIZE, getSegmentStartTs, getSegmentSpanMs, getSegmentRecords } = require('./constants')

/**
 * Generates file paths for a market's binary and metadata files (legacy single-file format).
 * 
 * @deprecated Use getSegmentPaths for segmented storage
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol (will be sanitized)
 * @param {number} timeframe - Timeframe in milliseconds
 * @returns {import('./constants').FilePaths} Object with bin and json file paths
 */
function getFilePath(exchange, symbol, timeframe) {
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
 * Generates file paths for a segment's binary and metadata files.
 * 
 * New directory structure:
 *   data/{exchange}/{symbol}/{timeframe}/{segmentStartTs}.bin|json
 * 
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol (will be sanitized)
 * @param {number} timeframe - Timeframe in milliseconds
 * @param {number} segmentStartTs - Segment start timestamp
 * @returns {import('./constants').FilePaths} Object with bin and json file paths
 */
function getSegmentPaths(exchange, symbol, timeframe, segmentStartTs) {
  const sanitizedSymbol = symbol.replace(/[/:]/g, '-')
  const tfLabel = getHms(timeframe)
  const segmentId = String(segmentStartTs)
  const dir = path.join(config.filesLocation, exchange, sanitizedSymbol, tfLabel)
  return {
    bin: path.join(dir, `${segmentId}.bin`),
    json: path.join(dir, `${segmentId}.json`)
  }
}

/**
 * Gets the timeframe directory path (contains all segments for a timeframe).
 * 
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol (will be sanitized)
 * @param {number} timeframe - Timeframe in milliseconds
 * @returns {string} Directory path
 */
function getTimeframeDir(exchange, symbol, timeframe) {
  const sanitizedSymbol = symbol.replace(/[/:]/g, '-')
  const tfLabel = getHms(timeframe)
  return path.join(config.filesLocation, exchange, sanitizedSymbol, tfLabel)
}

/**
 * Parses a market string into exchange and symbol components.
 * 
 * @param {string} market - Market identifier (e.g., "COINBASE:BTC-USD")
 * @returns {import('./constants').ParsedMarket|null} Parsed components or null if invalid format
 */
function parseMarket(market) {
  const match = market.match(/([^:]*):(.*)/)
  if (!match) return null
  return { exchange: match[1], symbol: match[2] }
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
 * @param {import('./constants').Bar} bar - Bar data to write
 * @param {import('./constants').BinaryMeta} meta - Metadata with scale factors
 */
function writeRecord(buffer, offset, bar, meta) {
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
 * @param {import('./constants').BinaryMeta} meta - Metadata with scale factors
 * @returns {import('./constants').RecordData} Decoded bar data
 */
function readRecord(buffer, offset, meta) {
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
 * @returns {import('./constants').BinaryMeta|null} Parsed metadata or null if not found
 */
function readMeta(jsonPath) {
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
 * @param {import('./constants').BinaryMeta} meta - Metadata to write
 */
function writeMeta(jsonPath, meta) {
  const tmpPath = jsonPath + '.tmp'
  fs.writeFileSync(tmpPath, JSON.stringify(meta, null, 2))
  fs.renameSync(tmpPath, jsonPath)
}

/**
 * Reads a single record from a segment file at a specific bucket timestamp.
 * Uses positioned read (pread) to read only the needed 56 bytes.
 * 
 * This is used as a disk fallback when processing trades for buckets
 * that exist on disk but not in memory caches.
 * 
 * @param {string} exchange - Exchange name
 * @param {string} symbol - Trading pair symbol
 * @param {number} timeframe - Timeframe in milliseconds
 * @param {number} bucketTs - Bucket timestamp to read
 * @returns {import('./constants').Bar|null} Bar data if found and non-null, otherwise null
 */
function readSingleRecordAtTs(exchange, symbol, timeframe, bucketTs) {
  // Compute which segment this bucket belongs to
  const segmentStartTs = getSegmentStartTs(bucketTs, timeframe)
  const paths = getSegmentPaths(exchange, symbol, timeframe, segmentStartTs)
  const meta = readMeta(paths.json)
  
  if (!meta) return null
  if (!fs.existsSync(paths.bin)) return null

  // Compute the segment span and validate bucket is within segment bounds
  const segmentSpanMs = getSegmentSpanMs(timeframe)
  const segmentEndTs = segmentStartTs + segmentSpanMs
  
  if (bucketTs < segmentStartTs || bucketTs >= segmentEndTs) return null

  // Calculate record index within the segment
  const index = Math.floor((bucketTs - segmentStartTs) / timeframe)
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

  const record = readRecord(buffer, 0, meta)

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
 * Read segment metadata JSON file.
 * 
 * @param {string} jsonPath - Path to the JSON metadata file
 * @returns {import('./constants').SegmentMeta|null} Parsed metadata or null if not found
 */
function readSegmentMeta(jsonPath) {
  return readMeta(jsonPath)
}

/**
 * Writes segment metadata JSON file atomically (write to temp, then rename).
 * 
 * @param {string} jsonPath - Path to the JSON metadata file
 * @param {import('./constants').SegmentMeta} meta - Metadata to write
 */
function writeSegmentMeta(jsonPath, meta) {
  writeMeta(jsonPath, meta)
}

module.exports = {
  getFilePath,
  getSegmentPaths,
  getTimeframeDir,
  parseMarket,
  writeRecord,
  readRecord,
  readMeta,
  writeMeta,
  readSingleRecordAtTs,
  readSegmentMeta,
  writeSegmentMeta
}
