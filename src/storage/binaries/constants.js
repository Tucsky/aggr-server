const config = require('../../config')

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
 * Default number of records per segment file.
 * Each segment stores exactly this many records (dense indexing).
 * segmentSpanMs = timeframeMs * SEGMENT_RECORDS
 * 
 * Can be overridden via config.binariesSegmentRecords (number or per-timeframe object)
 * @constant {number}
 */
const DEFAULT_SEGMENT_RECORDS = 4096

/**
 * Get segment records count for a specific timeframe.
 * Supports both single value and per-timeframe configuration.
 * 
 * @param {number} timeframeMs - Timeframe in milliseconds
 * @returns {number} Number of records per segment
 */
function getSegmentRecords(timeframeMs) {
  const configValue = config.binariesSegmentRecords
  if (!configValue) return DEFAULT_SEGMENT_RECORDS
  if (typeof configValue === 'number') return configValue
  if (typeof configValue === 'object') {
    // Check for timeframe-specific override (keys can be ms or human-readable)
    const msKey = String(timeframeMs)
    if (configValue[msKey]) return configValue[msKey]
    // Try human-readable key lookup (e.g., "10s", "1m")
    const { getHms } = require('../../helper')
    const hmsKey = getHms(timeframeMs)
    if (configValue[hmsKey]) return configValue[hmsKey]
    return DEFAULT_SEGMENT_RECORDS
  }
  return DEFAULT_SEGMENT_RECORDS
}

/**
 * Compute the segment span in milliseconds for a given timeframe.
 * 
 * @param {number} timeframeMs - Timeframe in milliseconds
 * @returns {number} Segment span in milliseconds
 */
function getSegmentSpanMs(timeframeMs) {
  return timeframeMs * getSegmentRecords(timeframeMs)
}

/**
 * Compute the segment start timestamp for a given bar timestamp.
 * 
 * @param {number} ts - Bar timestamp in milliseconds
 * @param {number} timeframeMs - Timeframe in milliseconds
 * @returns {number} Segment start timestamp
 */
function getSegmentStartTs(ts, timeframeMs) {
  const segmentSpanMs = getSegmentSpanMs(timeframeMs)
  return Math.floor(ts / segmentSpanMs) * segmentSpanMs
}

/**
 * Compute the segment ID (string) for a given bar timestamp.
 * The segment ID is the segment start timestamp as a string.
 * 
 * @param {number} ts - Bar timestamp in milliseconds
 * @param {number} timeframeMs - Timeframe in milliseconds
 * @returns {string} Segment ID
 */
function getSegmentId(ts, timeframeMs) {
  return String(getSegmentStartTs(ts, timeframeMs))
}

/**
 * @typedef {Object} SegmentMeta
 * @property {string} exchange - Exchange name
 * @property {string} symbol - Trading pair symbol
 * @property {string} timeframe - Human-readable timeframe label (e.g., "5s", "1m")
 * @property {number} timeframeMs - Timeframe in milliseconds
 * @property {number} segmentStartTs - Segment start timestamp (first record time)
 * @property {number} segmentEndTs - Segment end timestamp (segmentStartTs + segmentSpanMs)
 * @property {number} segmentSpanMs - Total time span of segment in ms
 * @property {number} segmentRecords - Fixed number of records in this segment
 * @property {number} priceScale - Price scaling factor used
 * @property {number} volumeScale - Volume scaling factor used
 * @property {number} records - Current number of records written (may be less than segmentRecords)
 * @property {number} lastInputStartTs - Last input bar timestamp processed
 */

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
 * @deprecated Use SegmentMeta for segmented storage
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
 * @typedef {Object} Bar
 * @property {number} time - Bucket timestamp
 * @property {string} [market] - Market identifier
 * @property {number|null} open - Open price
 * @property {number|null} high - High price
 * @property {number|null} low - Low price
 * @property {number|null} close - Close price
 * @property {number} vbuy - Buy volume
 * @property {number} vsell - Sell volume
 * @property {number} cbuy - Buy trade count
 * @property {number} csell - Sell trade count
 * @property {number} lbuy - Buy liquidation volume
 * @property {number} lsell - Sell liquidation volume
 */

module.exports = {
  RECORD_SIZE,
  PRICE_SCALE,
  VOLUME_SCALE,
  DEFAULT_SEGMENT_RECORDS,
  getSegmentRecords,
  getSegmentSpanMs,
  getSegmentStartTs,
  getSegmentId
}
