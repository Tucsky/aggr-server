/**
 * Represents a trade.
 * @typedef {Object} Trade
 * @property {string} exchange       - The exchange where the trade occurred.
 * @property {string} pair           - The trading pair involved in the trade.
 * @property {number} timestamp      - The timestamp of the trade.
 * @property {number} price          - The price at which the trade occurred.
 * @property {number} size           - The size or quantity of the trade.
 * @property {number} side           - The side of the trade (1 for buy, 2 for sell, for example).
 * @property {boolean} [liquidation] - Optional. Indicates whether the trade was a liquidation.
 */

/**
 * A bar
 * @typedef  Bar
 * @type {{
  time: number,
  exchange: string,
  pair: string,
  cbuy: number,
  csell: number,
  vbuy: number,
  vsell: number,
  lbuy: number,
  lsell: number,
  open: number,
  high: number,
  low: number,
  close: number
 }}
*/

/**
 * A ohlc
 * @typedef  OHLC
 * @type {{
  open: number,
  high: number,
  low: number,
  close: number
 }}
*/

/**
 * A range
 * @typedef  TimeRange
 * @type {{
  from: number,
  to: number
 }}
*/

/**
 * Registered connection
 * Keep track of a single connection (exchange + pair)
 * @typedef  Connection
 * @type {{
 *  exchange: string,
 *  pair: string,
 *  apiId: string,
 *  hit: number,
 *  ping: number,
 *  bar?: Bar
 * }}
 */

/**
 * Registered product
 * Result of manual parsing of exchange + pair combinaison
 * @typedef  Product
 * @type {{
 *  id: string,
 *  exchange: string,
 *  pair: string,
 *  local: string,
 *  type: string,
 *  base: string,
 *  quote: string,
 * }}
 */

/**
 * Index resulting of multiple products
 * @typedef ProductIndex
 * @type {{
 *   id: string,
 *   markets: string[],
 *   ohlc?: OHLC,
 * }}
 */
