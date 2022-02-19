/**
 * A trade
 * @typedef  Trade
 * @type {{exchange: string, pair: string, timestamp: number, price: number, size: number, side: number, liquidation: boolean?}}
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
