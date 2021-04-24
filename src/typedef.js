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
 * A range
 * @typedef  TimeRange
 * @type {{
  from: number,
  to: number
 }}
*/
