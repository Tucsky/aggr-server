/**
 * MEXC Futures-specific message handling
 * Handles JSON WebSocket messages for perpetual/futures contracts
 * Endpoint: wss://contract.mexc.com/edge
 */

/**
 * Handle futures JSON message
 * @param {Object} json - Parsed JSON message
 * @param {Object} contractSizes - Map of pair to contract size
 * @param {Object} inversed - Map of pair to inverse flag
 * @returns {Array|boolean} Formatted trades array, true if handled, or falsy
 */
function handleFuturesMessage(json, contractSizes, inversed) {
  // Futures trade messages
  if (json.channel === 'push.deal' && json.data && Array.isArray(json.data)) {
    return json.data.map(trade =>
      formatFuturesTrade(trade, json.symbol, contractSizes, inversed)
    )
  }

  // Handle futures subscription responses and pong
  if (json.channel === 'rs.sub.deal' || json.channel === 'pong') {
    return true
  }

  return false
}

/**
 * Format a single futures trade
 * @param {Object} trade - Raw trade data from WebSocket
 * @param {string} pair - Trading pair
 * @param {Object} contractSizes - Map of pair to contract size
 * @param {Object} inversed - Map of pair to inverse flag
 * @returns {Object} Formatted trade object
 */
function formatFuturesTrade(trade, pair, contractSizes, inversed) {
  // MEXC Futures: trade.v is number of contracts
  const contractSize = contractSizes[pair] || 1
  const isInverse = inversed[pair]

  // For linear (USDT): v * contractSize = base amount (BTC)
  // For inverse (USD): v * contractSize = USD value, divide by price for base
  const size = isInverse
    ? (+trade.v * contractSize) / +trade.p
    : +trade.v * contractSize

  return {
    exchange: 'MEXC',
    pair: pair,
    timestamp: trade.t,
    price: +trade.p,
    size: size,
    side: trade.T === 1 ? 'buy' : 'sell'
  }
}

module.exports = {
  handleFuturesMessage
}
