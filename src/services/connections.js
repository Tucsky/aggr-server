const config = require('../config')
const { parseMarket } = require('./catalog')

require('../typedef')

// only for debug purposes
module.exports.debugReportedTrades = {
  btcusdt: false
}

/**
 * @type {{[id: string]: Connection}}
 */
const connections = module.exports.connections = {}

/**
 * @type {ProductIndex[]}
 */
const indexes = module.exports.indexes = Object.values(config.pairs.reduce((acc, market) => {
  const product = parseMarket(market)
  console.log(`[index] registered product ${product.id} aka ${product.exchange} ${product.local} (${product.type})`)

  if (acc[product.local]) {
    acc[product.local].markets.push(market)
  } else {
    acc[product.local] = {
      markets: [market],
      id: product.local
    }
  }

  return acc
}, {}))

/**
 * Register or update a connection
 * @param {string} id 
 * @param {string} exchange 
 * @param {string} pair 
 * @param {number} apiLength total number of pairs connected to the same api
 * @returns {Connection} connection created or updated
 */
module.exports.registerConnection = function(id, exchange, pair, apiLength) {
  const now = Date.now()

  const exists = connections[id]

  console.log(`[connections] registered ${exists ? 'connection' : 'new connection'} ${id} (${apiLength})`)

  if (!exists) {

    connections[id] = {
      exchange,
      pair,
      hit: 0,
      start: now,
    }
  }

  connections[id].timestamp = now

  return connections[id]
}

module.exports.updateIndexes = function(callback) {
  for (const index of indexes) {
    let high = 0
    let low = 0
    let nbSources = 0
  
    for (const market of index.markets) {
      if (!connections[market] || !isFinite(connections[market].high)) {
        continue
      }
  
      high += connections[market].high
      low += connections[market].low
  
      nbSources++
    }

    if (!nbSources) {
      continue
    }

    callback(index.id, high / nbSources, low / nbSources)
  }
}

module.exports.getIndex = function(market) {
  for (const index of indexes) {
    if (index.id === market || index.markets.indexOf(market) !== -1) {
      return index
    }
  }
}