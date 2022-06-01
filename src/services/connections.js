const config = require('../config')
const { parseMarket } = require('./catalog')

require('../typedef')

/**
 * @type {{[id: string]: Connection}}
 */
const connections = module.exports.connections = {}

/**
 * @type {ProductIndex[]}
 */
const indexes = module.exports.indexes = [];

(module.exports.registerIndexes = function() {
  indexes.splice(0, indexes.length)

  const cacheIndexes = {}

  for (const market of config.pairs) {
    const product = parseMarket(market)
  
    if (config.priceIndexesBlacklist.indexOf(product.exchange) !== -1) {
      continue
    }

    if (!cacheIndexes[product.local]) {
      cacheIndexes[product.local] = {
        id: product.local,
        markets: []
      }
    }

    // console.log(`[index] registered product ${product.id} aka ${product.exchange} ${product.local} (${product.type})`)

    cacheIndexes[product.local].markets.push(market)
  }

  for (const localPair in cacheIndexes) {
    indexes.push(cacheIndexes[localPair])
  }
})()

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

  if (!exists) {

    connections[id] = {
      exchange,
      pair,
      hit: 0,
      startedAt: now,
      lastConnectedAt: now,
      timestamp: null
    }
  } else {
    if (connections[id].timestamp) {
      const activeDuration = connections[id].timestamp - connections[id].startedAt
      const missDuration = now - connections[id].timestamp
      connections[id].lastConnectionMissEstimate = Math.floor(connections[id].hit * (missDuration / activeDuration))
    }

    connections[id].lastConnectedAt = now
  }

  return connections[id]
}

module.exports.updateIndexes = function(callback) {
  for (const index of indexes) {
    let high = 0
    let low = 0
    let nbSources = 0
  
    for (const market of index.markets) {
      if (!connections[market] || !connections[market].apiId || !isFinite(connections[market].high)) {
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