const config = require('../config')
const fs = require('fs')
const { parseMarket } = require('./catalog')
const { getHms, ensureDirectoryExists } = require('../helper')

require('../typedef')

/**
 * @type {{[id: string]: Connection}}
 */
const connections = (module.exports.connections = {})

/**
 * @type {ProductIndex[]}
 */
const indexes = (module.exports.indexes = [])

;(module.exports.registerIndexes = function () {
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
        markets: [],
      }
    }

    cacheIndexes[product.local].markets.push(market)
  }

  for (const localPair in cacheIndexes) {
    indexes.push(cacheIndexes[localPair])
  }
})()

module.exports.registerConnection = function (id, exchange, pair) {
  const now = Date.now()

  const exists = connections[id]

  if (!exists) {
    connections[id] = {
      exchange,
      pair,
      hit: 0,
      startedAt: now,
      timestamp: null,
    }

    if (config.pairs.indexOf(id) === -1) {
      // new connection manually added through pm2 actions (not in config.pairs yet)
      connections[id].timestamp = now - 1000 * 60 * 60 * 4
      connections[id].lastConnectionMissEstimate = 100
    }
  } else {
    if (connections[id].timestamp) {
      // calculate estimated missing trade since last trade processed on that connection
      const activeDuration = connections[id].timestamp - connections[id].startedAt
      const missDuration = now - connections[id].timestamp
      connections[id].lastConnectionMissEstimate = Math.floor(connections[id].hit * (missDuration / activeDuration))
    }
  }

  return connections[id]
}

module.exports.updateIndexes = function (callback) {
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

module.exports.getIndex = function (market) {
  for (const index of indexes) {
    if (index.id === market || index.markets.indexOf(market) !== -1) {
      return index
    }
  }
}

function getConnectionsPersistance() {
  const path = 'products/connections.json'

  return new Promise((resolve) => {
    fs.readFile(path, 'utf8', (err, data) => {
      if (err && err.code !== 'ENOENT') {
        console.error(`[connections] failed to persist connections timestamps to ${path}`, err)
      }

      if (data) {
        resolve(JSON.parse(data))
      } else {
        resolve({})
      }
    })
  })
}

/**
 * Inject connection data (start time, last ping, total hit) from shared persistance into current instance
 */
module.exports.restoreConnections = async function () {
  if (!config.persistConnections) {
    return
  }

  const persistance = await getConnectionsPersistance()

  const pings = []
  const now = Date.now()

  for (const market in persistance) {
    if (config.pairs.indexOf(market) === -1) {
      // filter out connection that doesn't concern this instance
      continue
    }

    if (!persistance[market].timestamp || now - persistance[market].timestamp > 1000 * 60 * 60 * 4) {
      // connection is to old (too much data to recoved)
      continue
    }

    connections[market] = persistance[market]

    const ping = now - connections[market].timestamp

    console.log(`[connections] restored ${market}'s connection state (last trade was ${getHms(ping, true)} ago)`)

    pings.push(now - connections[market].timestamp)
  }

  if (pings.length) {
    console.log(
      `[connections] restored ${pings.length} connections (last ping ${getHms(
        pings.reduce((acc, ping) => acc + ping) / pings.length,
        true
      )} ago avg)`
    )
  }
}

/**
 * Save used connections into shared persistance
 */
module.exports.saveConnections = async function () {
  if (!config.persistConnections) {
    return
  }

  const persistance = await getConnectionsPersistance()

  // only update connections in persistance that are used in this instance (config.pair)
  for (const market in connections) {
    if (connections[market].timestamp) {
      persistance[market] = connections[market]
    }
  }

  // cleanup - only keep whats matter
  for (const market in persistance) {
    const { exchange, pair, hit, startedAt, timestamp } = persistance[market]

    persistance[market] = {
      exchange,
      pair,
      hit,
      startedAt,
      timestamp,
    }
  }

  const path = 'products/connections.json'

  await ensureDirectoryExists(path)

  return new Promise((resolve) => {
    fs.writeFile(path, JSON.stringify(persistance), (err) => {
      if (err) {
        console.error(`[connections] failed to persist connections to ${path}`, err)
      } else {
        console.log(`[connections] saved ${Object.keys(persistance).length} connections in persistance`)
      }

      resolve()
    })
  })
}
