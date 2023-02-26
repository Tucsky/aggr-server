const config = require('../config')
const fs = require('fs')
const { parseMarket } = require('./catalog')
const { getHms, ensureDirectoryExists, formatAmount } = require('../helper')

require('../typedef')

let saveConnectionsTimeout = null

/**
 * @type {{[id: string]: Connection}}
 */
const connections = (module.exports.connections = {})

/**
 * @type {{[id: string]: Boolean}}
 */
module.exports.recovering = {}

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

/**
 * Read the connections file and return the content
 * @returns {Promise<{[id: string]: Connection}>}
 */
function getConnectionsPersistance() {
  const path = 'products/connections.json'

  return new Promise((resolve) => {
    fs.readFile(path, 'utf8', (err, data) => {
      if (err && err.code !== 'ENOENT') {
        console.error(`[connections] failed to persist connections timestamps to ${path}`, err)
      }
      
      let json = {}
      
      if (data) {
        try {
          json = JSON.parse(data)
        } catch (parseError) {
          console.error(`[connections] connection.json is corrupted`, parseError.message)
          json = {}
        }
      }

      resolve(json)
    })
  })
}

/**
 * Remove unessential properties from connection
 * Ensure essensital ones are valid
 *
 * @param {Connection} connection
 * @returns {Connection} clean connection
 */
function cleanConnection(connection) {
  let { exchange, pair, hit, startedAt, timestamp, restarts } = connection

  if (!exchange) {
    throw new Error(`unknown connection's exchange`)
  }

  if (!pair) {
    throw new Error(`unknown connection's pair`)
  }

  if (typeof hit === 'undefined') {
    hit = 0
  }

  if (typeof timestamp === 'undefined') {
    timestamp = null
  }

  if (typeof restarts === 'undefined') {
    restarts = 0
  }

  if (!startedAt) {
    startedAt = Date.now()
  }

  return {
    exchange,
    pair,
    hit,
    startedAt,
    timestamp,
    restarts,
  }
}

/**
 * Get dynamic threshold for that connection
 *
 * config.reconnectionThreshold now accepts simple formula
 * ex: `7200000 / Math.log(Math.exp(1) + connection.avg)`
 * for 0 hit/min = 2h
 * for 0.1 hit/min = 1h, 55m, 48s
 * for 1 hit/min = 1h, 31m, 22s
 * for 10 hit/min = 47m, 11s
 * for 100 hit/min = 25m, 54s
 * for 1000 hit/min = 17m, 21s
 *
 * Or just use fixed threshold (in ms) in config.reconnectionThreshold
 * ex: 7200000
 *
 * @param {Connection} connection
 * @returns {number} average hits per 1min
 */
function getConnectionThreshold(connection) {
  let threshold = config.reconnectionThreshold

  if (config.reconnectionThreshold && typeof config.reconnectionThreshold === 'string' && isNaN(config.reconnectionThreshold)) {
    threshold = new Function('connection', `'use strict'; return ${config.reconnectionThreshold}`)(connection)
  }

  if (!threshold || threshold < 0 || !isFinite(threshold)) {
    // invalid threshold : fallback to 1h
    return 3600000
  }

  return threshold
}

/**
 * Return the total hit count scaled to 1min
 * @param {Connection} connection
 * @returns {number} average hits per 1min
 */
function getConnectionHitAverage(connection) {
  return connection.hit * (60000 / (connection.timestamp - connection.startedAt))
}

/**
 * Return the total hit count scaled to 1min
 * @param {Connection} connection
 * @returns {number} average hits per 1min
 */
function getConnectionPing(connection) {
  return Math.max(connection.startedAt, connection.timestamp, connection.lastReconnection || connection.startedAt)
}

/**
 * Set the threshold, maximum ping and avg hit for the connection
 * @param {Connection} connection
 */
module.exports.updateConnectionStats = function (connection) {
  connection.avg = getConnectionHitAverage(connection)
  connection.thrs = getConnectionThreshold(connection)
  connection.ping = getConnectionPing(connection)
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
    if (config.pairs.indexOf(market) === -1 || config.exchanges.indexOf(persistance[market].exchange) === -1) {
      // filter out connection that doesn't concern this instance
      continue
    }

    if (
      !persistance[market].forceRecovery &&
      (!persistance[market].timestamp ||
        (config.staleConnectionThreshold > 0 && now - persistance[market].timestamp > config.staleConnectionThreshold))
    ) {
      console.log(
        `[connections] couldn't restore ${market}'s connection because ${
          persistance[market].timestamp
            ? `last ping is too old (${getHms(now - persistance[market].timestamp, true)} ago)`
            : `last ping is unknown`
        }`
      )
      // connection is to old (too much data to recover)
      continue
    }

    try {
      persistance[market] = cleanConnection(persistance[market])
    } catch (error) {
      console.error(
        `[connections] couldn't restore connection ${market} because persistance is missing some informations\n\t${error.message}`
      )
      continue
    }

    if (
      typeof persistance[market].startedAt !== 'number' ||
      typeof persistance[market].hit !== 'number' ||
      !persistance[market].exchange ||
      !persistance[market].pair
    ) {
      console.error(
        `[connections] couldn't restore connection ${market} because persistance is missing some informations\n\tmake sure the following properties are set for each connections : exchange, pair, startedAt and hit`
      )
      // connection is invalid
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
module.exports.saveConnections = async function (immediate = false) {
  if (!immediate) {
    if (saveConnectionsTimeout) {
      clearTimeout(saveConnectionsTimeout)
    }

    saveConnectionsTimeout = setTimeout(async () => {
      saveConnectionsTimeout = null
      if (await module.exports.saveConnections(true)) {
        console.log(`[connections] saved connections`)
      }
    }, 10000)

    return
  }

  if (!config.persistConnections) {
    return
  }

  const persistance = await getConnectionsPersistance()

  // only update connections in persistance which have been used in that instance
  for (const market in connections) {
    if (connections[market].timestamp) {
      persistance[market] = cleanConnection(connections[market])
    }
  }

  const path = 'products/connections.json'

  await ensureDirectoryExists(path)

  return new Promise((resolve) => {
    fs.writeFile(path, JSON.stringify(persistance), (err) => {
      if (err) {
        console.error(`[connections] failed to persist connections to ${path}`, err)
        resolve(false)
      }

      resolve(true)
    })
  })
}

/**
 * Register active connection (new or existing)
 *
 * @param {String} id
 * @param {String} exchange
 * @param {String} pair
 * @returns {Connection}
 */
module.exports.registerConnection = function (id, exchange, pair) {
  const now = Date.now()

  const exists = connections[id]

  if (!exists) {
    connections[id] = {
      exchange,
      pair,
      hit: 0,
      restarts: 0,
      startedAt: now,
      timestamp: null,
    }

    if (config.pairs.indexOf(id) === -1) {
      // force fetch last 4h of data through recent trades
      connections[id].forceRecovery = true
      connections[id].timestamp = now - 1000 * 60 * 60 * 4
    }
  } else {
    connections[id].restarts++
    
    if (connections[id].timestamp) {
      // calculate estimated missing trade since last trade processed on that connection
      const activeDuration = connections[id].timestamp - connections[id].startedAt
      const missDuration = now - connections[id].timestamp
      connections[id].lastConnectionMissEstimate = Math.floor(connections[id].hit * (missDuration / activeDuration))
    }

    connections[id].lastReconnection = now
  }

  module.exports.updateConnectionStats(connections[id])
  module.exports.saveConnections()

  return connections[id]
}

/**
 * Update high and low of range for an index based on their connections
 * @param {Function} callback function called for every index updated
 */
module.exports.updateIndexes = function (callback) {
  for (const index of indexes) {
    const open = index.price

    let high = 0
    let low = 0
    let close = 0
    let nbSources = 0

    for (const market of index.markets) {
      if (!connections[market] || !connections[market].apiId || !isFinite(connections[market].high)) {
        continue
      }

      high += connections[market].high
      low += connections[market].low
      close += connections[market].close

      nbSources++
    }

    if (!nbSources) {
      continue
    }

    index.price = close / nbSources

    callback(index.id, high / nbSources, low / nbSources, index.price > open ? 1 : -1)
  }
}

/**
 * Get index matching a given market
 * ex: BINANCE:btcusdt -> BTCUSD
 *
 * @param {String} market
 * @returns {ProductIndex}
 */
module.exports.getIndex = function (market) {
  for (const index of indexes) {
    if (index.id === market || index.markets.indexOf(market) !== -1) {
      return index
    }
  }
}

module.exports.dumpConnections = function (connections) {
  const now = Date.now()
  const table = {}

  for (const id in connections) {
    module.exports.updateConnectionStats(connections[id])

    const columns = {
      hit: formatAmount(connections[id].hit),
      'avg hit': `${formatAmount(Math.floor(connections[id].avg))}/min`,
      'last hit': getHms(now - connections[id].ping),
      thrs: getHms(connections[id].thrs),
      reco: connections[id].restarts,
    }

    if (!module.exports.recovering[connections[id].exchange] && now - connections[id].ping > connections[id].thrs) {
      columns.thrs + ' ⚠️ (RECONNECT)'
    } else if (module.exports.recovering[connections[id].exchange]) {
      columns.ping + ' ⏬ (RECOVERING)'
    }

    table[id] = columns
  }

  if (Object.keys(table).length) {
    console.table(table)
  }
}
