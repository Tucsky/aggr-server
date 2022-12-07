const fs = require('fs')
const path = require('path')
const decamelize = require('decamelize')

console.log(`[init] reading config.json...`)

/* Default configuration (its not ok to change here!, use config.json.)
 */

const defaultConfig = {
  // default pairs we track
  pairs: [
    'BITFINEX:BTCUSD',
    'BINANCE:btcusdt',
    'OKEX:BTC-USDT',
    'KRAKEN:XBT/USD',
    'COINBASE:BTC-USD',
    'POLONIEX:BTC_USDT',
    'HUOBI:btcusdt',
    'BITSTAMP:btcusd',
    'BITMEX:XBTUSD',
    'BITFINEX:BTCF0:USTF0',
    'OKEX:BTC-USD-SWAP',
    'OKEX:BTC-USDT-SWAP',
    'BINANCE_FUTURES:btcusdt',
    'BINANCE_FUTURES:btcusd_perp',
    'HUOBI:BTC-USD',
    'KRAKEN:PI_XBTUSD',
    'DERIBIT:BTC-PERPETUAL',
    'FTX:BTC-PERP',
    'FTX:BTC/USD',
    'FTX:BTC/USDT',
    'BYBIT:BTCUSD',
    'BYBIT:BTCUSDT',
    'BYBIT:ETHUSD',
    'BYBIT:ETHUSDT',
    'BITSTAMP:ethusd',
    'BITMEX:ETHUSD',
    'KRAKEN:PI_ETHUSD',
    'BITFINEX:ETHUSD',
    'COINBASE:ETH-USD',
    'OKEX:ETH-USDT',
    'OKEX:ETH-USDT-SWAP',
    'OKEX:ETH-USD-SWAP',
    'BINANCE_FUTURES:ethusdt',
    'FTX:ETH-PERP',
    'FTX:ETH/USD',
    'FTX:ETH/USDT',
    'DERIBIT:ETH-PERPETUAL',
    'KRAKEN:ETH/USD',
    'HUOBI:ethusdt',
    'HUOBI:ETH-USD',
    'BINANCE_FUTURES:ethusd_perp',
  ],
  
  // non standard products
  extraProducts: [],

  // will connect to exchanges and subscribe to pairs on startup
  collect: true,

  // default server port
  port: 3000,

  // restrict origin (now using regex)
  origin: '.*',

  // max n of bars a user can get in 1 call
  maxFetchLength: 100000,

  // persist connections between reload, and use that information to attempt filling the holes on startup
  persistConnections: true,

  // won't persist connection older than this (default 4h)
  staleConnectionThreshold: 1000 * 60 * 60 * 4,

  // reconnection threshold (default 1h without incoming trade, reconnect the whole api)
  // config.reconnectionThreshold now accepts simple formula which can use the `connection` variable
  // ex: `7200000 / Math.log(Math.exp(1) + connection.avg)`
  // for 0 hit/min = 2h
  // for 0.1 hit/min = 1h, 55m, 48s
  // for 1 hit/min = 1h, 31m, 22s
  // for 10 hit/min = 47m, 11s
  // for 100 hit/min = 25m, 54s
  // for 1000 hit/min = 17m, 21s
  reconnectionThreshold: 3600000,

  // delay between each recovery requests of a same pair (default 250ms)
  recoveryRequestDelay: 250,

  // bypass origin restriction for given ips (comma separated)
  whitelist: [],

  // enable websocket server (if you only use this for storing trade data set to false)
  broadcast: false,

  // separate the broadcasts by n ms (0 = broadcast instantly)
  broadcastDebounce: 0,

  // aggregate trades that came within same millisecond before broadcast
  // (note) saving to storage is NOT impacted
  // (warning) will add +50ms delay for confirmation that trade actually came on same ms
  broadcastAggr: true,

  // will only broadcast trades >= broadcastThreshold
  // expressed in base currency (ex: BTC)
  // default 0
  broadcastThreshold: 0,

  // enable api (historical/{from in ms}/{to in ms}/{timesfame in ms}/{markets separated by +})
  api: true,

  // storage solution, either
  // false | null (no storage, everything is wiped out after broadcast)
  // "files" (periodical text file),
  // "influx" (timeserie database),

  // NB: use array or comma separated storage names for multiple storage solution
  // default = "files" just store in text files, no further installation required.
  storage: 'files',

  // store interval (in ms)
  backupInterval: 10000,

  // influx db server to use when storage is set to "influx"
  influxHost: 'localhost',
  influxPort: 8086,

  // influx database
  influxDatabase: 'significant_trades',

  // base name measurement used to store the bars
  // if influxMeasurement is "trades" and influxTimeframe is "10000", influx will save to trades_10s
  influxMeasurement: 'trades',

  // timeframe in ms (default 10s === 10000ms)
  // this is lowest timeframe that influx will use to group the trades
  influxTimeframe: 10000,

  // downsampling
  influxResampleTo: [
    1000 * 30,
    1000 * 60,
    1000 * 60 * 3,
    1000 * 60 * 5,
    1000 * 60 * 15,
    1000 * 60 * 30,
    1000 * 60 * 60,
    1000 * 60 * 60 * 2,
    1000 * 60 * 60 * 4,
    1000 * 60 * 60 * 6,
    1000 * 60 * 60 * 24,
  ],

  // trigger resample every minute
  influxResampleInterval: 60000,

  // number of bars to retain within influx db per timeframe
  influxRetentionPerTimeframe: 5000,

  // prefix aggr retention policies with this (unused rp using that prefix get automaticaly removed)
  influxRetentionPrefix: 'aggr_',

  // create new text file every N ms when storage is set to "file" (default 1h)
  filesInterval: 3600000,

  // default place to store the trades data files
  filesLocation: './data',

  // automatic compression of file once done working with it
  filesGzipAfterUse: true,

  // close file stream after it's expired, allow for a delay before closing the file stream 
  // which can be useful when exchanges trades are lagging (default 5m)
  filesCloseAfter: 300000,

  // choose whether or not enable rate limiting on the provided api
  enableRateLimit: false,

  // rate limit time window (default 15m)
  rateLimitTimeWindow: 1000 * 60 * 15,

  // rate limit max request per rateLimitTimeWindow (default 30)
  rateLimitMax: 30,

  // enable cluster mode where you have node dedicated to collecting and 1 master node dedicated to serving data
  // one client request the data, the main cluster node will ask collectors for realtime data that is not yet in saved in the DB
  // reducing writes rate on influx yet allowing realtime data fetch for the client
  // config.api: true === cluster node
  // config.collect: true === collector node
  influxCollectors: false,

  // collector to cluster reconnection delay (default 10s)
  influxCollectorsReconnectionDelay: 1000 * 10,

  // unix socket used to communicate
  influxCollectorsClusterSocketPath: '/tmp/aggr.sock',

  // you must set an ID to each cluster in order to use push notifications with collectors (unique id to associate with notification endpoints persistences)
  id: null,

  publicVapidKey: null,
  privateVapidKey: null,
  alertExpiresAfter: 1000 * 60 * 60 * 24 * 7,
  alertEndpointExpiresAfter: 1000 * 60 * 60 * 24 * 30,
  priceIndexesBlacklist: [],

  // verbose
  debug: false,
}

/* Merge default
 */

const commandSettings = {}

if (process.argv.length > 2) {
  let exchanges = []
  process.argv.slice(2).forEach((arg) => {
    const keyvalue = arg.split('=')

    if (keyvalue.length > 1) {
      commandSettings[keyvalue[0]] = isNaN(+keyvalue[1]) ? keyvalue[1] : +keyvalue[1]
    } else if (/^\w+$/.test(keyvalue[0])) {
      exchanges.push(keyvalue[0])
    }
  })

  if (exchanges.length) {
    commandSettings.exchanges = exchanges
  }
}

/* Load custom server configuration
 */

let userSettings = {}

const specificConfigFile = commandSettings.config
  ? commandSettings.config
  : commandSettings.configFile
  ? commandSettings.configFile
  : commandSettings.configPath
  ? commandSettings.configPath
  : null

let configPath = specificConfigFile || 'config.json'

try {
  console.log('[init] using config file ' + configPath)
  configPath = path.resolve(__dirname, '../' + configPath)
  const configExamplePath = path.resolve(__dirname, '../config.json.example')

  if (!fs.existsSync(configPath) && fs.existsSync(configExamplePath) && !specificConfigFile) {
    fs.copyFileSync(configExamplePath, configPath)
  }

  commandSettings.configPath = configPath

  userSettings = require(configPath) || {}
} catch (error) {
  throw new Error(`Unable to parse ${configPath !== '../config.json' ? 'specified ' : ''}configuration file\n\n${error.message}`)
}

/* Merge cmd & file configuration
 */

const config = Object.assign(defaultConfig, userSettings, commandSettings)

/* Override config with ENV variables using decamelize + uppercase 
  (e.g. influxPreheatRange -> INFLUX_PREHEAT_RANGE)
 */

Object.keys(config).forEach((k) => {
  config_to_env_key = decamelize(k, '_').toUpperCase()
  config_env_value = process.env[config_to_env_key]
  if (config_env_value) {
    config[k] = config_env_value
    console.log(`overriding '${k}' to '${config_env_value}' via env '${config_to_env_key}'`)
  }
})

/* Validate storage
 */

if (config.storage) {
  if (!Array.isArray(config.storage)) {
    if (config.storage.indexOf(',') !== -1) {
      config.storage = config.storage.split(',').map((a) => a.trim())
    } else {
      config.storage = [config.storage.trim()]
    }
  }

  for (let storage of config.storage) {
    const storagePath = path.resolve(__dirname, 'storage/' + storage + '.js')
    if (!fs.existsSync(storagePath)) {
      throw new Error(`Unknown storage solution "${storagePath}"`)
    }
  }
} else {
  config.storage = null
}

/* Others validations
 */

if (config.whitelist && config.whitelist === 'string') {
  config.whitelist = config.whitelist.split(',').map(a => a.trim())
} else if (!config.whitelist) {
  config.whitelist = []
}

if (config.pair) {
  config.pairs = Array.isArray(config.pair) ? config.pair : config.pair.split(',')
  delete config.pair
}

if (!Array.isArray(config.pairs)) {
  if (config.pairs) {
    config.pairs = config.pairs
      .split(',')
      .map((a) => a.trim())
      .filter((a) => a.length)
  } else {
    config.pairs = []
  }
}

if (!config.pairs.length) {
  if (config.collect) {
    console.warn('[warning!] no pairs selected for collection')
  }

  config.pairs = []
}

if (config.exchanges && typeof config.exchanges === 'string') {
  config.exchanges = config.exchanges
    .split(',')
    .map((a) => a.trim())
    .filter((a) => a.length)
}

if (!config.api && config.broadcast) {
  console.warn(
    `[warning!] websocket is enabled but api is set to ${config.api}\n\t(ws server require an http server for the initial upgrade handshake)`
  )
}

if (!config.storage && config.collect) {
  console.warn(`[warning!] server will not persist any of the data it is receiving`)
}

if (!config.collect && !config.api) {
  console.warn(`[warning!] server has no purpose`)
}

if (!config.storage && !config.collect && (config.broadcast || config.api)) {
  console.warn(
    `[warning!] ${
      config.broadcast && config.api ? 'ws and api are' : config.broadcast ? 'ws is' : 'api is'
    } enabled but neither storage or collect is enabled (may be useless)`
  )
}

if (config.broadcast && !config.collect) {
  console.warn(`[warning!] collect is disabled but broadcast is set to ${config.broadcast} (may be useless)`)
}

if (!config.debug) {
  console.debug = function () {}
} else {
  console.debug = console.log
}

module.exports = config
