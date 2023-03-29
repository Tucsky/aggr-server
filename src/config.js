const fs = require('fs')
const path = require('path')
const { parseList, camelcaseToSnakecase } = require('./helper')

console.log(`[init] reading .env...`)
console.log(require('dotenv').config().parsed.COLLECT)
process.exit()

/*
*/

const cliOptions = {}
let specificConfigFile

if (process.argv.length > 2) {
  config.EXCHANGES = []
  process.argv.slice(2).forEach((arg) => {
    const keyvalue = arg.split('=')

    const envKey = camelcaseToSnakecase(keyvalue[0])

    if (['CONFIG', 'CONFIG_PATH', 'CONFIG_FILE'].includes(envKey)) {
      specificConfigFile = envKey
    }

    if (keyvalue.length > 1) {
      cliOptions[envKey] = isNaN(+keyvalue[1]) ? keyvalue[1] : +keyvalue[1]
    } else if (/^\w+$/.test(keyvalue[0])) {
      cliOptions.EXCHANGES.push(keyvalue[0])
    }
  })
}

require('dotenv').config()
const config = {
  ...require('dotenv').config({
    path: '.env.default'
  }).parsed,
  ...require('dotenv').config({
    path: cliOptions.config
  }).parsed,
}

for (const prop in config) {
  try {
    config[prop] = JSON.parse(process.env[prop])
  } catch (error) {
    config[prop] = process.env[prop]
  }
}

console.log(process.env.COLLECT === true)

/* Load custom server configuration
 */


if (specificConfigFile) {
  try {
    console.log('[init] using config file ' + specificConfigFile)
    specificConfigFile = path.resolve(__dirname, '../' + specificConfigFile)
    /*const configExamplePath = path.resolve(__dirname, '../config.json.example')
  
    if (!fs.existsSync(configPath) && fs.existsSync(configExamplePath) && !specificConfigFile) {
      fs.copyFileSync(configExamplePath, configPath)
    }*/
  
    config.CONFIG_PATH = specificConfigFile
  
    const legacyConfig = require(specificConfigFile) || {}
  
    for (const prop in legacyConfig) {
      console.log(prop, camelcaseToSnakecase(prop))
      process.env[camelcaseToSnakecase(prop)] = legacyConfig[prop]
    }
  } catch (error) {
    throw new Error(`Unable to parse json configuration file "${specificConfigFile}"\n\n${error.message}`)
  }
}

/* Validate lists
 */
config.STORAGE = parseList(config.STORAGE)
config.MARKETS = parseList(config.MARKETS)
config.MARKETS_EXTRA = parseList(config.INFLUX_RESAMPLE_TO)
config.INFLUX_RESAMPLE_TO = parseList(config.INFLUX_RESAMPLE_TO)
config.API_WHITELIST = parseList(config.API_WHITELIST)
config.INDEX_EXCHANGE_BLACKLIST = parseList(config.INDEX_EXCHANGE_BLACKLIST)
config.INDEX_QUOTE_WHITELIST = parseList(config.INDEX_QUOTE_WHITELIST)

/* Load available exchanges
 */

if (!config.EXCHANGES || !config.EXCHANGES.length) {
  config.EXCHANGES = []

  fs.readdirSync('./src/exchanges/').forEach((file) => {
    ;/\.js$/.test(file) && config.EXCHANGES.push(file.replace(/\.js$/, ''))
  })
}

/* Validate storage
*/
for (let storage of config.STORAGE) {
  const storagePath = path.resolve(__dirname, 'storage/' + storage + '.js')
  if (!fs.existsSync(storagePath)) {
    throw new Error(`Unknown storage solution "${storagePath}"`)
  }
}

/* Misconfiguration warnings
*/

if (!config.MARKETS.length) {
  if (config.COLLECT) {
    console.warn('[warning!] no pairs selected for collection')
  }
}

if (!config.API && config.BROADCAST) {
  console.warn(
    `[warning!] websocket is enabled but api is set to ${config.API}\n\t(ws server require an http server for the initial upgrade handshake)`
  )
}

if (!config.STORAGE && config.COLLECT) {
  console.warn(`[warning!] server will not persist any of the data it is receiving`)
}

if (!config.COLLECT && !config.API) {
  console.warn(`[warning!] server isn't doing anything (no collect, no api)`)
  process.exit()
}

if (!config.STORAGE && !config.COLLECT && (config.BROADCAST || config.API)) {
  console.warn(
    `[warning!] ${
      config.BROADCAST && config.API ? 'ws and api are' : config.broadcast ? 'ws is' : 'api is'
    } enabled but neither storage or collect is enabled (may be useless)`
  )
}

if (config.BROADCAST && !config.COLLECT) {
  console.warn(`[warning!] collect is disabled but broadcast is set to ${config.broadcast} (may be useless)`)
}

if (!config.DEBUG) {
  console.debug = function () {}
} else {
  console.debug = console.log
}

module.exports = config
