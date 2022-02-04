const fs = require('fs')

console.log('PID: ', process.pid)

const config = require('./src/config')
const { parseDuration, parseDatetime, getHms } = require('./src/helper')

/* Load available exchanges
 */

if (!config.exchanges || !config.exchanges.length) {
  config.exchanges = []

  fs.readdirSync('./src/exchanges/').forEach((file) => {
    ;/\.js$/.test(file) && config.exchanges.push(file.replace(/\.js$/, ''))
  })
}

const exchanges = []

for (let i = 0; i < config.exchanges.length; i++) {
  const name = config.exchanges[i]
  const exchange = new (require('./src/exchanges/' + name))(config)

  if (typeof exchange.getMissingTrades === 'function') {
    config.exchanges[i] = exchange.id
  
    exchanges.push(exchange)
  } else {
    config.exchanges.splice(i, 1)
    i--
  }
}

const HOUR = 1000 * 60 * 60 
const DAY = HOUR * 24

if (config.from) {
  config.from = parseDatetime(config.from)
} else {
  config.from = Math.floor(new Date() / DAY) * DAY
}

if (config.to) {
  config.to = parseDatetime(config.to)
} else {
  let range

  if (config.duration) {
    range = parseDuration(config.duration)
  }

  if (!range) {
    range = HOUR
  }

  config.to = config.from + range
}

const pairs = config.pairs.map(market => {
  const [exchange, symbol] = market.match(/([^:]*):(.*)/).slice(1, 3)

  if (config.exchanges.indexOf(exchange) === -1) {
    console.warn(`${market} is not supported`)
  }

  return {
    market,
    exchange,
    symbol
  }
})

console.log('Recover trades')
console.log('from', new Date(config.from).toISOString())
console.log('to', new Date(config.to).toISOString())
console.log('on', pairs.map(a => a.market).join(', '))

const recoveryRanges = pairs.reduce((timestamps, pair) => {
  timestamps[pair.market] = {
    pair: pair.symbol,
    from: config.from,
    to: config.to,
  }

  return timestamps
}, {})

const storages = []
const pendingTrades = []

async function insert() {
  const trades = pendingTrades.splice(0, pendingTrades.length)
  
  for (const storage of storages) {
    await storage.insert(trades)
  }
}

async function program() {
  const promisesOfStorages = []

  for (const exchange of exchanges) {
    await exchange.getProducts()
    exchange.on('trades', trades => {
      Array.prototype.push.apply(pendingTrades, trades)
    })
  }
  
  for (let name of config.storage) {
    console.log(`[storage] Using "${name}" storage solution`)
  
    if (config.api && config.storage.length > 1 && !config.storage.indexOf(name)) {
      console.log(`[storage] Set "${name}" as primary storage for API`)
    }
  
    let storage = new (require(`./src/storage/${name}`))(config)

    if (typeof storage.insert === 'function') {
      if (typeof storage.connect === 'function') {
        promisesOfStorages.push(storage.connect())
      } else {
        promisesOfStorages.push(Promise.resolve())
      }
    
      storages.push(storage)
    }
  }

  if (!storages.length) {
    throw new Error(`Found 0 suitable storage solution`)
  }

  await Promise.all(promisesOfStorages)

  for (const pair of pairs) {
    const exchange = exchanges.find(exchange => exchange.id === pair.exchange)

    if (!exchange) {
      continue
    }

    console.log(`[${pair.market}] start recovery procedure`)

    const recovered = await exchange.getMissingTrades(recoveryRanges[pair.market], config.to)

    await insert()

    console.log(`[${pair.market}] end recovery procedure (${recovered} recovered)`)
  }
}

program()