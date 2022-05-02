const fs = require('fs')
const config = require('../src/config')

module.exports = {
  getIp(req) {
    let ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress

    if (ip.indexOf('::ffff:') === 0) {
      ip = ip.substr('::ffff:'.length, ip.length)
    }

    return ip
  },

  parsePairsFromWsRequest(req, defaultPair) {
    let pairs = req.url.substr(1)

    if (!pairs || !pairs.length) {
      if (defaultPair) {
        pairs = [defaultPair]
      } else {
        pairs = []
      }
    } else {
      pairs = pairs.split('+')
    }

    return pairs
  },

  ID() {
    return Math.random().toString(36).substr(2, 9)
  },

  getHms(timestamp, round, ms = true) {
    var d = Math.floor(timestamp / 1000 / 86400)
    var h = Math.floor((timestamp / 1000 / 3600) % 24)
    var m = Math.floor(((timestamp / 1000) % 3600) / 60)
    var s = Math.floor(((timestamp / 1000) % 3600) % 60)
    var output = ''

    output += (!round || !output.length) && d > 0 ? d + 'd' + (!round && h ? ', ' : '') : ''
    output += (!round || !output.length) && h > 0 ? h + 'h' + (!round && m ? ', ' : '') : ''
    output += (!round || !output.length) && m > 0 ? m + 'm' + (!round && s ? ', ' : '') : ''
    output += (!round || !output.length) && s > 0 ? s + 's' : ''

    if (ms && (!output.length || (!round && timestamp < 60 * 1000 && timestamp > s * 1000)))
      output += (output.length ? ' ' : '') + Math.round(timestamp - s * 1000) + 'ms'

    return output.trim()
  },

  resolution(time) {
    if (time >= 10080) {
      return time / 10080 + 'W'
    } else if (time >= 1440) {
      return time / 1440 + 'D'
    } else {
      return time
    }
  },

  ago(timestamp) {
    const seconds = Math.floor((new Date() - timestamp) / 1000)
    let interval, output

    if ((interval = Math.floor(seconds / 31536000)) > 1) output = interval + 'y'
    else if ((interval = Math.floor(seconds / 2592000)) >= 1) output = interval + 'm'
    else if ((interval = Math.floor(seconds / 86400)) >= 1) output = interval + 'd'
    else if ((interval = Math.floor(seconds / 3600)) >= 1) output = interval + 'h'
    else if ((interval = Math.floor(seconds / 60)) >= 1) output = interval + 'm'
    else output = Math.ceil(seconds) + 's'

    return output
  },

  groupTrades(trades, includeMarket, threshold = 0) {
    const groups = {}

    for (let i = 0; i < trades.length; i++) {
      if (trades[i].size < threshold) {
        continue
      }

      const trade = trades[i]
      const identifier = trade.exchange + ':' + trade.pair

      if (!groups[identifier]) {
        groups[identifier] = []
      }

      let toPush

      if (includeMarket) {
        toPush = [trade.exchange, trade.pair, trade.timestamp, trade.price, trade.size, trade.side === 'buy' ? 1 : 0]
      } else {
        toPush = [trade.timestamp, trade.price, trade.size, trade.side === 'buy' ? 1 : 0]
      }

      if (trade.liquidation) {
        toPush.push(1)
      }

      groups[identifier].push(toPush)
    }

    return groups
  },

  formatAmount(amount, decimals) {
    const negative = amount < 0

    if (negative) {
      amount = Math.abs(amount)
    }

    if (amount >= 1000000) {
      amount = +(amount / 1000000).toFixed(isNaN(decimals) ? 1 : decimals) + 'M'
    } else if (amount >= 100000) {
      amount = +(amount / 1000).toFixed(isNaN(decimals) ? 0 : decimals) + 'K'
    } else if (amount >= 1000) {
      amount = +(amount / 1000).toFixed(isNaN(decimals) ? 1 : decimals) + 'K'
    } else {
      amount = +amount.toFixed(4)
    }

    if (negative) {
      return '-' + amount
    } else {
      return amount
    }
  },

  sleep(delay = 1000) {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve()
      }, delay)
    })
  },

  async ensureDirectoryExists(target) {
    const folder = target.substring(0, target.lastIndexOf('/'))

    return new Promise((resolve, reject) => {
      fs.stat(folder, (err) => {
        if (!err) {
          resolve()
        } else if (err.code === 'ENOENT') {
          fs.mkdir(folder, { recursive: true }, (err) => {
            if (err) {
              reject(err)
            }

            resolve()
          })
        } else {
          reject(err)
        }
      })
    })
  },
  parseDuration(duration) {
    duration = duration.toString().trim()

    if (/d$/i.test(duration)) {
      output = parseFloat(duration) * 60 * 60 * 24
    } else if (/h$/i.test(duration)) {
      output = parseFloat(duration) * 60 * 60
    } else if (/m$/i.test(duration)) {
      output = parseFloat(duration) * 60
    } else if (/ms$/i.test(duration)) {
      output = parseFloat(duration) / 1000
    } else {
      output = parseFloat(duration)
    }

    return output * 1000
  },
  parseDatetime(datetime) {
    const date = new Date(datetime)

    if (isNaN(+date)) {
      throw new Error(`Invalid date ${datetime}`)
    }

    return +date
    //return +new Date(date.getTime() - date.getTimezoneOffset() * 60000)
  },
  getMarkets() {
    return config.pairs.map((market) => {
      const [exchange, symbol] = market.match(/([^:]*):(.*)/).slice(1, 3)

      if (config.exchanges.indexOf(exchange) === -1) {
        console.warn(`${market} is not supported`)
      }

      return {
        market,
        exchange,
        symbol,
      }
    })
  },
  async prepareStandalone(onlyNativeRecovery = true) {
    if (!config.exchanges || !config.exchanges.length) {
      config.exchanges = []

      fs.readdirSync('./src/exchanges/').forEach((file) => {
        ;/\.js$/.test(file) && config.exchanges.push(file.replace(/\.js$/, ''))
      })
    }

    const exchanges = []

    for (let i = 0; i < config.exchanges.length; i++) {
      const name = config.exchanges[i]
      const exchange = new (require('../src/exchanges/' + name))(config)

      if (!onlyNativeRecovery || typeof exchange.getMissingTrades === 'function') {
        config.exchanges[i] = exchange.id

        exchanges.push(exchange)
      } else {
        config.exchanges.splice(i, 1)
        i--
      }
    }

    if (config.from) {
      config.from = module.exports.parseDatetime(config.from)
    } else {
      throw new Error('from is required')
    }

    if (config.to) {
      config.to = module.exports.parseDatetime(config.to) - 1
    } else {
      config.to = +new Date()
    }

    if (isNaN(config.to) || isNaN(config.to)) {
      throw new Error('invalid from / to')
    }

    config.resolution = module.exports.parseDuration(config.resolution)

    if (isNaN(config.resolution)) {
      throw new Error('invalid resolution')
    }

    if (onlyNativeRecovery) {
      for (const exchange of exchanges) {
        await exchange.getProducts()
      }
    }

    for (let name of config.storage) {
      if (name !== 'influx') {
        continue
      }

      console.log(`[storage] Using "${name}" storage solution`)

      storage = new (require(`../src/storage/${name}`))(config)

      if (typeof storage.connect === 'function') {
        await storage.connect()
      }

      break
    }

    if (!storage) {
      throw new Error('this utility script requires influx storage')
    }

    return { exchanges, storage }
  },
  humanFileSize(size) {
    var i = Math.floor(Math.log(size) / Math.log(1024))
    return (size / Math.pow(1024, i)).toFixed(2) * 1 + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i]
  },
}
