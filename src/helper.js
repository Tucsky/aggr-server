const fs = require('fs')

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
      pairs = pairs.split('+').map((a) => a.toUpperCase())
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
      output += (output.length ? ', ' : '') + (timestamp - s * 1000) + 'ms'

    return output.trim()
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

  groupTrades(trades, includeMarket, toArray = false) {
    const groups = {}

    for (let i = 0; i < trades.length; i++) {
      const trade = trades[i]
      const identifier = trade.exchange + ':' + trade.pair

      if (!groups[identifier]) {
        groups[identifier] = []
      }

      if (!toArray) {
        groups[identifier].push(trade)
      } else {
        let toPush;

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
    return new Promise(resolve => {
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
  }
}
