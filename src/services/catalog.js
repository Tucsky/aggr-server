const axios = require('axios')
const fs = require('fs')
const { ensureDirectoryExists } = require('../helper')

require('../typedef')


module.exports.saveProducts = async function (exchangeId, data) {
  const path = 'products/' + exchangeId + '.json'
  const storage = {
    expiration: +new Date() + 1000 * 60 * 60 * 24 * 2, // 7 days
    data,
  }

  await ensureDirectoryExists(path)

  await new Promise((resolve) => {
    fs.writeFile(path, JSON.stringify(storage), (err) => {
      if (err) {
        console.error(`[${exchangeId}] failed to save products to ${path}`, err)
      }

      resolve()
    })
  })
}

/**
 * Read products from file (products/*exchange id*.json)
 * @returns {Promise<any>} Formated products
 */
module.exports.readProducts = async function (exchangeId) {
  console.debug(`[${exchangeId}] reading stored products...`)

  return new Promise((resolve, reject) => {
    fs.readFile('products/' + exchangeId + '.json', (err, raw) => {
      if (err) {
        console.debug(`[${exchangeId}] no stored products`)
        return resolve(null) // no products returned = will fetch
      }

      try {
        const { expiration, data } = JSON.parse(raw)

        if (!data) {
          throw new Error('invalid exchanges products')
        }

        const now = new Date()

        if (+now > expiration) {
          return resolve(null)
        }

        console.debug(`[${exchangeId}] using stored products`)

        resolve(data)
      } catch (error) {
        reject(error)
      }
    })
  })
}

/**
 * Get products from api endpoint(s) and save to file for 7 days
 * @returns Formated products
 */
module.exports.fetchProducts = async function (exchangeId, endpoints) {
  if (!endpoints || !endpoints.PRODUCTS) {
    /*if (!this.products) {
      this.products = []
    }*/

    return Promise.resolve()
  }

  let urls = typeof endpoints.PRODUCTS === 'function' ? endpoints.PRODUCTS() : endpoints.PRODUCTS

  if (!Array.isArray(urls)) {
    urls = [urls]
  }

  console.debug(`[${exchangeId}] fetching products...`, urls)

  let data = []

  for (let url of urls) {
    const action = url.split('|')

    let method = action.length > 1 ? action.shift() : 'GET'
    let target = action[0]

    data.push(
      await axios
        .get(target, {
          method: method,
        })
        .then((response) => response.data)
        .catch((err) => {
          console.error(`[${exchangeId}] failed to fetch ${target}\n\t->`, err.message)
          throw err
        })
    )
  }

  if (data.length === 1) {
    data = data[0]
  }

  if (data) {
    return data
  }

  return null
}

/**
 * 
 * @param {string} market 
 * @returns {Product}
 */
module.exports.parseMarket = function (market, mergeStable = true) {
  const [exchangeId, pair] = market.match(/([^:]*):(.*)/).slice(1, 3)

  const baseRegex = '([a-z0-9]{2,})'
  const quoteRegexKnown = '(eur|usd|usdt|usdc|tusd)'
  const quoteRegexOthers = '([a-z0-9]{3,})'

  const baseQuoteLookup1 = new RegExp(`^${baseRegex}[^a-z0-9]?${quoteRegexKnown}$`, 'i')
  const baseQuoteLookup2 = new RegExp(`^${baseRegex}[^a-z0-9]?${quoteRegexOthers}$`, 'i')
  const baseQuoteLookupPoloniex = new RegExp(`^(.*)_(.*)$`)

  const id = exchangeId + ':' + pair
  let type = 'spot'

  if (/[UZ_-]\d{2}/.test(pair)) {
    type = 'future'
  } else if (exchangeId === 'BYBIT' && !/-SPOT$/.test(pair)) {
    type = 'perp'
  } else if (exchangeId === 'BITMEX' || /(-|_)swap$|(-|_|:)perp/i.test(pair)) {
    type = 'perp'
  } else if (exchangeId === 'BINANCE_FUTURES') {
    type = 'perp'
  } else if (exchangeId === 'BITFINEX' && /F0$/.test(pair)) {
    type = 'perp'
  } else if (exchangeId === 'PHEMEX' && pair[0] !== 's') {
    type = 'perp'
  } else if (exchangeId === 'HUOBI' && /_(CW|CQ|NW|NQ)$/.test(pair)) {
    type = 'future'
  } else if (exchangeId === 'HUOBI' && /-/.test(pair)) {
    type = 'perp'
  } else if (exchangeId === 'KRAKEN' && /PI_/.test(pair)) {
    type = 'perp'
  }

  let localSymbol = pair

  if (exchangeId === 'BYBIT') {
    localSymbol = localSymbol.replace(/-SPOT$/, '')
  }

  if (exchangeId === 'KRAKEN') {
    localSymbol = localSymbol.replace(/PI_/, '').replace(/FI_/, '')
  }

  if (exchangeId === 'BITFINEX') {
    localSymbol = localSymbol.replace(/(.*)F0:USTF0/, '$1USDT').replace(/UST$/, 'USDT')
  }

  if (exchangeId === 'HUOBI') {
    localSymbol = localSymbol.replace(/_CW|_CQ|_NW|_NQ/i, 'USD')
  }

  if (exchangeId === 'DERIBIT') {
    localSymbol = localSymbol.replace(/_(\w+)-PERPETUAL/i, '$1')
  }

  localSymbol = localSymbol
    .replace(/-PERP(ETUAL)?/i, 'USD')
    .replace(/[^a-z0-9](perp|swap|perpetual)$/i, '')
    .replace(/[^a-z0-9]\d+$/i, '')
    .replace(/[-_/:]/, '')
    .replace(/XBT/i, 'BTC')
    .toUpperCase()

  let match

  if (exchangeId === 'POLONIEX') {
    match = pair.match(baseQuoteLookupPoloniex)

    if (match) {
      match[0] = match[2]
      match[2] = match[1]
      match[1] = match[0]

      localSymbol = match[1] + match[2]
    }
  } else {
    match = localSymbol.match(baseQuoteLookup1)

    if (!match) {
      match = localSymbol.match(baseQuoteLookup2)
    }
  }

  if (!match && (exchangeId === 'DERIBIT' || exchangeId === 'FTX' || exchangeId === 'HUOBI')) {
    match = localSymbol.match(/(\w+)[^a-z0-9]/i)

    if (match) {
      match[2] = match[1]
    }
  }

  let base
  let quote

  if (match) {
    base = match[1]
    quote = match[2]

    localSymbol = base + (mergeStable ? quote.replace(/usd\w|ust/i, 'USD') : quote)
  }

  console.log(`[catalog] registered product ${base}/${quote} from ${exchangeId} (${type})`)

  return {
    id,
    base,
    quote,
    pair,
    local: localSymbol,
    exchange: exchangeId,
    type
  }
}
