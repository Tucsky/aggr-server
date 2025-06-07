const axios = require('axios')
const fs = require('fs')
const { ensureDirectoryExists } = require('../helper')

const stablecoins = [
  'USDT',
  'USDC',
  'TUSD',
  'FDUSD',
  'BUSD',
  'USDD',
  'USDK',
  'USDP',
  'UST'
]

const normalStablecoinLookup = /^U/

const normalStablecoins = stablecoins.filter(s =>
  normalStablecoinLookup.test(s)
)

const reverseStablecoins = stablecoins.filter(
  s => !normalStablecoinLookup.test(s)
)

const currencies = ['EUR', 'USD', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'CNH']

const currencyPairLookup = new RegExp(
  `^([A-Z0-9]{2,})[-/:]?(${currencies.join('|')})$`
)

// use 2+ caracters symbol for normal stablecoins, and 3+ for reversed
// not infallible but avoids coin with symbol finishing with T or B to be labeled as TUSD or BUSD quoted markets
const stablecoinPairLookup = new RegExp(
  `^([A-Z0-9]{2,})[-/:_]?(${normalStablecoins.join(
    '|'
  )})$|^([A-Z0-9]{3,})[-/:_]?(${reverseStablecoins.join('|')})$`
)
const simplePairLookup = new RegExp(`^([A-Z0-9]{2,})[-/_]?([A-Z0-9]{3,})$`)

const reverseStablecoinPairLookup = new RegExp(
  `(\\w{3,})(${reverseStablecoins.join('|')})$`,
  'i'
)
const standardCurrencyPairLookup = new RegExp(
  `(\\w{3})?(${currencies.join('|')})[a-z]?$`,
  'i'
)

require('../typedef')

module.exports.saveProducts = async function (exchangeId, data) {
  const path = 'products/' + exchangeId + '.json'
  const storage = {
    expiration: +new Date() + 1000 * 60 * 60 * 24 * 2, // 7 days
    data
  }

  await ensureDirectoryExists(path)

  return new Promise(resolve => {
    fs.writeFile(path, JSON.stringify(storage), err => {
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
    return Promise.resolve()
  }

  let urls =
    typeof endpoints.PRODUCTS === 'function'
      ? endpoints.PRODUCTS()
      : endpoints.PRODUCTS

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
          method: method
        })
        .then(response => response.data)
        .catch(err => {
          console.error(
            `[${exchangeId}] failed to fetch ${target}\n\t->`,
            err.message
          )
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

function stripStablePair(pair) {
  return pair
    .replace(reverseStablecoinPairLookup, '$1USD')
    .replace(standardCurrencyPairLookup, '$1$2')
}

/**
 *
 * @param {string} market
 * @returns {Product}
 */
module.exports.parseMarket = function (exchangeId, symbol, noStable = true) {
  const id = exchangeId + ':' + symbol

  let type = 'spot'

  if (/[HUZ_-]\d{2}/.test(symbol)) {
    type = 'future'
  } else if (
    exchangeId === 'BINANCE_FUTURES' ||
    exchangeId === 'DYDX' ||
    exchangeId === 'HYPERLIQUID'
  ) {
    type = 'perp'
  } else if (exchangeId === 'BITFINEX' && /F0$/.test(symbol)) {
    type = 'perp'
  } else if (exchangeId === 'HUOBI' && /_(CW|CQ|NW|NQ)$/.test(symbol)) {
    type = 'future'
  } else if (exchangeId === 'BITMART' && !/_/.test(symbol)) {
    type = 'perp'
  } else if (exchangeId === 'HUOBI' && /-/.test(symbol)) {
    type = 'perp'
  } else if (exchangeId === 'BYBIT' && !/-SPOT$/.test(symbol)) {
    if (/.*[0-9]{2}$/.test(symbol)) {
      type = 'future'
    } else if (!/-SPOT$/.test(symbol)) {
      type = 'perp'
    }
  } else if (
    exchangeId === 'BITMEX' ||
    /(-|_)swap$|(-|_|:)perp/i.test(symbol)
  ) {
    if (/\d{2}/.test(symbol)) {
      type = 'future'
    } else {
      type = 'perp'
    }
  } else if (exchangeId === 'PHEMEX' && symbol[0] !== 's') {
    type = 'perp'
  } else if (exchangeId === 'KRAKEN' && /_/.test(symbol) && type === 'spot') {
    type = 'perp'
  } else if (
    (exchangeId === 'BITGET' || exchangeId === 'MEXC') &&
    symbol.indexOf('_') !== -1
  ) {
    type = 'perp'
  } else if (exchangeId === 'KUCOIN' && symbol.indexOf('-') === -1) {
    type = 'perp'
  }

  let localSymbol = symbol

  if (exchangeId === 'BYBIT') {
    localSymbol = localSymbol.replace(/-SPOT$/, '')
  } else if (exchangeId === 'KRAKEN') {
    localSymbol = localSymbol.replace(/(PI|FI|PF)_/, '')
  } else if (exchangeId === 'BITFINEX') {
    localSymbol = localSymbol
      .replace(/(.*)F0:(\w+)F0/, '$1-$2')
      .replace(/UST($|F0)/, 'USDT$1')
  } else if (exchangeId === 'HUOBI') {
    localSymbol = localSymbol.replace(/_CW|_CQ|_NW|_NQ/i, 'USD')
  } else if (exchangeId === 'DERIBIT') {
    localSymbol = localSymbol.replace(/_(\w+)-PERPETUAL/i, '$1')
  } else if (exchangeId === 'BITGET') {
    localSymbol = localSymbol
      .replace('USD_DMCBL', 'USD')
      .replace('PERP_CMCBL', 'USDC')
      .replace(/_.*/, '')
  } else if (exchangeId === 'KUCOIN') {
    localSymbol = localSymbol.replace(/M$/, '')
  } else if (exchangeId === 'HYPERLIQUID') {
    localSymbol = localSymbol.replace(/^k/, '') + 'USD'
  } else if (exchangeId === 'PHEMEX') {
    localSymbol = localSymbol.replace(/^[a-z]/, '')
  }

  localSymbol = localSymbol
    .replace(/xbt$|^xbt/i, 'BTC')
    .replace(/-PERP(ETUAL)?/i, '-USD')
    .replace(/[^a-z0-9](perp|swap|perpetual)$/i, '')
    .replace(/[^a-z0-9]\d+$/i, '')
    .toUpperCase()

  let localSymbolAlpha = localSymbol.replace(/[-_/:]/, '')

  let match
  if (!/BINANCE/.test(exchangeId)) {
    match = localSymbol.match(currencyPairLookup)
  }

  if (!match) {
    match = localSymbol.match(stablecoinPairLookup)

    if (!match) {
      match = localSymbolAlpha.match(simplePairLookup)
    }

    if (!match && (exchangeId === 'DERIBIT' || exchangeId === 'HUOBI')) {
      match = localSymbolAlpha.match(/(\w+)[^a-z0-9]/i)

      if (match) {
        match[2] = match[1]
      }
    }
  }
  if (!match) {
    return null
  }

  let base
  let quote

  if (match[1] === undefined && match[2] === undefined) {
    base = match[3]
    quote = match[4] || ''
  } else {
    base = match[1]
    quote = match[2] || ''
  }

  if (noStable) {
    localSymbolAlpha = stripStablePair(base + quote)
  } else {
    localSymbolAlpha = base + quote
  }

  return {
    id,
    base,
    quote,
    pair: symbol,
    local: localSymbolAlpha,
    exchange: exchangeId,
    type
  }
}
