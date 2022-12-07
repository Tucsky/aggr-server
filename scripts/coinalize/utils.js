const axios = require('axios')
const fs = require('fs')

const config = require('../../src/config')
const { sleep } = require('../../src/helper')

const COINALIZE_RESOLUTIONS = [1, 5, 15, 30, 60, 60 * 2, 60 * 4, 60 * 6, 60 * 12, 60 * 24, '1D']

const COINALIZE_VENUES = {
  BINANCE: 'A',
  BITMEX: '0',
  BITFINEX: 'F',
  BYBIT: '6',
  DERIBIT: '2',
  FTX: '5',
  HUOBI: '4',
  KRAKEN: 'K',
  OKEX: '3',
  PHEMEX: '7',
  DYDX: '8',
  BITSTAMP: 'B',
  BITTREX: 'T',
  COINBASE: 'C',
  GEMINI: 'G',
  POLONIEX: 'P',
}

const COINALIZE_REQ_KEY_PATH = 'products/coinalize.key'

const BARS_PER_REQUEST = 300

const baseHeaders = {
  accept: '*/*',
  'accept-language': 'en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7',
  'cache-control': 'no-cache',
  pragma: 'no-cache',
  'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-origin',
  cookie: 'p_sid=s%3ARY_H7HfwdvenT8Pz2XjPjBq-mHVLMsmL.ONSYPlNqShlYwQoVZ4Ez8igcJZ1r1wAG6pw7HQ%2FJ2WE',
  Referer: 'https://coinalyze.net/bitcoin/usd/binance/btcusd_perp/price-chart-live/',
  'Referrer-Policy': 'strict-origin-when-cross-origin',
}

const baseJSONHeaders = {
  ...baseHeaders,
  'content-type': 'application/json',
  'x-requested-with': 'XMLHttpRequest',
}

let REQ_KEY

function getCoinalizeMarket(product) {
  let symbol = product.local

  if ((product.exchange === 'FTX' && product.type === 'perp') || product.exchange === 'DERIBIT' || product.exchange === 'BYBIT') {
    symbol = product.pair
  } else if (product.type === 'perp') {
    symbol += '_PERP'
  }

  if (product.exchange === 'BINANCE' && product.type === 'spot' && product.quote === 'USDT') {
    symbol = product.base + 'USD'
  }

  if (product.exchange === 'BYBIT' && product.type === 'spot') {
    symbol = 's' + symbol.replace('-SPOT', '')
  }

  if (product.exchange === 'BITFINEX') {
    if (product.local === 'LUNA2USD') {
      symbol = symbol.replace('LUNA2', 'LUNA')
    } else if (product.local === 'LUNAUSD') {
      symbol = symbol.replace('LUNA', 'LUNC')
    }
  }

  return symbol.replace('LUNA', 'LUNC') + '.' + COINALIZE_VENUES[product.exchange.replace(/_\w+/, '')]
}

function getCoinalizeResolution(time) {
  let resolution

  if (time >= 604800000) {
    resolution = time / 604800000 + 'W'
  } else if (time >= 86400000) {
    resolution = time / 86400000 + 'D'
  } else {
    resolution = time / 60000 // minutes
  }

  if (COINALIZE_RESOLUTIONS.indexOf(resolution) === -1) {
    throw new Error('unavailable resolution (' + resolution + ')')
  }

  return resolution
}

function readReqKey() {
  return new Promise((resolve) => {
    fs.readFile(COINALIZE_REQ_KEY_PATH, 'utf8', (err, keyStamp) => {
      if (err) {
        if (err.code === 'ENOENT') {
          resolve([null, null])
          return
        }

        throw new Error(`failed to read coinalize req key (${err.message})`)
      }

      resolve(keyStamp.split('|'))
    })
  })
}

function saveReqKey(key) {
  return new Promise((resolve) => {
    fs.writeFile(COINALIZE_REQ_KEY_PATH, key + '|' + Date.now(), (err) => {
      if (err) {
        throw new Error(`failed to save coinalize req key (${err.message})`)
      }

      resolve()
    })
  })
}

async function getReqKey() {
  let [key, timestamp] = await readReqKey()

  let renew = config.renew || !key

  if (!renew && Date.now() - timestamp > 1000 * 60 * 2) {
    renew = true
  }

  if (renew || !key) {
    key = await fetchReqKey()

    await saveReqKey(key)
  }

  REQ_KEY = key

  return key
}

function fetchReqKey() {
  return axios
    .get('https://coinalyze.net/bitcoin/usd/binance/btcusd_perp/price-chart-live/', {
      headers: baseHeaders,
    })
    .then((response) => response.data)
    .then((body) => {
      const match = body.match(/window.REQ_KEY\s*=\s*'([a-z0-9-]+)';/)

      if (match && match[1]) {
        return match[1]
      } else {
        throw new Error('invalid req key')
      }
    })
}

module.exports.getAllData = async function (product, from, to, timeframe) {
  const coinalizeMarket = getCoinalizeMarket(product)
  
  const coinalizeResolution = getCoinalizeResolution(timeframe)
  console.log('[getAllData]', product.id, coinalizeMarket, coinalizeResolution, 'from', new Date(from).toISOString(), 'to', new Date(to).toISOString())
  const timePerRequest = timeframe * BARS_PER_REQUEST

  const data = []

  let isFirst = true

  for (let time = to; time >= from; time -= timePerRequest) {
    await sleep(Math.random() * 1500 + 1500)
    const reqFrom = Math.max(from, time - timePerRequest)
    const reqTo = Math.min(to, time)
    console.log('- from', new Date(reqFrom).toISOString(), 'to', new Date(reqTo).toISOString(), coinalizeMarket, coinalizeResolution)
    const chunk = await getData(product, reqFrom, reqTo, timeframe, isFirst)

    if (chunk.length) {
      Array.prototype.push.apply(data, chunk)
    } else {
      break
    }

    isFirst = false
  }

  return data
}

function getBars(product, from, to, timeframe, isFirst, dataset) {
  const coinalizeMarket = getCoinalizeMarket(product)
  const coinalizeResolution = getCoinalizeResolution(timeframe)

  let source = [coinalizeMarket]

  if (dataset === 'liquidations') {
    const [pair, exchangeId] = coinalizeMarket.split('.')
    source.push(pair + '_LQB.' + exchangeId)
    source.push(pair + '_LQS.' + exchangeId)
  }

  source = source.join(',') + (dataset ? '#' + dataset : '')

  const flooredFrom = Math.floor(from / timeframe) * timeframe
  const flooredTo = Math.floor(to / timeframe) * timeframe

  // console.log('[FETCH]', `${source} (${new Date(flooredFrom).toISOString()} -> ${new Date(flooredTo).toISOString()})`)

  const data = `{"from":${flooredFrom / 1000},"to":${
    flooredTo / 1000
  },"resolution":"${coinalizeResolution}","symbol":"${source}","firstDataRequest":${
    isFirst ? 'true' : 'false'
  },"symbolsForUsdConversion":[],"rk":"${REQ_KEY}"}`
  
  return axios
    .post('https://coinalyze.net/chart/getBars/', data, {
      headers: baseJSONHeaders,
    })
    .then((response) => {
      return response.data
    })
    .catch((err) => {
      debugger
      throw err
    })
}

function getOHLC(product, from, to, timeframe, isFirst) {
  return getBars(product, from, to, timeframe, isFirst).then(({ barData }) => {
    return barData.reduce((output, dataPoint) => {
      output[dataPoint[0] * 1000] = {
        open: dataPoint[1],
        high: dataPoint[2],
        low: dataPoint[3],
        close: dataPoint[4],
      }

      return output
    }, {})
  })
}

function getBuySellVolume(product, from, to, timeframe, isFirst) {
  return getBars(product, from, to, timeframe, isFirst, 'buy_sell_volume').then(({ barData }) => {
    return barData.reduce((output, dataPoint) => {
      output[dataPoint[0] * 1000] = {
        vbuy: dataPoint[1],
        vsell: dataPoint[2],
      }

      return output
    }, {})
  })
}

function getBuySellCount(product, from, to, timeframe, isFirst) {
  return getBars(product, from, to, timeframe, isFirst, 'buy_sell_count').then(({ barData }) => {
    return barData.reduce((output, dataPoint) => {
      output[dataPoint[0] * 1000] = {
        cbuy: dataPoint[1],
        csell: dataPoint[2],
      }

      return output
    }, {})
  })
}

function getBuySellLiquidations(product, from, to, timeframe, isFirst) {
  return getBars(product, from, to, timeframe, isFirst, 'liquidations').then(({ barData }) => {
    return Object.keys(barData).reduce((output, timestamp) => {
      output[timestamp * 1000] = barData[timestamp].reduce(
        (dataPoint, keyValue) => {
          if (/_LQB\./.test(keyValue[0])) {
            dataPoint.lbuy = keyValue[1]
          } else if (/_LQS\./.test(keyValue[0])) {
            dataPoint.lsell = keyValue[1]
          }

          return dataPoint
        },
        {
          lbuy: 0,
          lsell: 0,
        }
      )

      return output
    }, {})
  })
}

async function getData(product, from, to, timeframe, isFirst) {
  if (!REQ_KEY) {
    await getReqKey()
  }

  const bars = []

  const ohlcData = await getOHLC(product, from, to, timeframe, isFirst)
  await sleep(Math.random() * 100 + 100)
  const buySellVolumeData = await getBuySellVolume(product, from, to, timeframe, isFirst)
  await sleep(Math.random() * 100 + 100)
  const buySellCountData = await getBuySellCount(product, from, to, timeframe, isFirst)
  await sleep(Math.random() * 100 + 100)
  const buySellLiquidationsData = await getBuySellLiquidations(product, from, to, timeframe, isFirst)
  await sleep(Math.random() * 100 + 100)

  const flooredFrom = Math.floor(from / timeframe) * timeframe
  const flooredTo = Math.floor(to / timeframe) * timeframe

  for (let time = flooredTo; time >= flooredFrom; time -= timeframe) {
    bars.push({
      time,
      ...ohlcData[time],
      ...buySellVolumeData[time],
      ...buySellCountData[time],
      ...buySellLiquidationsData[time],
    })
  }

  return bars
}
