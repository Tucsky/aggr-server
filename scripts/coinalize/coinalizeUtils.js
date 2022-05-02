const fetch = require('node-fetch')
const fs = require('fs')

const config = require('../../src/config')
const { sleep, resolution } = require('../../src/helper')

module.exports.COINALIZE_RESOLUTIONS = [1, 5, 15, 30, 60, 60 * 2, 60 * 4, 60 * 6, 60 * 12, 60 * 24]

module.exports.COINALIZE_VENUES = {
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

const COINALIZE_REQ_KEY_PATH = module.exports.COINALIZE_REQ_KEY_PATH = 'coinalize.key'

const BARS_PER_REQUEST = module.exports.BARS_PER_REQUEST = 300

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

let REQ_KEY = module.exports.REQ_KEY

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

module.exports.getReqKey = async function() {
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
  return fetch('https://coinalyze.net/bitcoin/usd/binance/btcusd_perp/price-chart-live/', {
    headers: baseHeaders,
  })
    .then((response) => response.text())
    .then((body) => {
      const match = body.match(/window.REQ_KEY\s*=\s*'([a-z0-9-]+)';/)

      if (match && match[1]) {
        return match[1]
      } else {
        throw new Error('invalid req key')
      }
    })
}

module.exports.getAllData = async function(from, to, timeframe, symbol) {
  const timePerBar = timeframe * 1000 * 60
  const timePerRequest = timePerBar * BARS_PER_REQUEST

  const data = []

  let first = true

  console.log('get all data for', symbol)

  for (let time = to; time >= from; time -= timePerRequest) {

    await sleep(Math.random() * 1500 + 1500)
    const reqFrom = Math.max(from, time - timePerRequest)
    const reqTo = Math.min(to, time)
    console.log('- from', new Date(reqFrom).toISOString(), 'to', new Date(reqTo).toISOString())
    const chunk = await getData(reqFrom, reqTo, timeframe, symbol, first)

    if (chunk.length) {
      Array.prototype.push.apply(data, chunk)
    } else {
      break;
    }

    first = false
  }

  return data
}

function getBars(from, to, timeframe, symbol, first, dataset) {
  let source = [symbol]

  if (dataset === 'liquidations') {
    const [pair, exchangeId] = symbol.split('.')
    source.push(pair + '_LQB.' + exchangeId)
    source.push(pair + '_LQS.' + exchangeId)
  }

  source = source.join(',') + (dataset ? '#' + dataset : '')

  const msResolution = timeframe * 1000 * 60

  const flooredFrom = Math.floor(from / msResolution) * msResolution
  const flooredTo = Math.floor(to / msResolution) * msResolution

  // console.log('[FETCH]', `${source} (${new Date(flooredFrom).toISOString()} -> ${new Date(flooredTo).toISOString()})`)

  const body = `{"from":${flooredFrom / 1000},"to":${
    flooredTo / 1000
  },"resolution":"${resolution(timeframe)}","symbol":"${source}","firstDataRequest":${
    first ? 'true' : 'false'
  },"symbolsForUsdConversion":[],"rk":"${REQ_KEY}"}`

  return fetch('https://coinalyze.net/chart/getBars/', {
    headers: baseJSONHeaders,
    body,
    method: 'POST',
  })
    .then((response) => {
      return response.json()
    })
    .catch((err) => {
      debugger
      throw err
    })
}

function getOHLC(from, to, timeframe, symbol, first) {
  return getBars(from, to, timeframe, symbol, first).then(({ barData }) => {
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

function getBuySellVolume(from, to, timeframe, symbol, first) {
  return getBars(from, to, timeframe, symbol, first, 'buy_sell_volume').then(({ barData }) => {
    return barData.reduce((output, dataPoint) => {
      output[dataPoint[0] * 1000] = {
        vbuy: dataPoint[1],
        vsell: dataPoint[2],
      }

      return output
    }, {})
  })
}

function getBuySellCount(from, to, resolution, symbol, first) {
  return getBars(from, to, resolution, symbol, first, 'buy_sell_count').then(({ barData }) => {
    return barData.reduce((output, dataPoint) => {
      output[dataPoint[0] * 1000] = {
        cbuy: dataPoint[1],
        csell: dataPoint[2],
      }

      return output
    }, {})
  })
}

function getBuySellLiquidations(from, to, resolution, symbol, first) {
  return getBars(from, to, resolution, symbol, first, 'liquidations').then(({ barData }) => {
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

async function getData(from, to, resolution, symbol, first) {
  if (!REQ_KEY) {
    throw new Error('get rek key first')
  }

  const bars = []

  const ohlcData = await getOHLC(from, to, resolution, symbol, first)
  await sleep(Math.random() * 100 + 100)
  const buySellVolumeData = await getBuySellVolume(from, to, resolution, symbol, first)
  await sleep(Math.random() * 100 + 100)
  const buySellCountData = await getBuySellCount(from, to, resolution, symbol, first)
  await sleep(Math.random() * 100 + 100)
  const buySellLiquidationsData = await getBuySellLiquidations(from, to, resolution, symbol, first)
  await sleep(Math.random() * 100 + 100)

  const timePerBar = resolution * 1000 * 60

  const msResolution = resolution * 1000 * 60
  const flooredFrom = Math.floor(from / msResolution) * msResolution
  const flooredTo = Math.floor(to / msResolution) * msResolution

  for (let time = flooredTo; time >= flooredFrom; time -= timePerBar) {
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
