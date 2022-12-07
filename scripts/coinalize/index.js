const config = require('../../src/config')
const { prepareStandalone, getMarkets, getHms, formatAmount, sleep } = require('../../src/helper')
const { parseMarket } = require('../../src/services/catalog')
const { getReqKey, getAllData } = require('./utils')

console.log('PID: ', process.pid)

async function program() {
  const { exchanges, storage } = await prepareStandalone(false)

  const influxMeasurement = 'trades_' + getHms(config.timeframe)
  const influxRP = config.influxRetentionPrefix + getHms(config.timeframe)

  const pairs = getMarkets()

  console.log('Extend historical')
  console.log('from', new Date(config.from).toISOString())
  console.log('to', new Date(config.to).toISOString())
  console.log('on', pairs.map((a) => a.market).join(', '))

  const resampleRange = {
    from: Infinity,
    to: -Infinity,
    markets: [],
    points: 0,
  }

  for (const pair of pairs) {
    const exchange = exchanges.find((exchange) => exchange.id === pair.exchange)

    if (!exchange) {
      continue
    }

    const market = parseMarket(pair.market, pair.exchange === 'POLONIEX')
    
    resampleRange.markets.push(market.id)

    if ((market.exchange === 'OKEX' || market.exchange === 'HUOBI') && market.type === 'spot') {
      continue
    }

    const data = await getAllData(market, config.from, config.to, config.timeframe)

    const points = []
    let totalVbuy = 0
    let totalVsell = 0
    let totalCbuy = 0
    let totalCsell = 0
    let totalLbuy = 0
    let totalLsell = 0
    for (let i = data.length - 1; i >= 0; i--) {
      const bar = data[i]
      const hl3 = (bar.high + bar.low + bar.close) / 3
      let scaleAmount = true
      if (
        pair.exchange === 'BITMEX' ||
        (pair.exchange === 'OKEX' && market.quote === 'USD') ||
        (pair.exchange === 'BINANCE_FUTURES' && market.quote === 'USD') ||
        (pair.exchange === 'HUOBI' && market.quote === 'USD') ||
        (pair.exchange === 'KRAKEN' && market.quote === 'USD' && market.type === 'perp') ||
        (pair.exchange === 'DERIBIT' && market.quote === 'USD') ||
        (pair.exchange === 'BYBIT' && market.quote === 'USD')
      ) {
        scaleAmount = false
      }

      let empty = true

      const fields = {}

      if (bar.vsell || bar.vbuy) {
        fields.vsell = bar.vsell * (scaleAmount ? hl3 : 1)
        fields.vbuy = bar.vbuy * (scaleAmount ? hl3 : 1)
        empty = false
      }

      if (pair.exchange === 'OKEX' && market.quote === 'USDT') {
        scaleAmount = true
      }

      if (typeof bar.close === 'number') {
        fields.open = bar.open
        fields.high = bar.high
        fields.low = bar.low
        fields.close = bar.close
        empty = false
      }

      if (bar.csell || bar.cbuy) {
        fields.csell = bar.csell
        fields.cbuy = bar.cbuy
        empty = false
      }

      if (bar.lsell || bar.lbuy) {
        fields.lsell = bar.lsell * (scaleAmount ? hl3 : 1)
        fields.lbuy = bar.lbuy * (scaleAmount ? hl3 : 1)
        empty = false
      } else {
        fields.lsell = fields.lbuy = 0
      }

      if (!empty) {
        totalVbuy += fields.vbuy ? fields.vbuy : 0
        totalVsell += fields.vsell ? fields.vsell : 0
        totalCbuy += fields.cbuy ? fields.cbuy : 0
        totalCsell += fields.csell ? fields.csell : 0
        totalLbuy += fields.lbuy ? fields.lbuy : 0
        totalLsell += fields.lsell ? fields.lsell : 0

        points.push({
          measurement: influxMeasurement,
          tags: {
            market: pair.market,
          },
          fields: fields,
          timestamp: +bar.time,
        })

        resampleRange.from = Math.min(resampleRange.from, bar.time)
        resampleRange.to = Math.max(resampleRange.to, bar.time)
      }
    }

    console.log(
      market.id,
      points.length + 'points',
      '\n-vbuy',
      totalVbuy ? formatAmount(totalVbuy) : 'n/a',
      '\n-vsell',
      totalVsell ? formatAmount(totalVsell) : 'n/a',
      '\n-lbuy',
      totalLbuy ? formatAmount(totalLbuy) : 'n/a',
      '\n-lsell',
      totalLsell ? formatAmount(totalLsell) : 'n/a',
      '\n-cbuy',
      totalCbuy ? formatAmount(totalCbuy) : 'n/a',
      '\n-csell',
      totalCsell ? formatAmount(totalCsell) : 'n/a' + '\n\n'
    )

    console.log('-> write ' + points.length + ' points')

    if (points.length) {
      await storage.writePoints(points, {
        precision: 'ms',
        retentionPolicy: influxRP,
      })
    }

    resampleRange.points += points.length

    console.log(`[${pair.market}] end recovery procedure (${data.length} bars)`)

    await sleep(Math.random() * 3000 + 3000)
  }

  if (resampleRange.points) {
    await storage.resample(resampleRange, config.timeframe)
    console.log(`resampled ${resampleRange.markets.length} market${resampleRange.markets.length > 1 ? 's' : ''} from timeframe ${getHms(config.timeframe)}`)
  }
}

program()
