const config = require('../../src/config')
const { prepareStandalone, getMarkets, getHms } = require('../../src/helper')
const { COINALIZE_VENUES, COINALIZE_RESOLUTIONS, getReqKey, getAllData } = require('./coinalizeUtils')

console.log('PID: ', process.pid)

async function program() {
  const { exchanges, storage } = await prepareStandalone()

  const influxMeasurement = 'trades_' + getHms(config.resolution)
  const influxRP = config.influxRetentionPrefix + getHms(config.resolution)
  const coinalizeResolution = config.resolution / (1000 * 60)

  const pairs = getMarkets()

  console.log('Extend historical')
  console.log('from', new Date(config.from).toISOString())
  console.log('to', new Date(config.to).toISOString())
  console.log('on', pairs.map((a) => a.market).join(', '))

  if (COINALIZE_RESOLUTIONS.indexOf(coinalizeResolution) === -1) {
    throw new Error('unavailable resolution')
  }

  await getReqKey()

  for (const pair of pairs) {
    const exchange = exchanges.find((exchange) => exchange.id === pair.exchange)

    if (!exchange) {
      continue
    }

    console.log(`[${pair.market}] start extend procedure`)

    let symbol = pair.symbol.replace(/\//, '').toUpperCase()

    if (pair.exchange === 'BINANCE_FUTURES' && !/_PERP$/.test(symbol)) {
      symbol += '_PERP'
    }

    const coinalizeMarket = symbol + '.' + COINALIZE_VENUES[exchange.id.replace(/_\w+/, '')]

    const data = await getAllData(config.from, config.to, coinalizeResolution, coinalizeMarket)

    const points = []
    for (let i = data.length - 1; i >= 0; i--) {
      const bar = data[i]
      const fields = {
        cbuy: bar.cbuy,
        csell: bar.csell,
        // vbuy: exchange.getSize(bar.vbuy, bar.close, pair.symbol),
        vbuy: bar.vbuy * bar.close,
        vsell: bar.vsell * bar.close,
        lbuy: bar.lbuy,
        lsell: bar.lsell,
      }

      if (typeof bar.close !== 'undefined' && bar.close !== null) {
        ;(fields.open = bar.open), (fields.high = bar.high), (fields.low = bar.low), (fields.close = bar.close)
      }

      points.push({
        measurement: influxMeasurement,
        tags: {
          market: pair.market,
        },
        fields: fields,
        timestamp: +bar.time,
      })
    }

    await storage.writePoints(points, {
      precision: 'ms',
      retentionPolicy: influxRP,
    })

    console.log(`[${pair.market}] end recovery procedure (${data.length} bars)`)
  }
}

program()
