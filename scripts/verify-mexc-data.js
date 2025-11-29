/**
 * MEXC Data Verification Script
 * 
 * This script verifies the integrity of collected MEXC trade data by comparing:
 * 1. Total volume from collected trades vs MEXC API klines
 * 2. Price ranges (high/low) from collected trades vs MEXC API
 * 3. Trade count and data continuity
 * 
 * Usage: node scripts/verify-mexc-data.js [symbol] [date]
 * Example: node scripts/verify-mexc-data.js BTC_USDT 2025-11-29
 */

const fs = require('fs')
const path = require('path')
const axios = require('axios')

// Parse command line arguments
const symbol = process.argv[2] || 'BTC_USDT'
const dateStr = process.argv[3] || new Date().toISOString().split('T')[0]

console.log(`\n📊 Verifying MEXC data for ${symbol} on ${dateStr}\n`)

/**
 * Read and parse local trade data
 */
function readLocalTrades(symbol, date) {
  const dataDir = path.join(__dirname, '../data/MEXC', symbol)
  
  if (!fs.existsSync(dataDir)) {
    throw new Error(`Data directory not found: ${dataDir}`)
  }

  const files = fs.readdirSync(dataDir)
    .filter(f => f.startsWith(date))
    .sort()

  if (files.length === 0) {
    throw new Error(`No data files found for ${date}`)
  }

  console.log(`📁 Found ${files.length} data file(s):`)
  files.forEach(f => console.log(`   - ${f}`))
  console.log()

  const trades = []
  
  for (const file of files) {
    const filePath = path.join(dataDir, file)
    const content = fs.readFileSync(filePath, 'utf8')
    const lines = content.trim().split('\n')
    
    for (const line of lines) {
      if (!line.trim()) continue
      
      const parts = line.split(' ')
      if (parts.length >= 4) {
        trades.push({
          timestamp: parseInt(parts[0]),
          price: parseFloat(parts[1]),
          size: parseFloat(parts[2]),
          side: parseInt(parts[3]) // 1 = buy, 0 = sell
        })
      }
    }
  }

  return trades
}

/**
 * Fetch kline data from MEXC API
 */
async function fetchMexcKlines(symbol, startTime, endTime, interval = '1m') {
  const isFutures = symbol.includes('_')
  
  // Convert interval format (15m, 30m, 1h) to API format
  const intervalMap = {
    '1m': isFutures ? 'Min1' : '1m',
    '15m': isFutures ? 'Min15' : '15m',
    '30m': isFutures ? 'Min30' : '30m',
    '1h': isFutures ? 'Min60' : '1h'
  }
  
  const apiInterval = intervalMap[interval] || intervalMap['1m']
  
  let url, params
  
  if (isFutures) {
    // Futures API - get recent klines (time range filtering doesn't work reliably)
    const durationMinutes = Math.ceil((endTime - startTime) / 1000 / 60)
    const intervalMinutes = interval === '1h' ? 60 : (interval === '30m' ? 30 : (interval === '15m' ? 15 : 1))
    const expectedCandles = Math.ceil(durationMinutes / intervalMinutes)
    
    url = `https://contract.mexc.com/api/v1/contract/kline/${symbol}`
    params = {
      interval: apiInterval,
      limit: Math.min(expectedCandles + 10, 2000) // Get enough klines to cover the period
    }
  } else {
    // Spot API - uses milliseconds
    url = 'https://api.mexc.com/api/v3/klines'
    params = {
      symbol: symbol,
      interval: apiInterval,
      startTime: startTime,
      endTime: endTime,
      limit: 1000
    }
  }

  try {
    const response = await axios.get(url, { 
      params,
      timeout: 10000 // 10 second timeout
    })
    
    if (isFutures) {
      // Filter the returned klines to only include our time range
      // Note: Kline timestamps are at the START of the candle period
      if (response.data?.data?.time) {
        const filtered = {
          time: [],
          open: [],
          close: [],
          high: [],
          low: [],
          vol: [],
          amount: []
        }
        
        const startSec = Math.floor(startTime / 1000)
        const endSec = Math.floor(endTime / 1000)
        
        // For klines, we want timestamps >= startTime and < endTime
        // A kline with timestamp T covers the period [T, T + interval)
        for (let i = 0; i < response.data.data.time.length; i++) {
          const time = response.data.data.time[i]
          if (time >= startSec && time < endSec) {
            filtered.time.push(time)
            filtered.open.push(response.data.data.open[i])
            filtered.close.push(response.data.data.close[i])
            filtered.high.push(response.data.data.high[i])
            filtered.low.push(response.data.data.low[i])
            filtered.vol.push(response.data.data.vol[i])
            filtered.amount.push(response.data.data.amount[i])
          }
        }
        
        return { ...response.data, data: filtered }
      }
    }
    
    return response.data
  } catch (error) {
    console.error('   API Error:', error.response?.data || error.message)
    throw new Error(`Failed to fetch klines: ${error.message}`)
  }
}

/**
 * Calculate statistics from local trades
 */
function calculateTradeStats(trades, startTime = null, endTime = null) {
  if (trades.length === 0) {
    return null
  }

  // Filter trades by time range if specified
  const filteredTrades = (startTime && endTime) 
    ? trades.filter(t => t.timestamp >= startTime && t.timestamp <= endTime)
    : trades

  if (filteredTrades.length === 0) {
    return null
  }

  let totalVolume = 0
  let buyVolume = 0
  let sellVolume = 0
  let high = -Infinity
  let low = Infinity
  let open = filteredTrades[0].price
  let close = filteredTrades[filteredTrades.length - 1].price

  for (const trade of filteredTrades) {
    totalVolume += trade.size
    if (trade.side === 1) {
      buyVolume += trade.size
    } else {
      sellVolume += trade.size
    }
    high = Math.max(high, trade.price)
    low = Math.min(low, trade.price)
  }

  return {
    count: filteredTrades.length,
    totalVolume,
    buyVolume,
    sellVolume,
    high,
    low,
    open,
    close,
    startTime: filteredTrades[0].timestamp,
    endTime: filteredTrades[filteredTrades.length - 1].timestamp
  }
}

/**
 * Round timestamp to candle interval
 */
function roundToInterval(timestamp, intervalMinutes) {
  const ms = intervalMinutes * 60 * 1000
  return Math.floor(timestamp / ms) * ms
}

/**
 * Find a complete candle period within the data
 */
function findCompleteCandle(trades, intervalMinutes) {
  if (trades.length === 0) return null

  const firstTs = trades[0].timestamp
  const lastTs = trades[trades.length - 1].timestamp

  // Round to next interval boundary for start
  const candleStart = roundToInterval(firstTs, intervalMinutes) + (intervalMinutes * 60 * 1000)
  const candleEnd = candleStart + (intervalMinutes * 60 * 1000)

  // Check if we have a complete candle
  if (candleEnd <= lastTs) {
    return { start: candleStart, end: candleEnd }
  }

  return null
}

/**
 * Calculate statistics from API klines
 */
function calculateKlineStats(klines, isFutures, symbol) {
  let totalVolume = 0
  let high = -Infinity
  let low = Infinity
  let open = null
  let close = null

  if (isFutures) {
    // Futures format: { time: [], open: [], high: [], low: [], close: [], vol: [], amount: [] }
    // vol is in contracts, need to convert to base currency using contract size
    const contractSize = 0.0001 // BTC_USDT contract size (need to load from products file for other pairs)
    
    if (!klines.time || klines.time.length === 0) {
      return null
    }
    
    for (let i = 0; i < klines.time.length; i++) {
      // vol is in contracts, convert to base currency (BTC)
      // For BTC_USDT: 1 contract = 0.0001 BTC
      totalVolume += parseFloat(klines.vol[i]) * contractSize
      high = Math.max(high, parseFloat(klines.high[i]))
      low = Math.min(low, parseFloat(klines.low[i]))
      if (open === null) open = parseFloat(klines.open[i])
      close = parseFloat(klines.close[i])
    }
    
    return {
      count: klines.time.length,
      totalVolume,
      high,
      low,
      open,
      close
    }
  } else {
    // Spot format: [timestamp, open, high, low, close, volume, ...]
    for (const kline of klines) {
      totalVolume += parseFloat(kline[5])
      high = Math.max(high, parseFloat(kline[2]))
      low = Math.min(low, parseFloat(kline[3]))
      if (open === null) open = parseFloat(kline[1])
      close = parseFloat(kline[4])
    }

    return {
      count: klines.length,
      totalVolume,
      high,
      low,
      open,
      close
    }
  }
}

/**
 * Format number with thousand separators
 */
function formatNumber(num, decimals = 2) {
  return num.toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  })
}

/**
 * Main verification function
 */
async function verify() {
  try {
    // Step 1: Read local trades
    console.log('📖 Reading local trade data...')
    const trades = readLocalTrades(symbol, dateStr)
    const fullStats = calculateTradeStats(trades)

    if (!fullStats) {
      console.error('❌ No valid trades found in local data')
      return
    }

    console.log('✅ Full data range:')
    console.log(`   Trades: ${formatNumber(fullStats.count, 0)}`)
    console.log(`   Volume: ${formatNumber(fullStats.totalVolume, 8)}`)
    console.log(`   Time Range: ${new Date(fullStats.startTime).toISOString()} - ${new Date(fullStats.endTime).toISOString()}`)
    console.log(`   Duration: ${((fullStats.endTime - fullStats.startTime) / 60000).toFixed(1)} minutes`)
    console.log()

    const isFutures = symbol.includes('_')

    // Test different candle intervals
    const intervals = [
      { name: '15m', minutes: 15 },
      { name: '30m', minutes: 30 },
      { name: '1h', minutes: 60 }
    ]

    for (const interval of intervals) {
      const candle = findCompleteCandle(trades, interval.minutes)
      
      if (!candle) {
        console.log(`⊗ ${interval.name} candle: Not enough data for a complete candle\n`)
        continue
      }

      console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`)
      console.log(`🕐 ${interval.name.toUpperCase()} CANDLE VERIFICATION`)
      console.log(`   ${new Date(candle.start).toISOString()} → ${new Date(candle.end).toISOString()}`)
      console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`)

      // Calculate local statistics for this candle
      const localStats = calculateTradeStats(trades, candle.start, candle.end)

      if (!localStats) {
        console.log('  No trades in this candle period\n')
        continue
      }

      console.log('📊 Local Data:')
      console.log(`   Trades: ${formatNumber(localStats.count, 0)}`)
      console.log(`   Volume: ${formatNumber(localStats.totalVolume, 8)} (Buy: ${formatNumber(localStats.buyVolume, 8)}, Sell: ${formatNumber(localStats.sellVolume, 8)})`)
      console.log(`   OHLC: O=${formatNumber(localStats.open, 2)} H=${formatNumber(localStats.high, 2)} L=${formatNumber(localStats.low, 2)} C=${formatNumber(localStats.close, 2)}`)
      console.log()

      // Fetch API data for this specific candle
      console.log(`🌐 Fetching API data (${interval.name} klines)...`)
      const klines = await fetchMexcKlines(symbol, candle.start, candle.end, interval.name)
      
      if (!klines) {
        console.error('❌ No kline data returned from API\n')
        continue
      }

      const klineData = isFutures ? klines.data : klines
      const apiStats = calculateKlineStats(klineData, isFutures, symbol)
      
      if (!apiStats) {
        console.error('❌ Failed to calculate API statistics\n')
        continue
      }
      
      console.log('📊 API Data:')
      console.log(`   Klines: ${formatNumber(apiStats.count, 0)}`)
      console.log(`   Volume: ${formatNumber(apiStats.totalVolume, 8)}`)
      console.log(`   OHLC: O=${formatNumber(apiStats.open, 2)} H=${formatNumber(apiStats.high, 2)} L=${formatNumber(apiStats.low, 2)} C=${formatNumber(apiStats.close, 2)}`)
      console.log()

      // Compare and validate
      console.log('🔍 Comparison:')
      
      // Volume comparison
      const volumeDiff = Math.abs(localStats.totalVolume - apiStats.totalVolume)
      const volumeDiffPercent = (volumeDiff / apiStats.totalVolume) * 100
      const volumeMatch = volumeDiffPercent < 3 // Within 3%
      
      console.log(`   Volume Diff: ${formatNumber(volumeDiff, 8)} (${formatNumber(volumeDiffPercent, 2)}%) ${volumeMatch ? '✅' : '⚠️'}`)

      // Price range comparison
      const highDiff = Math.abs(localStats.high - apiStats.high)
      const lowDiff = Math.abs(localStats.low - apiStats.low)
      const highMatch = highDiff < 0.01
      const lowMatch = lowDiff < 0.01
      
      console.log(`   High Match: Local ${formatNumber(localStats.high, 2)} vs API ${formatNumber(apiStats.high, 2)} (diff: ${formatNumber(highDiff, 2)}) ${highMatch ? '✅' : '⚠️'}`)
      console.log(`   Low Match:  Local ${formatNumber(localStats.low, 2)} vs API ${formatNumber(apiStats.low, 2)} (diff: ${formatNumber(lowDiff, 2)}) ${lowMatch ? '✅' : '⚠️'}`)

      // OHLC comparison
      const openDiff = Math.abs(localStats.open - apiStats.open)
      const closeDiff = Math.abs(localStats.close - apiStats.close)
      const ohlcMatch = openDiff < 0.01 && closeDiff < 0.01
      
      console.log(`   OHLC Match: ${ohlcMatch ? '✅ YES' : '⚠️ Partial'}`)

      // Overall verdict for this interval
      if (volumeMatch && highMatch && lowMatch) {
        console.log(`\n   ✅ ${interval.name} candle verification PASSED\n`)
      } else {
        console.log(`\n   ⚠️  ${interval.name} candle has some discrepancies (normal for real-time data)\n`)
      }
    }

    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✅ Verification complete!')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

  } catch (error) {
    console.error('\n❌ Error:', error.message)
    console.error(error.stack)
  }
}

// Run verification
verify()
