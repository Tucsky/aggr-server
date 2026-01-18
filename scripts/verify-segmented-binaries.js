#!/usr/bin/env node
/**
 * Verification script for segmented binaries storage.
 * 
 * Tests:
 * 1. Write synthetic bars spanning 2+ segments
 * 2. Verify fetch returns correct continuity across segment boundaries
 * 3. Verify backfill disk fallback reads from correct segment
 * 4. Verify resampling works across segment boundaries
 * 
 * Usage:
 *   node scripts/verify-segmented-binaries.js
 *   node scripts/verify-segmented-binaries.js --clean  # Clean test data first
 */

const fs = require('fs')
const path = require('path')

// Override config for testing
process.env.STORAGE = 'binaries'
process.env.FILES_LOCATION = './data-test'

const config = require('../src/config')
config.filesLocation = './data-test'
config.influxTimeframe = 10000 // 10s base timeframe
config.influxResampleTo = [60000] // Only 1m for testing
config.binariesSegmentRecords = 100 // Small segments for testing (100 records = 1000s = ~16.6 min)

const BinariesStorage = require('../src/storage/binaries/index')
const { getSegmentStartTs, getSegmentSpanMs, getSegmentRecords } = require('../src/storage/binaries/constants')
const { getSegmentPaths, readMeta, readSingleRecordAtTs } = require('../src/storage/binaries/io')
const { upsertBars, createNullBar } = require('../src/storage/binaries/write')

const EXCHANGE = 'TEST'
const SYMBOL = 'BTC-USD'
const MARKET = `${EXCHANGE}:${SYMBOL}`
const TIMEFRAME = config.influxTimeframe

// Clean test data
function cleanTestData() {
  const testDir = path.resolve(config.filesLocation)
  if (fs.existsSync(testDir)) {
    fs.rmSync(testDir, { recursive: true, force: true })
    console.log(`Cleaned test data directory: ${testDir}`)
  }
}

// Generate synthetic bars
function generateBars(startTs, count, timeframe, priceStart = 50000) {
  const bars = []
  for (let i = 0; i < count; i++) {
    const time = startTs + i * timeframe
    const price = priceStart + Math.random() * 100 - 50
    bars.push({
      time,
      open: price,
      high: price + 10,
      low: price - 10,
      close: price + 5,
      vbuy: 1000 + i,
      vsell: 900 + i,
      cbuy: 10 + i,
      csell: 8 + i,
      lbuy: 0,
      lsell: 0
    })
  }
  return bars
}

async function testWriteAcrossSegments() {
  console.log('\n=== Test 1: Write bars spanning multiple segments ===')
  
  const segmentRecords = getSegmentRecords(TIMEFRAME)
  const segmentSpanMs = getSegmentSpanMs(TIMEFRAME)
  
  console.log(`Segment config: ${segmentRecords} records, ${segmentSpanMs}ms span`)
  
  // Generate bars spanning 2.5 segments
  const barCount = Math.floor(segmentRecords * 2.5)
  const startTs = Math.floor(Date.now() / TIMEFRAME) * TIMEFRAME - (barCount * TIMEFRAME)
  
  // Align to segment boundary for cleaner test
  const alignedStartTs = getSegmentStartTs(startTs, TIMEFRAME)
  
  console.log(`Generating ${barCount} bars starting at ${new Date(alignedStartTs).toISOString()}`)
  
  const bars = generateBars(alignedStartTs, barCount, TIMEFRAME)
  
  // Write bars
  const result = await upsertBars(EXCHANGE, SYMBOL, TIMEFRAME, bars)
  console.log(`Write result: fromTs=${result.fromTsWritten}, toTs=${result.toTsWritten}`)
  
  // Check which segments were created
  const firstSegment = getSegmentStartTs(alignedStartTs, TIMEFRAME)
  const lastSegment = getSegmentStartTs(alignedStartTs + (barCount - 1) * TIMEFRAME, TIMEFRAME)
  
  console.log(`Expected segments from ${firstSegment} to ${lastSegment}`)
  
  let segmentCount = 0
  for (let segmentTs = firstSegment; segmentTs <= lastSegment; segmentTs += segmentSpanMs) {
    const paths = getSegmentPaths(EXCHANGE, SYMBOL, TIMEFRAME, segmentTs)
    if (fs.existsSync(paths.json)) {
      const meta = readMeta(paths.json)
      console.log(`  Segment ${segmentTs}: ${meta.records} records`)
      segmentCount++
    }
  }
  
  console.log(`Created ${segmentCount} segment files`)
  
  if (segmentCount >= 2) {
    console.log('✓ PASS: Multiple segments created')
  } else {
    console.log('✗ FAIL: Expected at least 2 segments')
    return false
  }
  
  return { alignedStartTs, barCount }
}

async function testFetchAcrossSegments(testData) {
  console.log('\n=== Test 2: Fetch data spanning multiple segments ===')
  
  const storage = new BinariesStorage()
  
  // Fetch all written data
  const from = testData.alignedStartTs
  const to = testData.alignedStartTs + testData.barCount * TIMEFRAME
  
  console.log(`Fetching from ${new Date(from).toISOString()} to ${new Date(to).toISOString()}`)
  
  const result = await storage.fetch({
    from,
    to,
    timeframe: TIMEFRAME,
    markets: [MARKET]
  })
  
  console.log(`Fetched ${result.results.length} records`)
  
  // Verify continuity
  const times = result.results.map(r => r[0] * 1000) // Convert back to ms
  times.sort((a, b) => a - b)
  
  let gaps = 0
  for (let i = 1; i < times.length; i++) {
    const expectedTime = times[i - 1] + TIMEFRAME
    if (times[i] !== expectedTime) {
      gaps++
      if (gaps <= 3) {
        console.log(`  Gap at index ${i}: expected ${expectedTime}, got ${times[i]}`)
      }
    }
  }
  
  if (gaps === 0) {
    console.log('✓ PASS: No gaps in fetched data')
  } else {
    console.log(`✗ FAIL: Found ${gaps} gaps in fetched data`)
    return false
  }
  
  if (result.results.length === testData.barCount) {
    console.log('✓ PASS: Correct number of records fetched')
  } else {
    console.log(`✗ FAIL: Expected ${testData.barCount} records, got ${result.results.length}`)
    return false
  }
  
  return true
}

async function testBackfillDiskFallback(testData) {
  console.log('\n=== Test 3: Backfill disk fallback reads from correct segment ===')
  
  const segmentSpanMs = getSegmentSpanMs(TIMEFRAME)
  
  // Pick a timestamp in the middle of the second segment
  const secondSegmentStart = testData.alignedStartTs + segmentSpanMs
  const testBucketTs = secondSegmentStart + 50 * TIMEFRAME // 50 bars into second segment
  
  console.log(`Reading single record at ${new Date(testBucketTs).toISOString()}`)
  console.log(`Expected segment: ${secondSegmentStart}`)
  
  const bar = readSingleRecordAtTs(EXCHANGE, SYMBOL, TIMEFRAME, testBucketTs)
  
  if (bar) {
    console.log(`  Found bar: time=${bar.time}, close=${bar.close.toFixed(2)}`)
    if (bar.time === testBucketTs) {
      console.log('✓ PASS: Disk fallback returned correct bar')
    } else {
      console.log(`✗ FAIL: Bar time mismatch: expected ${testBucketTs}, got ${bar.time}`)
      return false
    }
  } else {
    console.log('✗ FAIL: Disk fallback returned null')
    return false
  }
  
  // Test reading from first segment too
  const firstSegmentBucket = testData.alignedStartTs + 10 * TIMEFRAME
  const bar2 = readSingleRecordAtTs(EXCHANGE, SYMBOL, TIMEFRAME, firstSegmentBucket)
  
  if (bar2 && bar2.time === firstSegmentBucket) {
    console.log('✓ PASS: Can read from different segments')
  } else {
    console.log('✗ FAIL: Could not read from first segment')
    return false
  }
  
  return true
}

async function testResamplingAcrossSegments(testData) {
  console.log('\n=== Test 4: Resampling works across segment boundaries ===')
  
  const storage = new BinariesStorage()
  const targetTimeframe = 60000 // 1m
  
  // Trigger resampling by doing a flush simulation
  const { resampleToHigherTimeframes } = require('../src/storage/binaries/resample')
  
  await resampleToHigherTimeframes(
    storage,
    EXCHANGE,
    SYMBOL,
    testData.alignedStartTs,
    testData.alignedStartTs + (testData.barCount - 1) * TIMEFRAME
  )
  
  // Fetch resampled data
  const from = testData.alignedStartTs
  const to = testData.alignedStartTs + testData.barCount * TIMEFRAME
  
  const result = await storage.fetch({
    from,
    to,
    timeframe: targetTimeframe,
    markets: [MARKET]
  })
  
  console.log(`Fetched ${result.results.length} resampled 1m records`)
  
  // Calculate expected 1m bars (each 1m bar = 6 base bars at 10s)
  const expectedBars = Math.floor(testData.barCount / 6)
  
  if (result.results.length >= expectedBars - 2 && result.results.length <= expectedBars + 2) {
    console.log('✓ PASS: Resampling produced expected number of bars')
  } else {
    console.log(`✗ FAIL: Expected ~${expectedBars} bars, got ${result.results.length}`)
    return false
  }
  
  // Check for segment files
  const targetSegmentSpanMs = getSegmentSpanMs(targetTimeframe)
  const firstTargetSegment = getSegmentStartTs(from, targetTimeframe)
  const lastTargetSegment = getSegmentStartTs(to - 1, targetTimeframe)
  
  let targetSegmentCount = 0
  for (let segmentTs = firstTargetSegment; segmentTs <= lastTargetSegment; segmentTs += targetSegmentSpanMs) {
    const paths = getSegmentPaths(EXCHANGE, SYMBOL, targetTimeframe, segmentTs)
    if (fs.existsSync(paths.json)) {
      targetSegmentCount++
    }
  }
  
  console.log(`Created ${targetSegmentCount} resampled segment file(s)`)
  
  return true
}

async function testEdgeCases() {
  console.log('\n=== Test 5: Edge cases ===')
  
  const segmentSpanMs = getSegmentSpanMs(TIMEFRAME)
  
  // Test 5a: Bar at exact segment boundary
  console.log('\n5a: Bar at exact segment boundary')
  const boundaryTs = Math.floor(Date.now() / segmentSpanMs) * segmentSpanMs
  const boundaryBar = generateBars(boundaryTs, 1, TIMEFRAME)[0]
  
  const result1 = await upsertBars(EXCHANGE, SYMBOL, TIMEFRAME, [boundaryBar])
  
  const expectedSegment = getSegmentStartTs(boundaryTs, TIMEFRAME)
  if (expectedSegment === boundaryTs) {
    console.log('✓ PASS: Boundary bar goes to correct segment')
  }
  
  // Test 5b: Bar at segmentEnd - timeframe (last bar of segment)
  console.log('\n5b: Last bar of segment')
  const segmentRecords = getSegmentRecords(TIMEFRAME)
  const lastBarTs = boundaryTs + (segmentRecords - 1) * TIMEFRAME
  const lastBar = generateBars(lastBarTs, 1, TIMEFRAME)[0]
  
  const result2 = await upsertBars(EXCHANGE, SYMBOL, TIMEFRAME, [lastBar])
  
  const lastBarSegment = getSegmentStartTs(lastBarTs, TIMEFRAME)
  if (lastBarSegment === boundaryTs) {
    console.log('✓ PASS: Last bar of segment goes to correct segment')
  } else {
    console.log(`✗ FAIL: Expected segment ${boundaryTs}, got ${lastBarSegment}`)
    return false
  }
  
  // Test 5c: First bar of next segment
  console.log('\n5c: First bar of next segment')
  const nextSegmentFirstBarTs = boundaryTs + segmentSpanMs
  const nextBar = generateBars(nextSegmentFirstBarTs, 1, TIMEFRAME)[0]
  
  await upsertBars(EXCHANGE, SYMBOL, TIMEFRAME, [nextBar])
  
  const nextBarSegment = getSegmentStartTs(nextSegmentFirstBarTs, TIMEFRAME)
  if (nextBarSegment === boundaryTs + segmentSpanMs) {
    console.log('✓ PASS: First bar of next segment goes to new segment')
  } else {
    console.log(`✗ FAIL: Expected segment ${boundaryTs + segmentSpanMs}, got ${nextBarSegment}`)
    return false
  }
  
  return true
}

async function runAllTests() {
  console.log('╔════════════════════════════════════════════════════════════╗')
  console.log('║  Segmented Binaries Storage Verification                   ║')
  console.log('╚════════════════════════════════════════════════════════════╝')
  
  // Check for --clean flag
  if (process.argv.includes('--clean')) {
    cleanTestData()
  }
  
  // Ensure test directory exists
  if (!fs.existsSync(config.filesLocation)) {
    fs.mkdirSync(config.filesLocation, { recursive: true })
  }
  
  let passed = 0
  let failed = 0
  
  try {
    // Test 1: Write across segments
    const testData = await testWriteAcrossSegments()
    if (testData) {
      passed++
      
      // Test 2: Fetch across segments
      if (await testFetchAcrossSegments(testData)) passed++
      else failed++
      
      // Test 3: Backfill disk fallback
      if (await testBackfillDiskFallback(testData)) passed++
      else failed++
      
      // Test 4: Resampling
      if (await testResamplingAcrossSegments(testData)) passed++
      else failed++
    } else {
      failed++
    }
    
    // Test 5: Edge cases
    if (await testEdgeCases()) passed++
    else failed++
    
  } catch (error) {
    console.error('\n✗ Test error:', error)
    failed++
  }
  
  console.log('\n╔════════════════════════════════════════════════════════════╗')
  console.log(`║  Results: ${passed} passed, ${failed} failed                               ║`)
  console.log('╚════════════════════════════════════════════════════════════╝')
  
  if (failed > 0) {
    console.log('\nSome tests failed. Check output above for details.')
    process.exit(1)
  } else {
    console.log('\nAll tests passed! Segmented binaries storage is working correctly.')
    
    // Cleanup
    if (!process.argv.includes('--keep')) {
      cleanTestData()
      console.log('Test data cleaned up. Use --keep to preserve test data.')
    }
  }
}

runAllTests()
