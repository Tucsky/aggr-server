const Influx = require('influx')
const { getHms, sleep, ID } = require('../helper')
const net = require('net')
const { statSync, unlinkSync } = require('fs')

require('../typedef')

const DAY = 1000 * 60 * 60 * 24

class InfluxStorage {
  constructor(options) {
    this.name = this.constructor.name
    this.format = 'point'

    /**
     * @type {net.Socket}
     */
    this.clusterSocket = null

    /**
     * @type {net.Server}
     */
    this.serverSocket = null

    /**
     * @type {net.Socket[]}
     */
    this.clusteredCollectors = []

    /**
     * @type {{[requestId: string]: (bars: Bar[]) => void}}
     */
    this.promisesOfRealtimeBars = {}

    /**
     * @type {{[identifier: string]: number}}
     */
    this.lastClose = {}

    /**
     * @type {{[identifier: string]: Bar}}
     */
    this.lastBar = {}

    /**
     * @type {{[identifier: string]: Bar[]}}
     */
    this.realtimeBars = {}

    this.options = options
  }

  async connect() {
    if (/\-/.test(this.options.influxDatabase)) {
      throw new Error('dashes not allowed inside influxdb database')
    }

    let host = this.options.influxHost
    let port = this.options.influxPort

    if (typeof this.options.influxUrl === 'string' && this.options.influxUrl.length) {
      ;[host, port] = this.options.influxUrl.split(':')
    }

    console.log(`[storage/influx] connecting to ${host}:${port}`)

    try {
      this.influx = new Influx.InfluxDB({
        host: host || 'localhost',
        port: port || '8086',
        database: this.options.influxDatabase,
      })

      const databases = await this.influx.getDatabaseNames()

      if (!databases.includes(this.options.influxDatabase)) {
        await this.influx.createDatabase(this.options.influxDatabase)
      }

      if (this.options.collect) {
        await this.ensureRetentionPolicies()
        await this.getPreviousCloses()
      }
    } catch (error) {
      console.error(`[storage/influx] ${error.message}... retrying in 1s`)

      await sleep()

      return this.connect()
    } finally {
      if (this.options.influxCollectors) {
        if (this.options.api && !this.options.collect) {
          // CLUSTER NODE (node is dedicated to serving data)
          this.createCluster()
        } else if (!this.options.api && this.options.collect) {
          // COLLECTOR NODE (node is just collecting + storing data)
          this.connectToCluster()
        }
      }
    }
  }

  /**
   *
   */
  async ensureRetentionPolicies() {
    const retentionsPolicies = (await this.influx.showRetentionPolicies()).reduce((output, retentionPolicy) => {
      output[retentionPolicy.name] = retentionPolicy.duration
      return output
    }, {})

    const timeframes = [this.options.influxTimeframe].concat(this.options.influxResampleTo)

    for (let timeframe of timeframes) {
      const rpDuration = timeframe * this.options.influxRetentionPerTimeframe // 5000 bars
      const rpDurationLitteral = getHms(rpDuration, true)
      const rpName = this.options.influxRetentionPrefix + getHms(timeframe)

      if (!retentionsPolicies[rpName]) {
        console.log(`[storage/influx] create retention policy ${rpName} (duration ${rpDurationLitteral})`)
        await this.influx.createRetentionPolicy(rpName, {
          database: this.options.influxDatabase,
          duration: rpDurationLitteral,
          replication: 1,
        })
      }

      delete retentionsPolicies[rpName]
    }

    for (let rpName in retentionsPolicies) {
      if (rpName.indexOf(this.options.influxRetentionPrefix) === 0) {
        console.warn(`[storage/influx] unused retention policy ? (${rpName})`)
        // await this.influx.dropRetentionPolicy(rpName, this.options.influxDatabase)
        // just warning now because of multiple instances of aggr-server running with different RPs
      }
    }

    this.baseRp = this.options.influxRetentionPrefix + getHms(this.options.influxTimeframe)
  }

  /**
   * Create continuous queries if not exists
   *
   * @memberof InfluxStorage
   */
  async resample(range) {
    let sourceTimeframeLitteral
    let destinationTimeframeLitteral

    this.options.influxResampleTo.sort((a, b) => a - b)

    for (let timeframe of this.options.influxResampleTo) {
      const isOddTimeframe = DAY % timeframe !== 0 && timeframe < DAY

      let flooredRange

      if (isOddTimeframe) {
        const dayOpen = Math.floor(range.from / DAY) * DAY
        flooredRange = {
          from: dayOpen + Math.floor((range.from - dayOpen) / timeframe) * timeframe,
          to: dayOpen + Math.floor((range.to - dayOpen) / timeframe) * timeframe + timeframe,
        }
      } else {
        flooredRange = {
          from: Math.floor(range.from / timeframe) * timeframe,
          to: Math.floor(range.to / timeframe) * timeframe + timeframe,
        }
      }

      for (let i = this.options.influxResampleTo.indexOf(timeframe); i >= 0; i--) {
        if (timeframe <= this.options.influxResampleTo[i] || timeframe % this.options.influxResampleTo[i] !== 0) {
          if (i === 0) {
            sourceTimeframeLitteral = getHms(this.options.influxTimeframe)
          }
          continue
        }

        sourceTimeframeLitteral = getHms(this.options.influxResampleTo[i])
        break
      }

      destinationTimeframeLitteral = getHms(timeframe)

      const query = `SELECT min(low) AS low, 
      max(high) AS high, 
      first(open) AS open, 
      last(close) AS close, 
      sum(count) AS count, 
      sum(cbuy) AS cbuy, 
      sum(csell) AS csell, 
      sum(lbuy) AS lbuy, 
      sum(lsell) AS lsell, 
      sum(vol) AS vol, 
      sum(vbuy) AS vbuy, 
      sum(vsell) AS vsell`

      const query_from = `${this.options.influxDatabase}.${this.options.influxRetentionPrefix}${sourceTimeframeLitteral}.${this.options.influxMeasurement}_${sourceTimeframeLitteral}`
      const query_into = `${this.options.influxDatabase}.${this.options.influxRetentionPrefix}${destinationTimeframeLitteral}.${this.options.influxMeasurement}_${destinationTimeframeLitteral}`
      const coverage = `WHERE time >= ${flooredRange.from}ms AND time < ${flooredRange.to}ms AND market =~ /${range.markets
        .join('|')
        .replace(/\//g, '\\/')}/`
      const group = `GROUP BY time(${destinationTimeframeLitteral}${
        isOddTimeframe ? ', ' + getHms(flooredRange.from % timeframe) : ''
      }), market fill(none)`

      await this.executeQuery(`${query} INTO ${query_into} FROM ${query_from} ${coverage} ${group}`)
    }
  }

  getPreviousCloses() {
    const timeframeLitteral = getHms(this.options.influxTimeframe)

    this.influx
      .query(
        `SELECT close FROM ${this.options.influxRetentionPrefix}${timeframeLitteral}.${this.options.influxMeasurement}${
          '_' + timeframeLitteral
        } GROUP BY "market" ORDER BY time DESC LIMIT 1`
      )
      .then((data) => {
        for (let bar of data) {
          this.lastClose[bar.market] = bar.close
        }
      })
  }

  /**
   * Trigger when save fired from main controller
   *
   * @param {Trade[]} trades
   * @returns {Promise<any>}
   * @memberof InfluxStorage
   */
  async save(trades, forceImport) {
    if (forceImport) {
      if (this.clusterSocket) {
        console.log('[storage/influx/collector] closing cluster connection')
        await new Promise((resolve) => {
          this.clusterSocket.end(() => {
            console.log('[storage/influx/collector] successfully closed cluster connection')
            resolve()
          })
        })
      }
    }

    if (!trades || !trades.length) {
      return Promise.resolve()
    }

    this.processTrades(trades)

    const now = +new Date()
    const timeBackupFloored = Math.floor(now / this.options.backupInterval) * this.options.backupInterval
    const timeMinuteFloored = Math.floor(now / this.options.influxResampleInterval) * this.options.influxResampleInterval

    if (forceImport || timeBackupFloored === timeMinuteFloored) {
      const resampleRange = await this.importRealtimeBars()
      await this.resample(resampleRange)
    }
  }

  /**
   * close a bar (register close + reference for next bar)
   * @param {Bar} bar
   */
  closeBar(bar) {
    if (typeof bar.close === 'number') {
      // reg close for next bar
      this.lastClose[bar.market] = bar.close
    }

    this.lastBar[bar.market] = bar

    return this.lastBar[bar.market]
  }

  /**
   * Import and clear pending bars
   *
   * @returns {Promise<{
      from: number,
      to: number,
      markets: string[],
    }>}
   * @memberof InfluxStorage
   */
  async importRealtimeBars() {
    /**
     * closed bars
     * @type {Bar[]}
     */
    const barsToImport = []

    /**
     * Total range of import
     * @type {TimeRange}
     */
    const importedRange = {
      from: Infinity,
      to: 0,
      markets: [],
    }

    for (const identifier in this.realtimeBars) {
      for (let i = 0; i < this.realtimeBars[identifier].length; i++) {
        const bar = this.realtimeBars[identifier][i]

        importedRange.from = Math.min(bar.time, importedRange.from)
        importedRange.to = Math.max(bar.time, importedRange.to)

        if (importedRange.markets.indexOf(identifier) === -1) {
          importedRange.markets.push(identifier)
        }

        barsToImport.push(this.realtimeBars[identifier].shift())
        i--
      }
    }

    // free up realtime bars
    this.realtimeBars = {}

    if (barsToImport.length) {
      await this.writePoints(
        barsToImport.map((bar, index) => {
          const fields = {
            cbuy: bar.cbuy,
            csell: bar.csell,
            vbuy: bar.vbuy,
            vsell: bar.vsell,
            lbuy: bar.lbuy,
            lsell: bar.lsell,
          }

          if (bar.close !== null) {
            ;(fields.open = bar.open), (fields.high = bar.high), (fields.low = bar.low), (fields.close = bar.close)
          }

          return {
            measurement: 'trades_' + getHms(this.options.influxTimeframe),
            tags: {
              market: bar.market,
            },
            fields: fields,
            timestamp: +bar.time,
          }
        }),
        {
          precision: 'ms',
          retentionPolicy: this.baseRp,
        }
      )
    }

    return importedRange
  }

  /**
   * Trades into bars (pending bars)
   *
   * @param {Trade[]} trades
   * @returns {Promise<{
      from: number,
      to: number,
      markets: string[],
    }>}
   * @memberof InfluxStorage
   */
  async processTrades(trades) {
    /**
     * Current bars
     * @type {{[identifier: string]: Bar}}
     */
    const activeBars = {}

    for (let i = 0; i <= trades.length; i++) {
      const trade = trades[i]

      let tradeIdentifier
      let tradeFlooredTime

      if (!trade) {
        // end of loop reached = close all bars
        for (let barIdentifier in activeBars) {
          this.closeBar(activeBars[barIdentifier])

          delete activeBars[barIdentifier]
        }

        break
      } else {
        tradeIdentifier = trade.exchange + ':' + trade.pair

        tradeFlooredTime = Math.floor(trade.timestamp / this.options.influxTimeframe) * this.options.influxTimeframe

        if (!activeBars[tradeIdentifier] || activeBars[tradeIdentifier].time < tradeFlooredTime) {
          if (activeBars[tradeIdentifier]) {
            // close bar required
            this.closeBar(activeBars[tradeIdentifier])

            delete activeBars[tradeIdentifier]
          }

          if (trade) {
            // create bar required

            if (!this.realtimeBars[tradeIdentifier]) {
              this.realtimeBars[tradeIdentifier] = []
            }

            if (
              this.realtimeBars[tradeIdentifier].length &&
              this.realtimeBars[tradeIdentifier][this.realtimeBars[tradeIdentifier].length - 1].time === tradeFlooredTime
            ) {
              activeBars[tradeIdentifier] = this.realtimeBars[tradeIdentifier][this.realtimeBars[tradeIdentifier].length - 1]
            } else if (this.lastBar[tradeIdentifier] && this.lastBar[tradeIdentifier].time === tradeFlooredTime) {
              // trades passed in save() contains some of the last batch (trade time = last bar time)
              // recover exchange point of lastbar
              this.realtimeBars[tradeIdentifier].push(this.lastBar[tradeIdentifier])
              activeBars[tradeIdentifier] = this.realtimeBars[tradeIdentifier][this.realtimeBars[tradeIdentifier].length - 1]
            } else {
              // create new bar
              this.realtimeBars[tradeIdentifier].push({
                time: tradeFlooredTime,
                market: tradeIdentifier,
                cbuy: 0,
                csell: 0,
                vbuy: 0,
                vsell: 0,
                lbuy: 0,
                lsell: 0,
                open: null,
                high: null,
                low: null,
                close: null,
              })

              activeBars[tradeIdentifier] = this.realtimeBars[tradeIdentifier][this.realtimeBars[tradeIdentifier].length - 1]

              if (typeof this.lastClose[tradeIdentifier] === 'number') {
                // this bar open = last bar close (from last save or getReferencePoint on startup)
                activeBars[tradeIdentifier].open =
                  activeBars[tradeIdentifier].high =
                  activeBars[tradeIdentifier].low =
                  activeBars[tradeIdentifier].close =
                    this.lastClose[tradeIdentifier]
              }
            }
          }
        }
      }

      if (trade.liquidation) {
        // trade is a liquidation
        activeBars[tradeIdentifier]['l' + trade.side] += trade.price * trade.size
      } else {
        if (activeBars[tradeIdentifier].open === null) {
          // new bar without close in db, should only happen once
          console.log(`[storage/influx] register new serie ${tradeIdentifier}`)
          activeBars[tradeIdentifier].open =
            activeBars[tradeIdentifier].high =
            activeBars[tradeIdentifier].low =
            activeBars[tradeIdentifier].close =
              +trade.price
        }

        activeBars[tradeIdentifier].high = Math.max(activeBars[tradeIdentifier].high, +trade.price)
        activeBars[tradeIdentifier].low = Math.min(activeBars[tradeIdentifier].low, +trade.price)
        activeBars[tradeIdentifier].close = +trade.price

        activeBars[tradeIdentifier]['c' + trade.side]++
        activeBars[tradeIdentifier]['v' + trade.side] += trade.price * trade.size
      }
    }
  }

  async writePoints(points, options, attempt = 0) {
    if (!points.length) {
      return
    }

    const measurement = points[0].measurement
    const from = points[0].timestamp
    const to = points[points.length - 1].timestamp

    try {
      await this.influx.writePoints(points, options)

      if (attempt > 0) {
        console.debug(`[storage/influx] successfully wrote points after ${attempt} attempt(s)`)
      }
    } catch (error) {
      attempt++

      console.error(
        `[storage/influx] write points failed (${attempt}${
          attempt === 1 ? 'st' : attempt === 2 ? 'nd' : attempt === 3 ? 'rd' : 'th'
        } attempt)`,
        error.message
      )

      if (attempt > 5) {
        console.error(
          `too many attemps at writing points\n\n${measurement}, ${new Date(from).toUTCString()} to ${new Date(
            to
          ).toUTCString()}\n\t-> abort`
        )
        throw error
      }

      await sleep(500)

      return this.writePoints(points, options, attempt)
    }
  }

  async executeQuery(query, attempt = 0) {
    try {
      await this.influx.query(query)

      if (attempt > 0) {
        console.debug(`[storage/influx] successfully executed query ${attempt} attempt(s)`)
      }
    } catch (error) {
      attempt++

      console.error(
        `[storage/influx] query failed (${attempt}${attempt === 1 ? 'st' : attempt === 2 ? 'nd' : attempt === 3 ? 'rd' : 'th'} attempt)`,
        error.message
      )

      if (attempt > 5) {
        console.error(`too many attemps at executing query\n\n${query}\n\t-> abort`)
        throw error
      }

      await sleep(500)

      return this.executeQuery(query, attempt)
    }
  }

  fetch({ from, to, timeframe = 60000, markets = [] }) {
    const timeframeLitteral = getHms(timeframe)

    let query = `SELECT * FROM "${this.options.influxDatabase}"."${this.options.influxRetentionPrefix}${timeframeLitteral}"."trades_${timeframeLitteral}" WHERE time >= ${from}ms AND time < ${to}ms`

    if (markets.length) {
      query += ` AND market =~ /${markets.join('|').replace(/\//g, '\\/')}/`
    }

    return this.influx
      .queryRaw(query, {
        precision: 's',
        epoch: 's',
      })
      .then((results) => {
        if (!results.results[0].series) {
          return []
        }

        if (to > +new Date() - this.options.influxResampleInterval) {
          return this.completeBarsWithRealtime(results.results[0].series[0].values, markets, from, to)
        } else {
          // return only db results
          return results.results[0].series[0].values
        }
      })
      .catch((err) => {
        console.error(`[storage/influx] failed to retrieves trades between ${from} and ${to} with timeframe ${timeframe}\n\t`, err.message)
      })
  }

  /**
   * Concat given results of bars with realtime bars (pending bars)
   * If clustering enabled, use collectors as source of pending bars
   * Otherwise current node pending bars will be used
   * @param {number[][]} bars
   * @param {string[]} markets
   * @param {number} from
   * @param {number} to
   * @returns
   */
  completeBarsWithRealtime(bars, markets, from, to) {
    if (this.options.influxCollectors && this.clusteredCollectors.length) {
      // use collectors nodes pending bars
      return this.requestPendingBars(markets, from, to).then((pendingBars) => {
        return bars.concat(pendingBars)
      })
    } else {
      // use current node pending bars
      let injectedPendingBars = []

      for (const market of markets) {
        if (this.realtimeBars[market] && this.realtimeBars[market].length) {
          for (const bar of this.realtimeBars[market]) {
            if (bar.time >= from && bar.time <= to) {
              injectedPendingBars.push(bar)
            }
          }
        }
      }

      injectedPendingBars = injectedPendingBars.sort((a, b) => a.time - b.time)

      return bars.concat(injectedPendingBars)
    }
  }

  /**
   * Called from a collector node
   * Connect current collector node to cluster node
   * WILL try to reconnect if it fails
   * @returns {void}
   */
  connectToCluster() {
    if (this.clusterSocket) {
      console.warn('[storage/influx/collector] already connected (aborting)')
      return
    }

    console.debug('[storage/influx/collector] connecting to cluster..')

    this.clusterSocket = net.createConnection(this.options.influxCollectorsClusterSocketPath)

    this.clusterSocket.on('connect', () => {
      console.log('[storage/influx/collector] successfully connected to cluster')
      this.clusterSocket.write(JSON.stringify(this.options.pairs) + '#')
    })

    // store current incoming to be filled by potentialy partial chunks
    let incomingData = ''

    this.clusterSocket.on('data', (data) => {
      // data is a stringified json inside a buffer
      // convert to string
      const stringData = data.toString()

      // complete data has a # char at it's end
      const incompleteData = stringData[stringData.length - 1] !== '#'

      if (stringData.indexOf('#') !== -1) {
        // data has delimiter

        // split chunks using given delimiter
        const chunks = stringData.split('#')

        for (let i = 0; i < chunks.length; i++) {
          if (!chunks[i].length) {
            // chunk is empty (last one can be as # used as divider:
            // partial_chunk#complete_chunk#*empty_chunk <-)
            // complete_chunk#complete_chunk#*empty_chunk <-)
            // partial_chunk#*empty_chunk <-)
            // complete_chunk#*empty_chunk <-)
            continue
          }

          // add to already existing incoming data (if i not last: this is a end of chunk)
          incomingData += chunks[i]

          if (i === chunks.length - 1 && incompleteData) {
            // last chunk and incomplete
            // wait for next data event
            continue
          }

          // this is a complete chunk either because i < last OR last and # at this end of the total stringData
          try {
            const data = JSON.parse(incomingData)

            // ONLY emited data from cluster is to ask the realtime bar from collector
            // thus no need to filter event op or anything ..
            this.emitRealtimeBars(data.requestId, data.markets, data.from, data.to)
          } catch (error) {
            console.error(
              '[storage/influx/cluster] failed to parse cluster incoming data',
              'first 10 chars: ',
              incomingData.slice(0, 10),
              'last 10 chars: ',
              stringData.slice(-10),
              error.message
            )
          }

          // flush incoming data for next chunk
          incomingData = ''
        }
      } else {
        // no delimiter in payload so this *has* to be incomplete data
        incomingData += stringData
      }
    })

    this.clusterSocket.on('close', () => {
      // collector never close connection with cluster by itself
      console.error('[storage/influx/collector] cluster closed (unexpectedly)')

      // schedule reconnection
      this.reconnectCluster()
    })

    this.clusterSocket.on('error', (error) => {
      // the close even destroy the previous strem and may trigger error
      // reconnect in this situation as well
      this.reconnectCluster()
    })
  }

  /**
   * Handle connectToCluster failure and unexpected close
   * @returns {void}
   */
  reconnectCluster() {
    if (this.clusterSocket) {
      // ensure previous stream is donezo
      this.clusterSocket.destroy()
      this.clusterSocket = null
    }

    if (this._clusterConnectionTimeout) {
      clearTimeout(this._clusterConnectionTimeout)
    } else {
      console.log(`[storage/influx/collector] schedule reconnect to cluster (${this.options.influxCollectorsReconnectionDelay / 1000}s)`)
    }

    this._clusterConnectionTimeout = setTimeout(() => {
      this._clusterConnectionTimeout = null

      this.connectToCluster()
    }, this.options.influxCollectorsReconnectionDelay)
  }

  /**
   * Create cluster unix socket
   * And listen for collectors joining
   */
  createCluster() {
    try {
      if (statSync(this.options.influxCollectorsClusterSocketPath)) {
        console.debug(`[storage/influx/cluster] unix socket was not closed propertly last time`)
        unlinkSync(this.options.influxCollectorsClusterSocketPath)
      }
    } catch (error) {}

    this.serverSocket = net.createServer((socket) => {
      console.log('[storage/influx/cluster] collector connected successfully')

      socket.on('end', () => {
        console.log('[storage/influx/cluster] collector disconnected (unexpectedly)')

        const index = this.clusteredCollectors.indexOf(socket)

        if (index !== -1) {
          this.clusteredCollectors.splice(index, 1)
        } else {
          console.error(`[storage/influx/cluster] disconnected collector WAS NOT found in collectors array`)
        }

        socket.destroy()
      })

      // store current incoming to be filled by potentialy partial chunks
      let incomingData = ''

      socket.on('data', (data) => {
        // data is a stringified json inside a buffer
        // convert to string
        const stringData = data.toString()

        // complete data has a # char at it's end
        const incompleteData = stringData[stringData.length - 1] !== '#'

        if (stringData.indexOf('#') !== -1) {
          // data has delimiter

          // split chunks using given delimiter
          const chunks = stringData.split('#')

          for (let i = 0; i < chunks.length; i++) {
            if (!chunks[i].length) {
              // chunk is empty (last one can be as # used as divider:
              // partial_chunk#complete_chunk#*empty_chunk <-)
              // complete_chunk#complete_chunk#*empty_chunk <-)
              // partial_chunk#*empty_chunk <-)
              // complete_chunk#*empty_chunk <-)
              continue
            }

            // add to already existing incoming data (if i not last: this is a end of chunk)
            incomingData += chunks[i]

            if (i === chunks.length - 1 && incompleteData) {
              // last chunk and incomplete
              // wait for next data event
              continue
            }

            // this is a complete chunk either because i < last OR last and # at this end of the total stringData
            try {
              const data = JSON.parse(incomingData)

              if (!socket.markets) {
                // this is our welcome message
                socket.markets = data

                console.log('[storage/influx/cluster] registered collector with markets', socket.markets.join(', '))

                this.clusteredCollectors.push(socket)
              } else if (data.requestId) {
                if (this.promisesOfRealtimeBars[data.requestId]) {
                  this.promisesOfRealtimeBars[data.requestId](data.results)
                } else {
                  console.error('there was no promiseOfRealtimeBars with given requestId', data.requestId)
                }
                // this is a pending data response to cluster
              }
            } catch (error) {
              console.error(
                '[storage/influx/cluster] failed to parse collector incoming data',
                'first 10 chars: ',
                incomingData.slice(0, 10),
                'last 10 chars: ',
                stringData.slice(-10),
                error.message
              )
            }

            // flush incoming data for next chunk
            incomingData = ''
          }
        } else {
          // no delimiter in payload so this *has* to be incomplete data
          incomingData += stringData
        }
      })
    })

    this.serverSocket.listen(this.options.influxCollectorsClusterSocketPath)

    this.serverSocket.on('error', (error) => {
      console.error(`[storage/influx/cluster] server socket error`, error.message)
    })
  }

  /**
   * Called from the cluster node
   * Return array of realtime bars matching the given criteras (markets, start time & end time)
   * This WILL query all nodes responsible of collecting trades for given markets
   * @param {net.Socket} markets
   * @param {number} from
   * @param {number} to
   */
  async requestPendingBars(markets, from, to) {
    // console.debug('[storage/influx/cluster] requested pending bars', markets.map((m) => m[0]).join(''))

    const collectors = []

    for (let i = 0; i < markets.length; i++) {
      for (let j = 0; j < this.clusteredCollectors.length; j++) {
        if (collectors.indexOf(this.clusteredCollectors[j]) === -1 && this.clusteredCollectors[j].markets.indexOf(markets[i]) !== -1) {
          collectors.push(this.clusteredCollectors[j])
        }
      }
    }

    const promisesOfBars = []

    // console.log(`\t${collectors.length} collector${collectors.length > 1 ? 's' : ''} match${collectors.length > 1 ? 'es' : ''} ${markets.length > 1 ? 'these' : 'this'} market${markets.length > 1 ? 's' : ''}`)

    for (const collector of collectors) {
      promisesOfBars.push(this.requestCollectorPendingBars(collector, markets, from, to))
    }

    return [].concat.apply([], await Promise.all(promisesOfBars)).sort((a, b) => a.time - b.time)
  }

  /**
   * Called from THE cluster node
   * Query specific collector node (socket) for realtime bars matching given criteras
   * @param {net.Socket} collector
   * @param {string[]} markets
   * @param {number} from
   * @param {number} to
   */
  async requestCollectorPendingBars(collector, markets, from, to) {
    return new Promise((resolve) => {
      const requestId = ID()

      let promiseOfRealtimeBarsTimeout = setTimeout(() => {
        console.error('[storage/influx/cluster] promise of realtime bar timeout fired (requestId: ' + requestId + ')')

        // response empty array as we didn't got the expected bars...
        this.promisesOfRealtimeBars[requestId]([])

        // invalidate timeout
        promiseOfRealtimeBarsTimeout = null
      }, 1000)

      // register promise
      this.promisesOfRealtimeBars[requestId] = (realtimeBars) => {
        if (promiseOfRealtimeBarsTimeout) {
          clearTimeout(promiseOfRealtimeBarsTimeout)
        }

        // unregister promise
        delete this.promisesOfRealtimeBars[requestId]

        resolve(realtimeBars)
      }

      collector.write(
        JSON.stringify({
          requestId,
          markets,
          from,
          to,
        }) + '#'
      )
    })
  }

  emitRealtimeBars(requestId, markets, from, to) {
    console.debug(`[storage/influx/collector] cluster is requesting realtime bars data`)

    const results = []

    for (const market of markets) {
      if (this.realtimeBars[market] && this.realtimeBars[market].length) {
        for (const bar of this.realtimeBars[market]) {
          if (bar.time >= from && bar.time <= to) {
            results.push(bar)
          }
        }
      }
    }

    this.clusterSocket.write(
      JSON.stringify({
        requestId,
        results,
      }) + '#'
    )
  }
}

module.exports = InfluxStorage
