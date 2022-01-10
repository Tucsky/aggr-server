const EventEmitter = require('events')
const WebSocket = require('ws')
const fs = require('fs')
const { getIp, getHms, parsePairsFromWsRequest, groupTrades, ago } = require('./helper')
const express = require('express')
const path = require('path')
const rateLimit = require('express-rate-limit')

class Server extends EventEmitter {
  constructor(options, exchanges) {
    super()

    this.options = options
    this.exchanges = exchanges || []
    this.indexedProducts = {}
    this.storages = null

    /**
     * Raw trades ready to be persisted into storage next save
     * @type Trade[]
     */
    this.chunk = []

    /**
     * Keep track of all active connections (exchange + symbol)
     * @type {{[exchangeAndPair: string]: {exchange: string, pair: string, apiId: string, hit: number, ping: number}}}
     */
    this.connections = {}

    /**
     * Keep track of all active apis
     * @type {{[apiId: string]: number}}
     */
    this.apiStats = {}

    /**
     * delayedForBroadcast trades ready to be broadcasted next interval (see _broadcastDelayedTradesInterval)
     * @type Trade[]
     */
    this.delayedForBroadcast = []

    /**
     * active trades aggregations
     * @type {{[aggrIdentifier: string]: Trade}}
     */
    this.aggregating = {}

    /**
     * already aggregated trades ready to be broadcasted (see _broadcastAggregatedTradesInterval)
     * @type Trade[]
     */
    this.aggregated = []

    this.BANNED_IPS = []

    if (this.options.collect) {
      console.log(
        `\n[server] collect is enabled`,
        this.options.broadcast && this.options.broadcastAggr
          ? '\n\twill aggregate every trades that came on same ms (impact only broadcast)'
          : '',
        this.options.broadcast && this.options.broadcastDebounce
          ? `\n\twill broadcast trades every ${this.options.broadcastDebounce}ms`
          : this.options.broadcast
          ? `will broadcast trades instantly`
          : ''
      )
      console.log(`\tconnect to -> ${this.exchanges.map((a) => a.id).join(', ')}`)

      this.handleExchangesEvents()
      this.connectExchanges()

      // profile exchanges connections (keep alive)
      this._activityMonitoringInterval = setInterval(this.monitorExchangesActivity.bind(this, +new Date()), this.options.monitorInterval)
    }

    this.initStorages().then(() => {
      if (this.options.collect) {
        if (this.storages) {
          const delay = this.scheduleNextBackup()

          console.log(
            `[server] scheduling first save to ${this.storages.map((storage) => storage.constructor.name)} in ${getHms(delay)}...`
          )
        }
      }

      if (this.options.api || this.options.broadcast) {
        if (!this.options.port) {
          console.error(
            `\n[server] critical error occured\n\t-> setting a network port is mandatory for API or broadcasting (value is ${this.options.port})\n\n`
          )
          process.exit()
        }

        this.createHTTPServer()
      }

      if (this.options.broadcast) {
        this.createWSServer()

        if (this.options.broadcastAggr) {
          this._broadcastAggregatedTradesInterval = setInterval(this.broadcastAggregatedTrades.bind(this), 50)
        }
      }

      // update banned ip
      this.listenBannedIps()
    })
  }

  initStorages() {
    if (!this.options.storage) {
      return Promise.resolve()
    }

    this.storages = []

    const promises = []

    for (let name of this.options.storage) {
      console.log(`[storage] Using "${name}" storage solution`)

      if (this.options.api && this.options.storage.length > 1 && !this.options.storage.indexOf(name)) {
        console.log(`[storage] Set "${name}" as primary storage for API`)
      }

      let storage = new (require(`./storage/${name}`))(this.options)

      if (typeof storage.connect === 'function') {
        promises.push(storage.connect())
      } else {
        promises.push(Promise.resolve())
      }

      this.storages.push(storage)
    }

    console.log(`[storage] all storage ready`)

    return Promise.all(promises)
  }

  backupTrades(exitBackup) {
    if (exitBackup) {
      clearTimeout(this.backupTimeout)
    } else if (!this.storages || !this.chunk.length) {
      this.scheduleNextBackup()
      return Promise.resolve()
    }

    const chunk = this.chunk.splice(0, this.chunk.length)

    return Promise.all(
      this.storages.map((storage) => {
        if (exitBackup) {
          console.log(`[server/exit] saving ${chunk.length} trades into ${storage.constructor.name}`)
        }
        return storage
          .save(chunk, exitBackup)
          .then(() => {
            if (exitBackup) {
              console.log(`[server/exit] performed backup of ${chunk.length} trades into ${storage.constructor.name}`)
            }
          })
          .catch((err) => {
            console.error(`[storage/${storage.name}] saving failure`, err)
          })
      })
    )
      .then(() => {
        if (!exitBackup) {
          this.scheduleNextBackup()
        }
      })
      .catch((err) => {
        console.error(`[server] something went wrong while backuping trades...`, err)
      })
  }

  scheduleNextBackup() {
    if (!this.storages) {
      return
    }

    const now = new Date()
    let delay = Math.ceil(now / this.options.backupInterval) * this.options.backupInterval - now - 20

    if (delay < 1000) {
      delay += this.options.backupInterval
    }

    this.backupTimeout = setTimeout(this.backupTrades.bind(this), delay)

    return delay
  }

  handleExchangesEvents() {
    this.exchanges.forEach((exchange) => {
      if (this.options.broadcast && this.options.broadcastAggr) {
        exchange.on('trades', this.dispatchAggregateTrade.bind(this, exchange.id))
      } else {
        exchange.on('trades', this.dispatchRawTrades.bind(this, exchange.id))
      }

      exchange.on('liquidations', this.dispatchRawTrades.bind(this, exchange.id))

      exchange.on('index', (pairs) => {
        for (let pair of pairs) {
          if (this.indexedProducts[pair]) {
            this.indexedProducts[pair].count++

            if (this.indexedProducts[pair].exchanges.indexOf(exchange.id) === -1) {
              this.indexedProducts[pair].exchanges.push(exchange.id)
            }
          } else {
            this.indexedProducts[pair] = {
              value: pair,
              count: 1,
              exchanges: [exchange.id],
            }
          }
        }
      })

      exchange.on('open', (event) => {
        this.broadcastJson({
          type: 'exchange_connected',
          id: exchange.id,
        })
      })

      exchange.on('disconnected', (pair, apiId, apiLength) => {
        const id = exchange.id + ':' + pair

        if (!this.connections[id]) {
          console.error(`[server] couldn't delete connection ${id} because the connections[${id}] does not exists`)
          return
        }

        console.log(`[server] deleted connection ${id} (${apiLength} connected)`)

        delete this.connections[id]

        if (!apiLength) {
          console.log(`[server] deleted api stats ${apiId}`)

          delete this.apiStats[apiId]
        } else {
          this.apiStats[apiId].pairs.splice(this.apiStats[apiId].pairs.indexOf(pair), 1)
        }

        this.checkApiStats()
      })

      exchange.on('connected', (pair, apiId, apiLength) => {
        const id = exchange.id + ':' + pair

        if (this.connections[id]) {
          console.error(`[server] couldn't register connection ${id} because the connections[${id}] does not exists`)
          return
        }

        console.log(`[server] registered connection ${id} (${apiLength})`)

        const now = +new Date()

        this.connections[id] = {
          apiId,
          exchange: exchange.id,
          pair: pair,
          hit: 0,
          start: now,
          timestamp: now,
        }

        if (typeof this.apiStats[apiId] === 'undefined') {
          this.apiStats[apiId] = {
            exchange: exchange.id,
            totalHits: 0,
            windowHits: 0,
            ping: 0,
            pairs: [],
            start: now,
            timestamp: now,
          }
        }

        this.apiStats[apiId].pairs.push(pair)
        this.apiStats[apiId].name =
          this.apiStats[apiId].exchange +
          ':' +
          this.apiStats[apiId].pairs.slice(0, 3).join('+') +
          (this.apiStats[apiId].pairs.length > 3 ? '.. (' + this.apiStats[apiId].pairs.length + ')' : '')

        this.checkApiStats()
      })

      exchange.on('err', (event) => {
        this.broadcastJson({
          type: 'exchange_error',
          id: exchange.id,
          message: event.message,
        })
      })

      exchange.on('close', (event) => {
        this.broadcastJson({
          type: 'exchange_disconnected',
          id: exchange.id,
        })
      })
    })
  }

  createWSServer() {
    if (!this.options.broadcast) {
      return
    }

    this.wss = new WebSocket.Server({
      server: this.server,
    })

    this.wss.on('listening', () => {
      console.log(`[server] websocket server listening at localhost:${this.options.port}`)
    })

    this.wss.on('connection', (ws, req) => {
      const ip = getIp(req)
      const pairs = parsePairsFromWsRequest(req)

      if (pairs && pairs.length) {
        ws.pairs = pairs
      }

      ws.pairs = pairs

      const data = {
        type: 'welcome',
        supportedPairs: Object.values(this.connections).map((a) => a.exchange + ':' + a.pair),
        timestamp: +new Date(),
        exchanges: this.exchanges.map((exchange) => {
          return {
            id: exchange.id,
            connected: exchange.apis.reduce((pairs, api) => {
              pairs.concat(api._connected)
              return pairs
            }, []),
          }
        }),
      }

      console.log(`[${ip}/ws/${ws.pairs.join('+')}] joined ${req.url} from ${req.headers['origin']}`)

      this.emit('connections', this.wss.clients.size)

      ws.send(JSON.stringify(data))

      ws.on('message', (event) => {
        const message = event.trim()

        const pairs = message.length
          ? message
              .split('+')
              .map((a) => a.trim())
              .filter((a) => a.length)
          : []

        console.log(`[${ip}/ws] subscribe to ${pairs.join(' + ')}`)

        ws.pairs = pairs
      })

      ws.on('close', (event) => {
        let error = null

        switch (event) {
          case 1002:
            error = 'Protocol Error'
            break
          case 1003:
            error = 'Unsupported Data'
            break
          case 1007:
            error = 'Invalid frame payload data'
            break
          case 1008:
            error = 'Policy Violation'
            break
          case 1009:
            error = 'Message too big'
            break
          case 1010:
            error = 'Missing Extension'
            break
          case 1011:
            error = 'Internal Error'
            break
          case 1012:
            error = 'Service Restart'
            break
          case 1013:
            error = 'Try Again Later'
            break
          case 1014:
            error = 'Bad Gateway'
            break
          case 1015:
            error = 'TLS Handshake'
            break
        }

        if (error) {
          console.log(`[${ip}] unusual close "${error}"`)
        }

        setTimeout(() => this.emit('connections', this.wss.clients.size), 100)
      })
    })
  }

  createHTTPServer() {
    const app = express()

    if (this.options.enableRateLimit) {
      const limiter = rateLimit({
        windowMs: this.options.rateLimitTimeWindow,
        max: this.options.rateLimitMax,
        handler: function (req, res) {
          res.header('Access-Control-Allow-Origin', '*')
          return res.status(429).send({
            error: 'too many requests :v',
          })
        },
      })

      // otherwise ip are all the same
      app.set('trust proxy', 1)

      // apply to all requests
      app.use(limiter)
    }

    app.all('/*', (req, res, next) => {
      var ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress

      if (!req.headers['origin'] || !new RegExp(this.options.origin).test(req.headers['origin'])) {
        console.log(`[${ip}/BLOCKED] socket origin mismatch "${req.headers['origin']}"`)
        setTimeout(() => {
          return res.status(500).json({
            error: 'ignored',
          })
        }, 5000 + Math.random() * 5000)
      } else if (this.BANNED_IPS.indexOf(ip) !== -1) {
        console.debug(`[${ip}/BANNED] at "${req.url}" from "${req.headers['origin']}"`)

        setTimeout(() => {
          return res.status(500).json({
            error: 'ignored',
          })
        }, 5000 + Math.random() * 5000)
      } else {
        res.header('Access-Control-Allow-Origin', '*')
        res.header('Access-Control-Allow-Headers', 'X-Requested-With')
        next()
      }
    })

    app.get('/', function (req, res) {
      res.json({
        message: 'hi',
      })
    })

    app.get('/historical/:from/:to/:timeframe?/:markets([^/]*)?', (req, res) => {
      const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress
      let from = parseInt(req.params.from)
      let to = parseInt(req.params.to)
      let length
      let timeframe = req.params.timeframe

      let markets = req.params.markets || []

      if (typeof markets === 'string') {
        markets = markets
          .split('+')
          .map((a) => a.trim())
          .filter((a) => a.length)
      }

      if (!this.options.api || !this.storages) {
        return res.status(501).json({
          error: 'no storage',
        })
      }

      const storage = this.storages[0]

      if (isNaN(from) || isNaN(to)) {
        return res.status(400).json({
          error: 'missing interval',
        })
      }

      if (storage.format === 'point') {
        timeframe = parseInt(timeframe) || 1000 * 60 // default to 1m

        length = (to - from) / timeframe

        if (length > this.options.maxFetchLength) {
          return res.status(400).json({
            error: 'too many bars',
          })
        }
      }

      if (from > to) {
        return res.status(400).json({
          error: 'from > to',
        })
      }

      const fetchStartAt = +new Date()

      ;(storage
        ? storage.fetch({
            from,
            to,
            timeframe,
            markets,
          })
        : Promise.resolve([])
      )
        .then((output) => {
          if (!output) {
            return res.status(404).json({
              error: 'no results',
            })
          }

          if (length > 2000 || markets.length > 20) {
            console.log(
              `[${ip}/${req.get('origin')}] ${getHms(to - from)} (${markets.length} markets, ${getHms(timeframe, true)} tf) -> ${
                +length ? parseInt(length) + ' bars into ' : ''
              }${output.length} ${storage.format}s, took ${getHms(+new Date() - fetchStartAt)}`
            )
          }

          if (storage.format === 'trade') {
            for (let i = 0; i < this.chunk.length; i++) {
              if (this.chunk[i][1] <= from || this.chunk[i][1] >= to) {
                continue
              }

              output.push(this.chunk[i])
            }
          }

          return res.status(200).json({
            format: storage.format,
            results: output,
          })
        })
        .catch((err) => {
          return res.status(500).json({
            err: err.message,
          })
        })
    })

    app.use(function (err, req, res, next) {
      if (err) {
        console.error(err)

        return res.status(500).json({
          error: 'internal server error 💀',
        })
      }
    })

    this.server = app.listen(this.options.port, () => {
      console.log(
        `[server] http server listening at localhost:${this.options.port}`,
        !this.options.api ? '(historical api is disabled)' : ''
      )
    })

    this.app = app
  }

  connectExchanges() {
    if (!this.exchanges.length || !this.options.pairs.length) {
      return
    }

    this.chunk = []

    const exchangesProductsResolver = Promise.all(
      this.exchanges.map((exchange) => {
        const exchangePairs = this.options.pairs.filter(
          (pair) => pair.indexOf(':') === -1 || new RegExp('^' + exchange.id + ':').test(pair)
        )

        if (!exchangePairs.length) {
          return Promise.resolve()
        }

        return exchange.getProductsAndConnect(exchangePairs)
      })
    )

    exchangesProductsResolver.then(() => {
      this.checkApiStats()
    })

    if (this.options.broadcast && this.options.broadcastDebounce && !this.options.broadcastAggr) {
      this._broadcastDelayedTradesInterval = setInterval(() => {
        if (!this.delayedForBroadcast.length) {
          return
        }

        this.broadcastTrades(this.delayedForBroadcast)

        this.delayedForBroadcast = []
      }, this.options.broadcastDebounce || 1000)
    }
  }

  /**
   * Trigger subscribe to pairs on all activated exchanges
   * @param {string[]} pairs
   * @returns {Promise<any>} promises of connections
   * @memberof Server
   */
  async connectPairs(pairs) {
    console.log(`[server] connect to ${pairs.join(',')}`)

    const promises = []

    for (let exchange of this.exchanges) {
      for (let pair of pairs) {
        promises.push(
          exchange.link(pair).catch((err) => {
            console.debug(`[server/connectPairs/${exchange.id}] ${err}`)

            if (err instanceof Error) {
              console.error(err)
            }
          })
        )
      }
    }

    await Promise.all(promises)
  }

  /**
   * Trigger unsubscribe to pairs on all activated exchanges
   * @param {string[]} pairs
   * @returns {Promise<void>} promises of disconnection
   * @memberof Server
   */
  async disconnectPairs(pairs) {
    console.log(`[server] disconnect from ${pairs.join(',')}`)

    for (let exchange of this.exchanges) {
      for (let pair of pairs) {
        try {
          await exchange.unlink(pair)
        } catch (err) {
          console.debug(`[server/disconnectPairs/${exchange.id}] ${err}`)

          if (err instanceof Error) {
            console.error(err)
          }
        }
      }
    }
  }

  broadcastJson(data) {
    if (!this.wss) {
      return
    }

    this.wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify(data))
      }
    })
  }

  broadcastTrades(trades) {
    if (!this.wss) {
      return
    }

    const groups = groupTrades(trades, true, this.options.broadcastThreshold)

    this.wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        for (let i = 0; i < client.pairs.length; i++) {
          if (groups[client.pairs[i]]) {
            client.send(JSON.stringify([client.pairs[i], groups[client.pairs[i]]]))
          }
        }
      }
    })
  }

  /**
   * Excecuted every options.monitorInterval
   * Check activity within that time window for every active apis
   */
  monitorExchangesActivity() {
    this.updateApiStats()
    this.checkApiStats(true)

    // print full connection report every hour
    const now = Date.now()
    const flooredTime = Math.floor(now / this.options.monitorInterval) * this.options.monitorInterval
    const currentHour = Math.floor(now / 3600000) * 3600000
    //const currentMinute = Math.floor(now / 60000) * 60000

    if (flooredTime === currentHour) {
      this.checkApiStats()
    }
  }

  /**
   * Aggregate individual connections stats (markets) into api stats (group of markets)
   * @param {number} now timestamp
   */
  updateApiStats() {
    const now = Date.now()

    for (const marketId in this.connections) {
      const connection = this.connections[marketId]
      const apiStat = this.apiStats[connection.apiId]

      if (apiStat.timestamp !== now) {
        apiStat.windowHits = 0
        apiStat.ping = 0
        apiStat.timestamp = now
      }

      const currentHits = typeof connection.lastHit !== 'undefined' ? connection.hit - connection.lastHit : connection.hit
      apiStat.windowHits += currentHits
      apiStat.totalHits += currentHits
      apiStat.ping = Math.max(connection.timestamp || connection.start, apiStat.ping)

      connection.lastHit = connection.hit
    }
  }

  /**
   * Check if an API is having more idle time than usual
   * Or sudden stops of very active feeds
   * By default only prints the APIs that fits in one of those 2 situations
   * Reconnect if reconnectIfNeeded is true
   * @param {boolean} reconnectIfNeeded reconnect above threshold apis
   * @param {boolean} bypassTimeout
   */
  checkApiStats(reconnectIfNeeded, bypassTimeout) {
    if (!reconnectIfNeeded && !bypassTimeout) {
      if (this._checkApiStatsTimeout) {
        clearTimeout(this._checkApiStatsTimeout)
      }

      this._checkApiStatsTimeout = setTimeout(() => {
        this._checkApiStatsTimeout = null
        this.checkApiStats(false, true)
      }, 2500)

      return
    }

    const now = Date.now()

    const table = {}
    const apisToReconnect = []

    for (const apiId in this.apiStats) {
      const apiStat = this.apiStats[apiId]

      const avg = +((this.options.monitorInterval / (now - apiStat.start)) * apiStat.totalHits).toFixed(2)
      let hit = apiStat.windowHits

      const ping = apiStat.ping ? now - apiStat.ping : 0

      const thrs = Math.max(this.options.reconnectionThreshold / Math.sqrt(avg || 0.5), 1000)
      const thrsHms = getHms(thrs, true)
      let pingHms = ping ? getHms(ping, true) : 'never'

      let reconnectionThresholdReached = false
      let hitThresholdReached = false

      if (reconnectIfNeeded && now - apiStat.start > 10000) {
        if (ping > thrs) {
          reconnectionThresholdReached = true
          pingHms += '⚠'
        } else if (thrs < this.options.reconnectionThreshold / 10 && !hit) {
          hitThresholdReached = true
          hit += '⚠'
        }
      }

      if (!reconnectIfNeeded || reconnectionThresholdReached || hitThresholdReached) {
        table[apiStat.name] = {
          hit,
          avg,
          ping: pingHms,
          thrs: thrsHms,
        }

        if (reconnectIfNeeded && (reconnectionThresholdReached || hitThresholdReached)) {
          console.warn(`[server] reconnect api ${apiId} (${reconnectionThresholdReached ? 'ping' : 'hit'} threshold reached)`)
          apisToReconnect.push(apiId)
        }
      }
    }

    if (Object.keys(table).length) {
      console.table(table)
    }

    if (apisToReconnect.length) {
      console.log(`[server] reconnect ${apisToReconnect.length} apis`)

      for (let exchange of this.exchanges) {
        for (let api of exchange.apis) {
          if (apisToReconnect.indexOf(api.id) !== -1) {
            const timestampByPair = this.apiStats[api.id].pairs.reduce((timestamps, pair) => {
              timestamps[pair] = this.connections[exchange.id + ':' + pair].timestamp

              return timestamps
            }, {})

            exchange.reconnectApi(api, timestampByPair)
          }
        }
      }
    }
  }

  listenBannedIps() {
    const file = path.resolve(__dirname, '../banned.txt')

    const watch = () => {
      fs.watchFile(file, () => {
        this.updateBannedIps()
      })
    }

    try {
      fs.accessSync(file, fs.constants.F_OK)

      this.updateBannedIps().then((success) => {
        if (success) {
          watch()
        }
      })
    } catch (error) {
      const _checkForWatchInterval = setInterval(() => {
        fs.access(file, fs.constants.F_OK, (err) => {
          if (err) {
            return
          }

          this.updateBannedIps().then((success) => {
            if (success) {
              clearInterval(_checkForWatchInterval)

              watch()
            }
          })
        })
      }, 1000 * 10)
    }
  }

  updateBannedIps() {
    const file = path.resolve(__dirname, '../banned.txt')

    return new Promise((resolve) => {
      fs.readFile(file, 'utf8', (err, data) => {
        if (err) {
          return
        }

        this.BANNED_IPS = data
          .split('\n')
          .map((a) => a.trim())
          .filter((a) => a.length)

        resolve(true)
      })
    })
  }

  dispatchRawTrades(exchange, { source, data }) {
    const now = +new Date()

    for (let i = 0; i < data.length; i++) {
      const trade = data[i]
      const identifier = exchange + ':' + trade.pair

      if (!this.connections[identifier]) {
        // console.warn(`[${exchange}/dispatchRawTrades] connection ${identifier} doesn't exists but tried to dispatch a trade for it`)
        continue
      }

      // ping connection
      this.connections[identifier].hit++
      this.connections[identifier].timestamp = now

      // save trade
      if (this.storages) {
        this.chunk.push(trade)
      }
    }

    if (this.options.broadcast) {
      if (this.options.broadcastAggr && !this.options.broadcastDebounce) {
        this.broadcastTrades(data)
      } else {
        Array.prototype.push.apply(this.delayedForBroadcast, data)
      }
    }
  }

  dispatchAggregateTrade(exchange, { source, data }) {
    const now = +new Date()
    const length = data.length

    for (let i = 0; i < length; i++) {
      const trade = data[i]
      const identifier = exchange + ':' + trade.pair

      // ping connection
      this.connections[identifier].hit++
      this.connections[identifier].timestamp = now

      // save trade
      if (this.storages) {
        this.chunk.push(trade)
      }

      if (!this.connections[identifier]) {
        console.error(`UNKNOWN TRADE SOURCE`, trade)
        console.info('This trade will be ignored.')
        continue
      }

      if (this.aggregating[identifier]) {
        const queuedTrade = this.aggregating[identifier]

        if (queuedTrade.timestamp === trade.timestamp && queuedTrade.side === trade.side) {
          queuedTrade.size += trade.size
          queuedTrade.price += trade.price * trade.size
          continue
        } else {
          queuedTrade.price /= queuedTrade.size
          this.aggregated.push(queuedTrade)
        }
      }

      this.aggregating[identifier] = Object.assign({}, trade)
      this.aggregating[identifier].timeout = now + 50
      this.aggregating[identifier].price *= this.aggregating[identifier].size
    }
  }

  broadcastAggregatedTrades() {
    const now = +new Date()

    const onGoingAggregation = Object.keys(this.aggregating)

    for (let i = 0; i < onGoingAggregation.length; i++) {
      const trade = this.aggregating[onGoingAggregation[i]]
      if (now > trade.timeout) {
        trade.price /= trade.size
        this.aggregated.push(trade)

        delete this.aggregating[onGoingAggregation[i]]
      }
    }

    if (this.aggregated.length) {
      this.broadcastTrades(this.aggregated)

      this.aggregated.splice(0, this.aggregated.length)
    }
  }
}

module.exports = Server
