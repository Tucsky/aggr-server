const EventEmitter = require('events')
const WebSocket = require('ws')
const fs = require('fs')
const { getIp, getHms, parsePairsFromWsRequest, groupTrades } = require('./helper')
const express = require('express')
const cors = require('cors')
const path = require('path')
const rateLimit = require('express-rate-limit')
const webPush = require('web-push')
const bodyParser = require('body-parser')
const config = require('./config')
const alertService = require('./services/alert')
const { connections, registerConnection } = require('./services/connections')

class Server extends EventEmitter {
  constructor(exchanges) {
    super()

    this.exchanges = exchanges || []
    this.storages = null

    /**
     * raw trades ready to be persisted into storage next save
     * @type Trade[]
     */
    this.chunk = []

    /**
     * keep track of all active apis
     * @type {{[apiId: string]: {
     *  start: number,
     *  timestamp: number,
     *  hit: number
     * }}}
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

    if (config.collect) {
      console.log(
        `\n[server] collect is enabled`,
        config.broadcast && config.broadcastAggr ? '\n\twill aggregate every trades that came on same ms (impact only broadcast)' : '',
        config.broadcast && config.broadcastDebounce
          ? `\n\twill broadcast trades every ${config.broadcastDebounce}ms`
          : config.broadcast
          ? `will broadcast trades instantly`
          : ''
      )
      console.log(`\tconnect to -> ${this.exchanges.map((a) => a.id).join(', ')}`)

      this.handleExchangesEvents()
      this.connectExchanges()

      // profile exchanges connections (keep alive)
      this._activityMonitoringInterval = setInterval(this.monitorExchangesActivity.bind(this, Date.now()), config.monitorInterval)
    }

    this.initStorages().then(() => {
      if (config.collect) {
        if (this.storages) {
          const delay = this.scheduleNextBackup()

          console.log(
            `[server] scheduling first save to ${this.storages.map((storage) => storage.constructor.name)} in ${getHms(delay)}...`
          )
        }
      }

      if (config.api || config.broadcast) {
        if (!config.port) {
          console.error(
            `\n[server] critical error occured\n\t-> setting a network port is mandatory for API or broadcasting (value is ${config.port})\n\n`
          )
          process.exit()
        }

        this.createHTTPServer()
      }

      if (config.broadcast) {
        this.createWSServer()

        if (config.broadcastAggr) {
          this._broadcastAggregatedTradesInterval = setInterval(this.broadcastAggregatedTrades.bind(this), 50)
        }
      }

      // update banned users
      this.listenBannedIps()
    })
  }

  initStorages() {
    if (!config.storage) {
      return Promise.resolve()
    }

    this.storages = []

    const promises = []

    for (let name of config.storage) {
      console.log(`[storage] Using "${name}" storage solution`)

      if (config.api && config.storage.length > 1 && !config.storage.indexOf(name)) {
        console.log(`[storage] Set "${name}" as primary storage for API`)
      }

      let storage = new (require(`./storage/${name}`))()

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

    const chunk = this.chunk.splice(0, this.chunk.length).sort((a, b) => a.timestamp - b.timestamp)

    if (config.id === 'btceth' && chunk.length) {
      // only for debug purposes
      const firstTradeTimestamp = +chunk[0].timestamp
      const lastTradeTimestamp = +chunk[chunk.length - 1].timestamp
      try {
        console.log(
          `[server] backup trades ${chunk.length} ${new Date(firstTradeTimestamp).toISOString()} ->  ${new Date(lastTradeTimestamp).toISOString()}`
        )
      } catch (error) {
        console.log(
          `[server] backup trades (errored) ${chunk.length} ${firstTradeTimestamp} ->  ${lastTradeTimestamp}`
        )
      }
    }

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
    let delay = Math.ceil(now / config.backupInterval) * config.backupInterval - now - 20

    if (delay < 1000) {
      delay += config.backupInterval
    }

    this.backupTimeout = setTimeout(this.backupTrades.bind(this), delay)

    return delay
  }

  handleExchangesEvents() {
    this.exchanges.forEach((exchange) => {
      if (config.broadcast && config.broadcastAggr) {
        exchange.on('trades', this.dispatchAggregateTrade.bind(this, exchange.id))
      } else {
        exchange.on('trades', this.dispatchRawTrades.bind(this, exchange.id))
      }

      exchange.on('liquidations', this.dispatchRawTrades.bind(this, exchange.id))

      exchange.on('disconnected', (pair, apiId, apiLength) => {
        const id = exchange.id + ':' + pair

        console.log(`[server] deleted connection ${id} (${apiLength} connected)`)

        connections[id].apiId = null

        if (!apiLength) {
          delete this.apiStats[apiId]
        } else {
          this.apiStats[apiId].pairs.splice(this.apiStats[apiId].pairs.indexOf(pair), 1)
        }

        this.checkApiStats()
      })

      exchange.on('connected', (pair, apiId, apiLength) => {
        const id = exchange.id + ':' + pair

        if (registerConnection(id, exchange.id, pair, apiLength).hit) {
          if (typeof exchange.getMissingTrades === 'function') {
            exchange.registerRangeForRecovery(connections[id])
          }
        }

        connections[id].apiId = apiId

        if (typeof this.apiStats[apiId] === 'undefined') {
          const now = Date.now()

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

      exchange.on('open', (apiId, pairs) => {
        this.broadcastJson({
          type: 'exchange_connected',
          id: exchange.id,
        })
      })

      exchange.on('error', (apiId, message) => {
        this.broadcastJson({
          type: 'exchange_error',
          id: exchange.id,
          message: message,
        })
      })

      exchange.on('close', (apiId, pairs, event) => {
        if (pairs.length) {
          console.error(
            `[${exchange.id}] api closed unexpectedly (${event.code}, ${event.reason || 'no reason'}) (was handling ${pairs.join(',')})`
          )

          setTimeout(
            () => {
              this.reconnectApis([apiId])
            },
            this.apiStats[apiId] ? 100 : 2000
          )
        }

        this.broadcastJson({
          type: 'exchange_disconnected',
          id: exchange.id,
        })
      })
    })
  }

  createWSServer() {
    if (!config.broadcast) {
      return
    }

    this.wss = new WebSocket.Server({
      server: this.server,
    })

    this.wss.on('listening', () => {
      console.log(`[server] websocket server listening at localhost:${config.port}`)
    })

    this.wss.on('connection', (ws, req) => {
      const user = getIp(req)
      const pairs = parsePairsFromWsRequest(req)

      if (pairs && pairs.length) {
        ws.pairs = pairs
      }

      ws.pairs = pairs

      const data = {
        type: 'welcome',
        supportedPairs: Object.values(connections)
          .filter((connection) => connection.apiId)
          .map((a) => a.exchange + ':' + a.pair),
        timestamp: Date.now(),
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

      console.log(`[${user}/ws/${ws.pairs.join('+')}] joined ${req.url} from ${req.headers['origin']}`)

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

        console.log(`[${user}/ws] subscribe to ${pairs.join(' + ')}`)

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
          console.log(`[${user}] unusual close "${error}"`)
        }

        setTimeout(() => this.emit('connections', this.wss.clients.size), 100)
      })
    })
  }

  createHTTPServer() {
    const app = express()

    app.use(cors())

    if (config.enableRateLimit) {
      const limiter = rateLimit({
        windowMs: config.rateLimitTimeWindow,
        max: config.rateLimitMax,
        handler: function (req, res) {
          res.header('Access-Control-Allow-Origin', '*')
          return res.status(429).send({
            error: 'too many requests :v',
          })
        },
      })

      // otherwise user are all the same
      app.set('trust proxy', 1)

      // apply to all requests
      app.use(limiter)
    }

    app.all('/*', (req, res, next) => {
      var user = req.headers['x-forwarded-for'] || req.connection.remoteAddress

      if (!req.headers['origin'] || (!new RegExp(config.origin).test(req.headers['origin']) && config.whitelist.indexOf(user) === -1)) {
        console.log(`[${user}/BLOCKED] socket origin mismatch "${req.headers['origin']}"`)
        setTimeout(() => {
          return res.status(500).json({
            error: 'ignored',
          })
        }, 5000 + Math.random() * 5000)
      } else if (this.BANNED_IPS.indexOf(user) !== -1) {
        console.debug(`[${user}/BANNED] at "${req.url}" from "${req.headers['origin']}"`)

        setTimeout(() => {
          return res.status(500).json({
            error: 'ignored',
          })
        }, 5000 + Math.random() * 5000)
      } else {
        next()
      }
    })

    app.get('/', function (req, res) {
      res.json({
        message: 'hi',
      })
    })

    if (alertService) {
      app.use(bodyParser.json())
      webPush.setVapidDetails('mailto:test@example.com', config.publicVapidKey, config.privateVapidKey)

      app.post('/alert', (req, res) => {
        const user = req.headers['x-forwarded-for'] || req.connection.remoteAddress
        const alert = req.body

        if (!alert || !alert.endpoint || !alert.keys || typeof alert.market !== 'string' || typeof alert.price !== 'number') {
          return res.status(400).json({ error: 'invalid alert payload' })
        }

        alert.user = user

        try {
          alertService.toggleAlert(alert)
          res.status(201).json({ ok: true })
        } catch (error) {
          console.error(`[alert] couldn't toggle user alert because ${error.message}`)
          res.status(400).json({ error: error.message })
        }
      })
    }

    app.get('/historical/:from/:to/:timeframe?/:markets([^/]*)?', (req, res) => {
      const user = req.headers['x-forwarded-for'] || req.connection.remoteAddress
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

      if (!config.api || !this.storages) {
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

        if (length > config.maxFetchLength) {
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

      const fetchStartAt = Date.now()

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

          if (output.length > 10000) {
            console.log(
              `[${user}/${req.get('origin')}] ${getHms(to - from)} (${markets.length} markets, ${getHms(timeframe, true)} tf) -> ${
                +length ? parseInt(length) + ' bars into ' : ''
              }${output.length} ${storage.format}s, took ${getHms(Date.now() - fetchStartAt)}`
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

    this.server = app.listen(config.port, () => {
      console.log(`[server] http server listening at localhost:${config.port}`, !config.api ? '(historical api is disabled)' : '')
    })

    this.app = app
  }

  connectExchanges() {
    if (!this.exchanges.length || !config.pairs.length) {
      return
    }

    this.chunk = []

    const exchangesProductsResolver = Promise.all(
      this.exchanges.map((exchange) => {
        const exchangePairs = config.pairs.filter((pair) => pair.indexOf(':') === -1 || new RegExp('^' + exchange.id + ':').test(pair))

        if (!exchangePairs.length) {
          return Promise.resolve()
        }

        return exchange.getProductsAndConnect(exchangePairs)
      })
    )

    exchangesProductsResolver.then(() => {
      this.checkApiStats()
    })

    if (config.broadcast && config.broadcastDebounce && !config.broadcastAggr) {
      this._broadcastDelayedTradesInterval = setInterval(() => {
        if (!this.delayedForBroadcast.length) {
          return
        }

        this.broadcastTrades(this.delayedForBroadcast)

        this.delayedForBroadcast = []
      }, config.broadcastDebounce || 1000)
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

    const groups = groupTrades(trades, true, config.broadcastThreshold)

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
    const flooredTime = Math.floor(now / config.monitorInterval) * config.monitorInterval
    const currentHour = Math.floor(now / 3600000) * 3600000
    //const currentHour = Math.floor(now / 10000) * 10000

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

    for (const marketId in connections) {
      const connection = connections[marketId]

      if (!connection.apiId) {
        continue
      }

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

      const avg = +((config.monitorInterval / (now - apiStat.start)) * apiStat.totalHits).toFixed(2)
      let hit = apiStat.windowHits

      const ping = apiStat.ping ? now - apiStat.ping : 0

      let thrs

      if (typeof apiStat.thrs === 'undefined' || hit > 0) {
        thrs = Math.max(config.reconnectionThreshold / Math.sqrt(avg || 0.5), 1000)
        apiStat.thrs = thrs
      } else {
        thrs = apiStat.thrs
      }

      const thrsHms = getHms(thrs, true)
      let pingHms = ping ? getHms(ping, true) : 'never'

      let reconnectionThresholdReached = false
      let hitThresholdReached = false

      if (reconnectIfNeeded && now - apiStat.start > 10000) {
        if (ping > thrs) {
          reconnectionThresholdReached = true
          pingHms += '⚠'
        } else if (thrs < config.reconnectionThreshold / 10 && !hit) {
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
      this.reconnectApis(apisToReconnect)
    }
  }

  reconnectApis(apiIds) {
    console.log(`[server.reconnectApis] reconnect ${apiIds.length} apis`)

    for (let exchange of this.exchanges) {
      for (let api of exchange.apis) {
        const index = apiIds.indexOf(api.id)

        if (index !== -1) {
          exchange.reconnectApi(api)

          apiIds.splice(index, 1)

          if (!apiIds.length) {
            break
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

  dispatchRawTrades(exchange, data) {
    const now = Date.now()

    for (let i = 0; i < data.length; i++) {
      const trade = data[i]
      const identifier = exchange + ':' + trade.pair

      // ping connection
      connections[identifier].hit++
      connections[identifier].timestamp = now

      // save trade
      if (this.storages) {
        this.chunk.push(trade)
      }
    }

    if (config.broadcast) {
      if (config.broadcastAggr && !config.broadcastDebounce) {
        this.broadcastTrades(data)
      } else {
        Array.prototype.push.apply(this.delayedForBroadcast, data)
      }
    }
  }

  dispatchAggregateTrade(exchange, data) {
    const now = Date.now()
    const length = data.length

    for (let i = 0; i < length; i++) {
      const trade = data[i]
      const identifier = exchange + ':' + trade.pair

      // ping connection
      connections[identifier].hit++
      connections[identifier].timestamp = now

      // save trade
      if (this.storages) {
        this.chunk.push(trade)
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
    const now = Date.now()

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
