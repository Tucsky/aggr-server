const EventEmitter = require('events')
const WebSocket = require('websocket').w3cwebsocket
const fs = require('fs')
const { getIp, getHms, parsePairsFromWsRequest, groupTrades, formatAmount } = require('./helper')
const express = require('express')
const cors = require('cors')
const path = require('path')
const rateLimit = require('express-rate-limit')
const bodyParser = require('body-parser')
const config = require('./config')
const alertService = require('./services/alert')
const socketService = require('./services/socket')
const {
  connections,
  registerConnection,
  registerIndexes,
  restoreConnections,
  recovering,
  updateConnectionStats,
  dumpConnections,
} = require('./services/connections')

class Server extends EventEmitter {
  constructor(exchanges) {
    super()

    this.exchanges = exchanges || []
    this.storages = null
    this.globalUsage = {
      tick: 0,
      sum: 0,
      points: [],
    }

    /**
     * raw trades ready to be persisted into storage next save
     * @type Trade[]
     */
    this.chunk = []

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

      restoreConnections().then(() => {
        this.connectExchanges()
      })

      // profile exchanges connections (keep alive)
      this._activityMonitoringInterval = setInterval(this.monitorExchangesActivity.bind(this, Date.now()), 1000 * 60)
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

        // monitor data requests
        this._usageMonitorInterval = setInterval(this.monitorUsage.bind(this), 10000)
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
    // console.log(`[server] saving ${chunk.length} trades to storages`)

    return Promise.all(
      this.storages.map((storage) => {
        if (exitBackup) {
          console.debug(`[server/exit] saving ${chunk.length} trades into ${storage.constructor.name}`)
        }
        return storage
          .save(chunk, exitBackup)
          .then(() => {
            if (exitBackup) {
              console.debug(`[server/exit] performed backup of ${chunk.length} trades into ${storage.constructor.name}`)
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

        let lastPing = ''

        if (connections[id].timestamp) {
          lastPing = ' (last ping ' + new Date(+connections[id].timestamp).toISOString() + ')'
        }

        console.log(`[connections] ${id}${lastPing} disconnected from ${apiId} (${apiLength} remaining)`)

        connections[id].apiId = null
      })

      exchange.on('connected', (pair, apiId, apiLength) => {
        const id = exchange.id + ':' + pair

        registerConnection(id, exchange.id, pair, apiLength)

        if (typeof exchange.getMissingTrades === 'function') {
          exchange.registerRangeForRecovery(connections[id])
        }

        let lastPing = ''

        if (connections[id].timestamp) {
          lastPing =
            ' (last ping ' +
            new Date(+connections[id].timestamp).toISOString() +
            ', ' +
            connections[id].lastConnectionMissEstimate +
            ' estimated miss)'
        }

        console.log(`[connections] ${id}${lastPing} connected to ${apiId} (${apiLength} total)`)

        connections[id].apiId = apiId
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
            `[${exchange.id}] api closed unexpectedly (${apiId}, ${event.code}, ${event.reason || 'no reason'}) (was handling ${pairs.join(',')})`
          )

          setTimeout(() => {
            this.reconnectApis([apiId], 'unexpected close')
          }, 1000)
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

      const origin = typeof req.headers['origin'] !== 'undefined' ? req.headers['origin'].toString() : 'undefined'

      if (typeof origin === 'undefined' || (!new RegExp(config.origin).test(origin) && config.whitelist.indexOf(user) === -1)) {
        console.log(`[${user}/BLOCKED] socket origin mismatch "${origin}"`)
        return res.status(500).send('💀')
      } else if (this.BANNED_IPS.indexOf(user) !== -1) {
        console.debug(`[${user}/BANNED] at "${req.url}" (origin was ${origin})`)

        return res.status(500).send('💀')
      } else {
        next()
      }
    })

    app.get('/', function (req, res) {
      res.json({
        message: 'hi',
      })
    })

    if (alertService.enabled) {
      app.use(bodyParser.json())

      app.post('/alert', async (req, res) => {
        const user = req.headers['x-forwarded-for'] || req.connection.remoteAddress
        const alert = req.body

        if (!alert || !alert.endpoint || !alert.keys || typeof alert.market !== 'string' || typeof alert.price !== 'number') {
          return res.status(400).json({ error: 'invalid alert payload' })
        }

        alert.user = user

        try {
          const data = await alertService.toggleAlert(alert)
          res.status(201).json(data || {})
        } catch (error) {
          console.error(`[alert] couldn't toggle user alert because ${error.message}`)
          res.status(400).json({ error: error.message })
        }
      })
    }

    app.get('/products', (req, res) => {
      let products = config.extraProducts

      if (socketService.clusteredCollectors.length) {
        // node is a cluster

        products = products.concat(socketService.clusteredCollectors
          .reduce((acc, collectorSocket) => acc.concat(collectorSocket.markets), [])
          .filter((x, i, a) => a.indexOf(x) == i))
      } else {
        products = products.concat(config.pairs)
      }

      res.json(products)
    })

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

        if (config.influxResampleTo.indexOf(timeframe) === -1) {
          return res.status(400).json({
            error: 'unknown timeframe',
          })
        }

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

      this.globalUsage.tick += length * markets.length
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

          if (output.results.length > 10000) {
            console.log(
              `[${user}/${req.get('origin')}] ${getHms(to - from)} (${markets.length} markets, ${getHms(timeframe, true)} tf) -> ${
                +length ? parseInt(length) + ' bars into ' : ''
              }${output.results.length} ${storage.format}s, took ${getHms(Date.now() - fetchStartAt)}`
            )
          }

          return res.status(200).json(output)
        })
        .catch((err) => {
          return res.status(500).json({
            error: err.message,
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

  monitorUsage() {
    const tick = this.globalUsage.tick
    this.globalUsage.points.push(tick)
    this.globalUsage.sum += tick
    this.globalUsage.tick = 0
    
    if (this.globalUsage.length > 90) {
      this.globalUsage.sum -= this.globalUsage.shift()
    }

    const avg = this.globalUsage.sum / this.globalUsage.points.length

    if (tick) {
      console.log(`[usage] ${formatAmount(tick)} points requested (${formatAmount(avg)} avg)`)
    }
  }

  connectExchanges() {
    if (!this.exchanges.length || !config.pairs.length) {
      return
    }

    this.chunk = []

    for (const exchange of this.exchanges) {
      const exchangePairs = config.pairs.filter((pair) => pair.indexOf(':') === -1 || new RegExp('^' + exchange.id + ':').test(pair))

      if (!exchangePairs.length) {
        continue
      }

      exchange.getProductsAndConnect(exchangePairs)
    }

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

  async connect(markets) {
    markets = markets.filter((market) => {
      if (config.pairs.indexOf(market) !== -1) {
        return false
      }

      return true
    })

    if (!markets.length) {
      throw new Error('nothing to connect')
    }

    const results = []

    for (const exchange of this.exchanges) {
      const exchangeMarkets = markets.filter((market) => {
        const [exchangeId] = (market.match(/([^:]*):(.*)/) || []).slice(1, 3)

        return exchange.id === exchangeId
      })

      if (exchangeMarkets.length) {
        try {
          await exchange.getProducts(true)
        } catch (error) {
          console.error(`[server.connect] failed to retrieve ${exchange.id}'s products: ${error.message}`)
        }

        for (let market of exchangeMarkets) {
          try {
            await exchange.link(market, true)
            config.pairs.push(market)
            results.push(`${market} ✅`)
          } catch (error) {
            console.error(error.message)
            results.push(`${market} ❌ (${error.message})`)
          }
        }
      }
    }

    if (!results.length) {
      throw new Error('nothing was done')
    } else {
      registerIndexes()
      socketService.syncMarkets()

      this.savePairs()
    }

    return results
  }

  async disconnect(markets) {
    markets = markets.filter((market) => {
      if (config.pairs.indexOf(market) === -1) {
        return false
      }

      return true
    })

    if (!markets.length) {
      throw new Error('nothing to disconnect')
    }

    const results = []

    for (let i = 0; i < markets.length; i++) {
      const market = markets[i]
      const marketIndex = config.pairs.indexOf(market)

      const [exchangeId] = market.match(/([^:]*):(.*)/).slice(1, 3)

      for (const exchange of this.exchanges) {
        if (exchange.id === exchangeId) {
          try {
            await exchange.unlink(market)
            config.pairs.splice(marketIndex, 1)
            results.push(`${market} ✅`)
          } catch (error) {
            console.error(error.message)
            results.push(`${market} ❌ (${error.message})`)
          }
          break
        }
      }
    }

    if (!results.length) {
      throw new Error('nothing was done')
    } else {
      registerIndexes()
      socketService.syncMarkets()

      this.savePairs()
    }

    return results
  }

  savePairs(isScheduled = false) {
    if (!isScheduled) {
      if (this._savePairsTimeout) {
        clearTimeout(this._savePairsTimeout)
      }

      this._savePairsTimeout = setTimeout(this.savePairs.bind(this, true), 1000 * 60)

      return
    }

    if (!config.configPath) {
      console.warn(`[server] couldn't save config because configPath isn't known`)
      return Promise.resolve()
    }

    return new Promise((resolve, reject) => {
      fs.readFile(config.configPath, 'utf8', (err, rawConfig) => {
        if (err) {
          return reject(new Error(`failed to read config file (${err.message})`))
        }

        const jsonConfig = JSON.parse(rawConfig)

        jsonConfig.pairs = config.pairs

        fs.writeFile(config.configPath, JSON.stringify(jsonConfig, null, '\t'), (err) => {
          if (err) {
            return reject(new Error(`failed to write config file (${err.message})`))
          }

          console.log(`[server] saved active pairs in ${config.configPath}`)

          resolve()
        })
      })
    })
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

  reconnectApis(apiIds, reason) {
    for (let exchange of this.exchanges) {
      for (let api of exchange.apis) {
        const index = apiIds.indexOf(api.id)

        if (index !== -1) {
          exchange.reconnectApi(api, reason)

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
    for (let i = 0; i < data.length; i++) {
      const trade = data[i]

      if (!trade.liquidation) {
        const identifier = exchange + ':' + trade.pair

        // ping connection
        connections[identifier].hit++
        connections[identifier].timestamp = trade.timestamp
      }

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

  dispatchAggregateTrade(exchange, data, apiId) {
    const now = Date.now()
    const length = data.length

    for (let i = 0; i < length; i++) {
      const trade = data[i]
      const identifier = exchange + ':' + trade.pair

      if (!trade.liquidation) {
        // ping connection
        connections[identifier].hit++
        connections[identifier].timestamp = trade.timestamp
      }

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

  monitorExchangesActivity() {
    const now = Date.now()

    const staleConnections = []
    const apisToReconnect = []

    for (const id in connections) {
      if (!connections[id].apiId) {
        continue
      }

      updateConnectionStats(connections[id])

      if (now - connections[id].ping > connections[id].thrs && apisToReconnect.indexOf(connections[id].apiId) === -1) {
        // connection ping threshold reached
        staleConnections.push(connections[id])
        apisToReconnect.push(connections[id].apiId)
        continue
      }
    }

    if (apisToReconnect.length) {
      dumpConnections(staleConnections)
      this.reconnectApis(apisToReconnect, `reconnection threshold reached`)
    }
  }

  canExit() {
    let output = true

    for (const exchange of this.exchanges) {
      if (recovering[exchange.id]) {
        console.error(`Exchange is recovering trades, don't quit while it's doing its thing because all will be lost`)
        output = false
        break
      }
    }

    if (!output) {
      if (!this.exitAttempts) {
        this.exitAttempts = 0
      }
      this.exitAttempts++
      if (this.exitAttempts === 3) {
        console.error(`[server] last warning.`)
      } else if (this.exitAttempts === 4) {
        output = true
      }
    }

    return output
  }
}

module.exports = Server
