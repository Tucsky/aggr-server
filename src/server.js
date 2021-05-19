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
     * delayed trades ready to be broadcasted next interval (see _broadcastDelayedTradesInterval)
     * @type Trade[]
     */
    this.delayed = []

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

    this.initStorages().then(() => {
      if (this.options.collect) {
        console.log(
          `\n[server] collect is enabled`,
          this.options.broadcast && this.options.broadcastAggr ? '\n\twill aggregate every trades that came on same ms (impact only broadcast)' : '',
          this.options.broadcast && this.options.broadcastDebounce ? `\n\twill broadcast trades every ${this.options.broadcastDebounce}ms` : (this.options.broadcast ? `will broadcast trades instantly` : '')
        )
        console.log(`\tconnect to -> ${this.exchanges.map((a) => a.id).join(', ')}`)

        this.handleExchangesEvents()
        this.connectExchanges()

        // profile exchanges connections (keep alive)
        this._activityMonitoringInterval = setInterval(this.monitorExchangesActivity.bind(this, +new Date()), 1000 * 60)

        if (this.storages) {
          const delay = this.scheduleNextBackup()

          console.log(
            `[server] scheduling first save to ${this.storages.map((storage) => storage.constructor.name)} in ${getHms(delay)}...`
          )
        }
      }

      if (this.options.api || this.options.broadcast) {
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

    return Promise.all(promises)
  }

  backupTrades(exitBackup) {
    if (!this.storages || !this.chunk.length) {
      this.scheduleNextBackup()
      return Promise.resolve()
    }

    const chunk = this.chunk.splice(0, this.chunk.length)

    return Promise.all(
      this.storages.map((storage) =>
        storage.save(chunk).then(() => {
          if (exitBackup) {
            console.log(`[server/exit] performed backup of ${chunk.length} trades into ${storage.constructor.name}`)
          }
        })
      )
    ).then(() => {
      if (!exitBackup) {
        this.scheduleNextBackup()
      }
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
      if (this.options.broadcastAggr) {
        exchange.on('trades', this.aggregateTrades.bind(this, exchange.id))
      } else {
        exchange.on('trades', this.dispatchTrades.bind(this, exchange.id))
      }

      exchange.on('liquidations', this.dispatchTrades.bind(this, exchange.id))

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

        // this.dumpSymbolsByExchanges() 
      })

      exchange.on('open', (event) => {
        this.broadcastJson({
          type: 'exchange_connected',
          id: exchange.id,
        })
      })

      exchange.on('disconnected', (pair, apiId) => {
        const id = exchange.id + ':' + pair

        if (!this.connections[id]) {
          throw new Error(`[server] couldn't delete connection ${id} because the connections[${id}] does not exists`)
          return
        }

        console.debug(`[server] deleted connection ${id}`)

        delete this.connections[id]
      })

      exchange.on('connected', (pair, apiId) => {
        const id = exchange.id + ':' + pair

        if (this.connections[id]) {
          throw new Error(`[server] couldn't register connection ${id} because the connections[${id}] does not exists`)
          return
        }

        console.debug(`[server] registered connection ${id}`)

        this.connections[id] = {
          apiId,
          exchange: exchange.id,
          pair: pair,
          hit: 0,
          timestamp: +new Date(),
        }
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

        this.dumpConnections()
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
        supportedPairs: Object.values(this.connection).map((a) => a.exchange + ':' + a.pair),
        timestamp: +new Date(),
        exchanges: this.exchanges.map((exchange) => {
          return {
            id: exchange.id,
            connected: exchange.pairs.length,
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
          return res.status(429).json({
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

      if (req.headers['origin'] && !new RegExp(this.options.origin).test(req.headers['origin'])) {
        console.error(`[${ip}/BLOCKED] socket origin mismatch "${req.headers['origin']}"`)
        setTimeout(() => {
          return res.status(500).json({
            error: 'ignored',
          })
        }, 5000 + Math.random() * 5000)
      } else if (this.BANNED_IPS.indexOf(ip) !== -1) {
        console.error(`[${ip}/BANNED] at "${req.url}" from "${req.headers['origin']}"`)

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

    app.get('/:pair/volume', (req, res) => {
      let pair = req.params.pair

      if (!/[a-zA-Z]+/.test(pair)) {
        return res.status(400).json({
          error: `invalid pair`,
        })
      }

      this.getRatio(pair)
        .then((ratio) => {
          res.status(500).json({
            pair,
            ratio,
          })
        })
        .catch((error) => {
          return res.status(500).json({
            error: `no info (${error.message})`,
          })
        })
    })

    app.get('/historical/:from/:to/:timeframe?/:markets([^/]*)?', (req, res) => {
      const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress
      let from = req.params.from
      let to = req.params.to
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

        from = Math.floor(from / timeframe) * timeframe
        to = Math.ceil(to / timeframe) * timeframe

        const length = (to - from) / timeframe

        if (length > this.options.maxFetchLength) {
          return res.status(400).json({
            error: 'too many bars',
          })
        }
      } else {
        from = parseInt(from)
        to = parseInt(to)
      }

      if (from > to) {
        let _from = parseInt(from)
        from = parseInt(to)
        to = _from
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
          if (to - from > 1000 * 60) {
            console.log(
              `[${ip}] requesting ${getHms(to - from)} (${output.length} ${storage.format}s, took ${getHms(+new Date() - fetchStartAt)})`
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
        .catch((error) => {
          return res.status(500).json({
            error: error.message,
          })
        })
    })

    this.server = app.listen(this.options.port, () => {
      console.log(
        `[server] http server listening at localhost:${this.options.port}`,
        !this.options.api ? '(historical api is disabled)' : ''
      )
    })

    this.app = app
  }

  dumpConnections() {
    if (typeof this._dumpConnectionsTimeout !== 'undefined') {
      clearTimeout(this._dumpConnectionsTimeout)
      delete this._dumpConnectionsTimeout
    }

    this._dumpConnectionsTimeout = setTimeout(() => {
      const structPairs = {}

      for (let id in this.connections) {
        const connection = this.connections[id]

        structPairs[connection.exchange + ':' + connection.pair] = {
          exchange: connection.exchange,
          pair: connection.pair,
          hit: connection.hit,
          ping: connection.hit ? ago(connection.timestamp) : 'never',
        }
      }

      console.table(structPairs)
    }, 1000)

    return Promise.resolve()
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
      this.dumpConnections()
    })

    if (this.options.broadcastDebounce && !this.options.broadcastAggr) {
      this._broadcastDelayedTradesInterval = setInterval(() => {
        if (!this.delayed.length) {
          return
        }

        this.broadcastTrades(this.delayed)

        this.delayed = []
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

    this.dumpConnections()
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
        if (!exchange.pairs.indexOf(pair) === -1) {
          console.log(`[server/disconnectPairs] ${pair} is NOT currently connected on ${exchange.id} exchange (warning)`)
          continue
        }

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

    this.dumpConnections()
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

    const groups = groupTrades(trades, true)

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

  monitorExchangesActivity(startedAt) {
    const now = +new Date()

    if (Math.round((now - startedAt) / (1000 * 60)) % 5 === 0) {
      this.dumpConnections()
    }

    const sources = []
    const activity = {}
    const pairs = {}

    for (let id in this.connections) {
      const connection = this.connections[id]

      if (!activity[connection.apiId]) {
        activity[connection.apiId] = []
        pairs[connection.apiId] = []
      }

      activity[connection.apiId].push(now - connection.timestamp)

      pairs[connection.apiId].push(connection.remote)
    }

    for (let source in activity) {
      const min = activity[source].length ? Math.min.apply(null, activity[source]) : 0

      if (min > this.options.reconnectionThreshold) {
        // one of the feed did not received any data since 1m or more
        // => reconnect api (and all the feed binded to it)

        console.log(
          `[warning] api ${source} reached reconnection threshold ${getHms(min)} > ${getHms(
            this.options.reconnectionThreshold
          )}\n\t-> reconnect ${pairs[source].join(', ')}`
        )

        sources.push(source)
      }
    }

    for (let exchange of this.exchanges) {
      for (let api of exchange.apis) {
        if (sources.indexOf(api.id) !== -1) {
          exchange.reconnectApi(api)

          sources.splice(sources.indexOf(api.id), 1)
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

  getInfo(pair) {
    const now = +new Date()

    let currencyLength = 3
    if (['USDT', 'TUSD'].indexOf(pair) === pair.length - 4) {
      currencyLength = 4
    }

    let fsym = pair.substr(-currencyLength, currencyLength)
    let tsym = pair.substr(0, pair.length - currencyLength)

    if (!/^(USD|BTC)/.test(tsym)) {
      tsym = 'USD'
    }

    pair = fsym + tsym

    if (this.symbols[pair]) {
      if (Math.floor((now - this.symbols[pair].time) / (1000 * 60 * 60 * 24)) > 1) {
        delete this.symbols[pair]
      } else {
        return Promise.resolve(this.symbols[pair].volume)
      }
    }

    return axios
      .get(
        `https://min-api.cryptocompare.com/data/symbol/histoday?fsym=${fsym}&tsym=${tsym}&limit=1&api_key=f640d9ddbd98b4cade666346aab18687d73720d2ede630bf8bacea710e08df55`
      )
      .then((response) => response.data)
      .then((data) => {
        if (!data || !data.Data || !data.Data.length) {
          throw new Error(`no data`)
        }

        data.Data.Data[0].time *= 1000

        this.symbols[pair] = data.Data.Data[0]

        return {
          data: this.symbols[pair],
          base: fsym,
          quote: tsym,
        }
      })
  }

  getRatio(pair) {
    return this.getInfo(pair).then(async ({ data, base, quote }) => {
      const BTC = await this.getInfo('BTCUSD')

      if (quote === 'BTC') {
        return data.volumeTo / BTC.volumeFrom
      } else {
        return data.volumeFrom / BTC.volumeTo
      }
    })
  }

  dispatchTrades(exchange, { source, data }) {
    for (let i = 0; i < data.length; i++) {
      // push for storage...
      if (this.storages) {
        this.chunk.push(data[i])
      }
    }

    if (!this.options.broadcastDebounce || this.options.broadcastAggr) {
      this.broadcastTrades(data)
    } else {
      Array.prototype.push.apply(this.delayed, data)
    }
  }

  aggregateTrades(exchange, { source, data }) {
    const now = +new Date()
    const length = data.length

    for (let i = 0; i < length; i++) {
      const trade = data[i]
      const identifier = exchange + ':' + trade.pair

      if (!this.connections[identifier]) {
        console.error(`UNKNOWN TRADE SOURCE`, trade)
        console.info('This trade will be ignored.')
        continue
      }

      this.connections[identifier].hit++
      this.connections[identifier].timestamp = now

      // push for storage...
      if (this.storages) {
        this.chunk.push(data[i])
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

  /**
   * For debug only 
   */
  dumpSymbolsByExchanges() {
    const symbols = Object.keys(this.indexedProducts)

    fs.writeFileSync(
      './symbols',
      symbols.reduce((output, symbol) => {
        output += `${symbol} (${this.indexedProducts[symbol].exchanges.join(',')})\n`

        return output
      }, ''),
      { encoding: 'utf8', flag: 'w' }
    )
  }
}

module.exports = Server
