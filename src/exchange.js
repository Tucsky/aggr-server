const EventEmitter = require('events')
const WebSocket = require('websocket').w3cwebsocket
const config = require('./config')

const { ID, getHms, sleep, humanReadyState } = require('./helper')
const { readProducts, fetchProducts, saveProducts } = require('./services/catalog')
const { connections, recovering, dumpConnections } = require('./services/connections')

require('./typedef')

class Exchange extends EventEmitter {
  constructor() {
    super()

    /**
     * ping timers
     * @type {{[url: string]: number}}
     */
    this.keepAliveIntervals = {}

    /**
     * active websocket apis
     * @type {WebSocket[]}
     */
    this.apis = []

    /**
     * promises of ws. opens
     * @type {{[apiId: string]: {promise: Promise<void>, resolver: Function}}}
     */
    this.connecting = {}

    /**
     * promises of ws. closes
     * @type {{[apiId: string]: {promise: Promise<void>, resolver: Function}}}
     */
    this.disconnecting = {}

    /**
     * Operations timeout id by operationId
     * @type {{[operationId: string]: number]}}
     */
    this.scheduledOperations = {}

    /**
     * Operation timeout delay by operationId
     * @type {{[operationId: string]: number]}}
     */
    this.scheduledOperationsDelays = {}

    /**
     * Promises of api reconnection
     * @type {{[apiId: string]: WebSocket]}}
     */
    this.promisesOfApiReconnections = {}

    /**
     * @type {number}
     */
    this.failedConnections = 0

    /**
     * Max connections per apis
     * @type {number}
     */
    this.maxConnectionsPerApi = null

    /**
     * Define if the incoming trades should be queued
     * @type {boolean}
     */
    this.shouldQueueTrades = false

    /**
     * Pending recovery ranges
     * @type {{pair: string, from: number, to: number}[]}
     */
    this.recoveryRanges = []

    /**
     * Trades goes theres while we wait for historical response
     * @type {Trade[]}
     */
    this.queuedTrades = []
  }

  /**
   * Get exchange equivalent for a given pair
   * @param {string} pair
   */
  isMatching(pair) {
    if (!this.products || !this.products.length) {
      console.debug(`[${this.id}.isMatching] couldn't match ${pair}, exchange has no products`)
      return false
    }

    if (this.products.indexOf(pair) === -1) {
      console.debug(`[${this.id}.isMatching] couldn't match ${pair}`)

      const caseInsencitiveMatch = this.products.filter(
        (exchangePair) => exchangePair.toLowerCase().replace(/[^a-z]/g, '') === pair.toLowerCase().replace(/[^a-z]/g, '')
      )

      if (caseInsencitiveMatch.length) {
        console.debug(`\t did you write it correctly ? (found ${caseInsencitiveMatch.join(', ')})`)
      }

      return false
    }

    return true
  }

  /**
   * Get exchange ws url
   */
  getUrl(pair) {
    if (typeof this.url === 'function') {
      return this.url(pair)
    }

    return Promise.resolve(this.url)
  }

  /**
   * Link exchange to a pair
   * @param {*} pair
   * @returns {Promise<WebSocket>}
   */
  async link(pair, returnConnectedEvent) {
    pair = pair.replace(/[^:]*:/, '')

    if (!this.isMatching(pair)) {
      throw new Error(`${pair} did NOT match with any existing symbol on ${this.id}`)
    }

    console.debug(`[${this.id}.link] connecting ${pair}`)

    const api = await this.resolveApi(pair)

    if (returnConnectedEvent) {
      let promiseOfApiOpen

      if (api && this.connecting[api.id]) {
        // need to init new ws connection
        promiseOfApiOpen = this.connecting[api.id].promise
      } else {
        // api already opened
        promiseOfApiOpen = Promise.resolve(true)
      }

      if (await promiseOfApiOpen) {
        return new Promise((resolve) => {
          let timeout

          const connectedEventHandler = (connectedPair) => {
            if (connectedPair === pair) {
              clearTimeout(timeout)
              this.off('connected', connectedEventHandler)

              resolve()
            }
          }

          this.on('connected', connectedEventHandler)

          timeout = setTimeout(() => {
            console.error(`[${this.id}/link] ${pair} connected event never fired, resolving returnConnectedEvent immediately`)
            connectedEventHandler(pair)
          }, 10000)
        })
      }
    }
  }

  async resolveApi(pair) {
    const url = await this.getUrl(pair)
    let api = this.getActiveApiByUrl(url)

    if (!api) {
      api = this.createWs(url, pair)
    } else {
      console.debug(`[${this.id}.resolveApi] use existing api (api is ${humanReadyState(api.readyState)})`)
    }

    if (api._pending.indexOf(pair) !== -1) {
      console.warn(`[${this.id}.resolveApi] ${pair}'s api is already connecting to ${pair}`)
      return
    }

    if (api._connected.indexOf(pair) !== -1) {
      console.warn(`[${this.id}.resolveApi] ${pair}'s api is already connected to ${pair}`)
      return
    }

    api._pending.push(pair)

    if (api.readyState === WebSocket.OPEN) {
      this.schedule(
        () => {
          this.subscribePendingPairs(api)
        },
        'subscribe-' + api.url,
        1000
      )
    }

    return api
  }

  createWs(url, pair) {
    const api = new WebSocket(url)
    api.id = ID()

    console.log(`[${this.id}.createWs] initiate new ws connection ${url} (${api.id})`)

    api.binaryType = 'arraybuffer'

    api._connected = []
    api._pending = []

    this.apis.push(api)

    api._send = api.send
    api.send = (data) => {
      if (api.readyState !== WebSocket.OPEN) {
        console.error(`[${this.id}.createWs] attempted to send data to an non-OPEN websocket api`, data)
        return
      }

      if (!/ping|pong/.test(data)) {
        console.debug(`[${this.id}.createWs] sending ${data.substr(0, 64)}${data.length > 64 ? '...' : ''} to ${api.url}`)
      }

      api._send.apply(api, [data])
    }

    api.onmessage = (event) => {
      const wasBadData = !this.onMessage(event, api)

      if (
        wasBadData &&
        event.data &&
        /\b(unrecognized|failure|invalid|error|expired|cannot|exceeded|error|alert|bad|please|warning)\b/.test(event.data)
      ) {
        console.error(`[${this.id}] error message intercepted\n`, event.data)
      }
    }

    api.onopen = (event) => {
      this.onOpen(event, api)
    }

    api.onclose = (event) => {
      this.onClose(event, api)
    }

    api.onerror = (event) => {
      this.onError(event, api)

      if (event.target.readyState === WebSocket.CLOSING) {
        this.onClose(event, api)
      }
    }

    this.connecting[api.id] = {}

    this.connecting[api.id].promise = new Promise((resolve, reject) => {
      this.connecting[api.id].resolver = (success) => {
        if (success) {
          this.onApiCreated(api)
          resolve(api)
        } else {
          reject(new Error('Failed to establish a websocket connection with exchange'))
        }
      }
    })

    return api
  }

  async subscribePendingPairs(api) {
    console.debug(
      `[${this.id}.subscribePendingPairs] subscribe to ${api._pending.length} pairs of api ${api.url} (${api._pending.join(', ')})`
    )

    const pairsToConnect = api._pending.slice()

    for (const pair of pairsToConnect) {
      await this.subscribe(api, pair)
    }
  }

  /**
   * Unlink a pair
   * @param {string} pair
   * @param {boolean} skipSending skip sending unsusbribe message
   * @returns {Promise<void>}
   */
  async unlink(pair, skipSending = false) {
    pair = pair.replace(/[^:]*:/, '')

    const api = this.getActiveApiByPair(pair)

    if (!api) {
      return
    }

    if (api._connected.indexOf(pair) === -1 && api._pending.indexOf(pair) === -1) {
      return
    }

    console.debug(`[${this.id}.unlink] disconnecting ${pair} ${skipSending ? '(skip sending)' : ''}`)

    await this.unsubscribe(api, pair, skipSending)

    if (!api._connected.length) {
      console.debug(`[${this.id}.unlink] ${pair}'s api is now empty (trigger close api)`)
      return this.removeWs(api)
    } else {
      return
    }
  }

  /**
   * Get active websocket api by pair
   * @param {string} pair
   * @returns {WebSocket}
   */
  getActiveApiByPair(pair) {
    for (let i = 0; i < this.apis.length; i++) {
      if (this.apis[i]._connected.indexOf(pair) !== -1 || this.apis[i]._pending.indexOf(pair) !== -1) {
        return this.apis[i]
      }
    }
  }

  /**
   * Get active websocket api by url
   * @param {string} url
   * @returns {WebSocket}
   */
  getActiveApiByUrl(url) {
    for (let i = 0; i < this.apis.length; i++) {
      if (
        this.apis[i].readyState < 2 &&
        this.apis[i].url === url &&
        (!this.maxConnectionsPerApi || this.apis[i]._connected.length + this.apis[i]._pending.length < this.maxConnectionsPerApi)
      ) {
        return this.apis[i]
      }
    }
  }

  /**
   * Close websocket api
   * @param {WebSocket} api
   * @returns {Promise<void>}
   */
  removeWs(api) {
    let promiseOfClose

    if (api.readyState !== WebSocket.CLOSED) {
      if (api._connected.length) {
        throw new Error(`cannot unbind api that still has pairs linked to it`)
      }

      console.debug(`[${this.id}.removeWs] close api ${api.id}`)

      this.disconnecting[api.id] = {}

      this.disconnecting[api.id].promise = new Promise((resolve, reject) => {
        this.disconnecting[api.id].resolver = (success) => (success ? resolve() : reject())

        if (api.readyState < WebSocket.CLOSING) {
          api.close()
        }
      })

      promiseOfClose = this.disconnecting[api.id].promise

      setTimeout(() => {
        // do NOT wait for exchange to close it
        this.onClose(
          {
            code: 'none',
            reason: 'forced close',
          },
          api
        )
      })
    } else {
      promiseOfClose = Promise.resolve()
    }

    return promiseOfClose.then(() => {
      console.log(`[${this.id}] remove api ${api.url}`)
      this.onApiRemoved(api)
      this.apis.splice(this.apis.indexOf(api), 1)
    })
  }

  /**
   * Reconnect api
   * @param {WebSocket} api
   * @param {String?} reason
   */
  async reconnectApi(api, reason) {
    if (this.promisesOfApiReconnections[api.id]) {
      return this.promisesOfApiReconnections[api.id]
    }

    api.onmessage = null

    console.debug(
      `[${this.id}.reconnectApi] reconnect api ${api.id}${reason ? ' reason: ' + reason : ''} (url: ${
        api.url
      }, _connected: ${api._connected.join(', ')}, _pending: ${api._pending.join(', ')})`
    )

    const pairsToReconnect = [...api._pending, ...api._connected]

    const affectedConnections = {}

    for (const pair of pairsToReconnect) {
      const key = this.id + ':' + pair
      const connection = connections[key]

      if (connection) {
        affectedConnections[key] = connection
      }
    }

    dumpConnections(affectedConnections)

    this.promisesOfApiReconnections[api.id] = this.reconnectPairs(pairsToReconnect).then(() => {
      console.log(`[${this.id}.reconnectApi] done reconnecting api (was ${api.id}${reason ? ' because of ' + reason : ''})`)
      delete this.promisesOfApiReconnections[api.id]
    })

    return this.promisesOfApiReconnections[api.id]
  }

  /**
   * Register a range for async recovery
   * @param {Connection} connection to recover
   */
  registerRangeForRecovery(connection) {
    if (!connection.timestamp) {
      return
    }

    if (!connection.forceRecovery && connection.lastConnectionMissEstimate < 10) {
      return
    } else if (connection.forceRecovery) {
      delete connection.forceRecovery
    }

    const now = Date.now()

    const range = {
      pair: connection.pair,
      from: connection.timestamp,
      to: now,
      missEstimate: connection.lastConnectionMissEstimate,
    }

    this.recoveryRanges.push(range)

    if (!recovering[this.id]) {
      this.recoverNextRange()
    }
  }

  async recoverNextRange(sequencial) {
    if (!this.recoveryRanges.length || (recovering[this.id] && !sequencial)) {
      return
    }

    const range = this.recoveryRanges.shift()

    const missingTime = range.to - range.from

    console.log(
      `[${this.id}.recoverTrades] get missing trades for ${range.pair} (expecting ${range.missEstimate} on a ${getHms(
        missingTime
      )} blackout going from ${new Date(range.from).toISOString().split('T').pop()} to ${new Date(range.to)
        .toISOString()
        .split('T')
        .pop()})`
    )

    const connection = connections[this.id + ':' + range.pair]

    recovering[this.id] = true

    try {
      const recoveredCount = await this.getMissingTrades(range)

      if (recoveredCount) {
        console.info(
          `[${this.id}.recoverTrades] recovered ${recoveredCount} (expected ${range.missEstimate}) trades on ${this.id}:${
            range.pair
          } (${getHms(missingTime - (range.to - range.from))} recovered out of ${getHms(missingTime)} | ${getHms(
            range.to - range.from
          )} remaining)`
        )
      } else {
        console.info(
          `[${this.id}.recoverTrades] 0 trade recovered on ${range.pair} (expected ${range.missEstimate} for ${getHms(
            missingTime
          )} blackout)`
        )
      }

      if (connection) {
        // save new timestamp to connection
        if (connection.timestamp && connection.timestamp > range.to) {
          ;`[${this.id}.recoverTrades] ${range.pair} trade recovery is late on the schedule (last emitted trade: ${new Date(
            +connection.timestamp
          ).toISOString()}, last recovered trade: ${new Date(+range.to).toISOString()})`
        }
        connection.timestamp = range.to
      }

      // in rare case of slow recovery and fast reconnection happening, propagate to pending ranges for that pair
      for (let i = 0; i < this.recoveryRanges.length; i++) {
        const nextRange = this.recoveryRanges[i]

        if (nextRange.pair === range.pair) {
          const newFrom = Math.max(nextRange.from, range.from)

          if (nextRange.from !== newFrom) {
            nextRange.from = Math.max(nextRange.from, range.from)

            if (nextRange.from > nextRange.to) {
              this.recoveryRanges.splice(i--, 1)
              continue
            }
          }
        }
      }
    } catch (error) {
      console.error(`[${this.id}.recoverTrades] something went wrong while recovering ${range.pair}'s missing trades`, error.message)
    }

    if (!this.recoveryRanges.length) {
      console.log(`[${this.id}] no more ranges to recover`)

      delete recovering[this.id]

      if (this.queuedTrades.length) {
        const sortedQueuedTrades = this.queuedTrades.sort((a, b) => a.timestamp - b.timestamp)

        console.log(
          `[${this.id}] release trades queue (${sortedQueuedTrades.length} trades, ${new Date(
            +sortedQueuedTrades[0].timestamp
          ).toISOString().split('T').pop()} to ${new Date(+sortedQueuedTrades[sortedQueuedTrades.length - 1].timestamp).toISOString().split('T').pop()})`
        )
        this.emit('trades', sortedQueuedTrades)
        this.queuedTrades = []
      }
    } else {
      return this.waitBeforeContinueRecovery().then(() => this.recoverNextRange(true))
    }
  }

  /**
   * Reconnect pairs
   * @param {string[]} pairs (local)
   * @returns {Promise<any>}
   */
  async reconnectPairs(pairs) {
    const pairsToReconnect = pairs.slice(0, pairs.length)

    console.info(`[${this.id}.reconnectPairs] reconnect pairs ${pairsToReconnect.join(',')}`)

    console.debug(`[${this.id}.reconnectPairs] unlinking ${pairsToReconnect.length} pairs`)

    for (let pair of pairsToReconnect) {
      await this.unlink(this.id + ':' + pair, true)
    }

    const promisesOfSubscriptions = []

    console.debug(`[${this.id}.reconnectPairs] linking ${pairsToReconnect.length} pairs`)

    for (let pair of pairsToReconnect) {
      promisesOfSubscriptions.push(this.link(this.id + ':' + pair))
    }

    console.info(`[${this.id}.reconnectPairs] reconnect pairs ${pairsToReconnect.join(',')}`)

    return Promise.all(promisesOfSubscriptions)
  }

  /**
   * Ensure product are fetched then connect to given pairs
   * @returns {Promise<any>}
   */
  async getProductsAndConnect(pairs, forceRefreshProducts = false) {
    try {
      await this.getProducts(forceRefreshProducts)
    } catch (error) {
      this.scheduledOperationsDelays.getProducts = this.schedule(
        () => {
          this.getProductsAndConnect(pairs, forceRefreshProducts)
        },
        'getProducts',
        4000,
        1.5,
        1000 * 60 * 3
      )

      return
    }

    for (let pair of pairs) {
      try {
        await this.link(pair)
      } catch (error) {
        console.error(error.message)
        // pair mismatch
      }
    }
  }

  /**
   * Get exchange products
   * @returns {Promise<void>}
   */
  async getProducts(forceRefreshProducts = false) {
    let formatedProducts

    if (!forceRefreshProducts) {
      try {
        formatedProducts = await readProducts(this.id)
      } catch (error) {
        console.error(`[${this.id}/getProducts] failed to read products`, error)
      }
    }

    if (!formatedProducts) {
      try {
        const rawProducts = await fetchProducts(this.id, this.endpoints)

        if (this.scheduledOperationsDelays.getProducts) {
          delete this.scheduledOperationsDelays.getProducts
        }

        if (rawProducts) {
          formatedProducts = this.formatProducts(rawProducts) || []

          await saveProducts(this.id, formatedProducts)
        }
      } catch (error) {
        console.error(`[${this.id}/getProducts] failed to fetch products`, error)
      }
    }

    if (formatedProducts) {
      if (typeof formatedProducts === 'object' && formatedProducts.hasOwnProperty('products')) {
        for (let key in formatedProducts) {
          this[key] = formatedProducts[key]
        }
      } else {
        this.products = formatedProducts
      }
    } else {
      console.error(`[${this.id}/getProducts] no stored products / no defined api endpoint`)

      this.products = null
    }
  }

  /**
   * Fire when a new websocket connection is created
   * @param {WebSocket} api WebSocket instance
   */
  onApiCreated(api) {
    // should be overrided by exchange class
  }

  /**
   * Fire when a new websocket connection has been removed
   * @param {WebSocket} api WebSocket instance
   */
  onApiRemoved(api) {
    // should be overrided by exchange class
  }

  /**
   * Fire when a new websocket connection received something
   * @param {Event} event
   * @param {WebSocket} api WebSocket instance
   */
  onMessage(event, api) {
    // should be overrided by exchange class
  }

  /**
   * Fire when a new websocket connection opened
   * @param {Event} event
   * @param {string[]} pairs pairs attached to ws at opening
   */
  async onOpen(event, api) {
    this.queueNextTrades()

    const pairs = [...api._pending, ...api._connected]

    console.debug(`[${this.id}.onOpen] opened api ${api.id} (${pairs.join(', ')})`)

    this.failedConnections = 0

    if (this.connecting[api.id]) {
      this.connecting[api.id].resolver(true)
      delete this.connecting[api.id]

      await sleep(50)
    }

    this.subscribePendingPairs(api)

    this.emit('open', api.id, pairs)
  }

  /**
   * Fire when a new websocket connection reported an error
   * @param {Event} event
   * @param {string[]} pairs
   */
  onError(event, api) {
    const pairs = [...api._pending, ...api._connected]

    console.error(`[${this.id}.onError] ${pairs.join(',')}'s api errored`, event.message)

    this.emit('error', api.id, event.message)
  }

  /**
   * Fire when a new websocket connection closed
   * @param {Event} event
   * @param {string[]} pairs
   */
  async onClose(event, api) {
    if (api._closeWasHandled) {
      // prevent double handling
      return
    }

    api._closeWasHandled = true

    if (this.connecting[api.id]) {
      this.failedConnections++
      const delay = 1000 * this.failedConnections
      console.debug(`[${this.id}] api refused connection, sleeping ${delay / 1000}s before trying again`)
      await sleep(delay)
      this.connecting[api.id].resolver(false)
      delete this.connecting[api.id]
    }

    if (this.disconnecting[api.id]) {
      this.disconnecting[api.id].resolver(true)
      delete this.disconnecting[api.id]
    }

    const pairs = [...api._pending, ...api._connected]

    console.log(`[${this.id}] ${pairs.join(',')}'s api closed`)

    this.emit('close', api.id, pairs, event)
  }

  /**
   *
   * @param {any} data products from HTTP response
   */
  formatProducts(data) {
    // should be overrided by exchange class

    return data
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async subscribe(api, pair) {
    if (!this.markPairAsConnected(api, pair)) {
      // pair is already attached
      return false
    }

    this.emit('connected', pair, api.id, api._connected.length)

    return true
  }

  /**
   * Unsub
   * @param {WebSocket} api
   * @param {string} pair
   * @param {boolean} skipSending skip sending unsusbribe message
   */
  async unsubscribe(api, pair, skipSending) {
    if (!this.markPairAsDisconnected(api, pair)) {
      // pair is already detached
      return false
    }

    this.emit('disconnected', pair, api.id, api._connected.length)

    return !skipSending && api.readyState === WebSocket.OPEN
  }

  /**
   * Emit trade to server
   * @param {string} source api id
   * @param {Trade[]} trades
   */
  emitTrades(source, trades) {
    /*if (source) {
      trades.forEach((trade) =>
        console.log('[feed]', new Date(trade.timestamp).toISOString(), trade.size, trade.liquidation ? '(!!) trade is a liquidation' : '')
      )
    }*/

    if (source && this.promisesOfApiReconnections[source]) {
      return
    }

    this.queueControl(source, trades, 'trades')

    return true
  }

  /**
   * Emit liquidations to server
   * @param {string} source api id
   * @param {Trade[]} trades
   */
  emitLiquidations(source, trades) {
    if (source && this.promisesOfApiReconnections[source]) {
      return
    }

    this.queueControl(source, trades, 'liquidations')

    return true
  }

  queueControl(source, trades, type) {
    if (!trades || !trades.length) {
      return null
    }

    if (this.shouldQueueTrades || recovering[this.id]) {
      Array.prototype.push.apply(this.queuedTrades, trades)
      return null
    }

    this.emit(type, trades, source)
  }

  startKeepAlive(api, payload = { event: 'ping' }, every = 30000) {
    if (this.keepAliveIntervals[api.id]) {
      this.stopKeepAlive(api.id)
    }

    console.debug(`[${this.id}] setup keepalive for ws ${api.url} (${api.id})`)

    this.keepAliveIntervals[api.id] = setInterval(() => {
      if (api.readyState === WebSocket.OPEN) {
        api.send(typeof payload === 'string' ? payload : JSON.stringify(payload))
      }
    }, every)
  }

  stopKeepAlive(api) {
    if (!this.keepAliveIntervals[api.id]) {
      return
    }

    console.debug(`[${this.id}] stop keepalive for ws ${api.url} (${api.id})`)

    clearInterval(this.keepAliveIntervals[api.id])
    delete this.keepAliveIntervals[api.id]
  }

  schedule(operationFunction, operationId, minDelay, delayMultiplier, maxDelay, currentDelay) {
    if (this.scheduledOperations[operationId]) {
      clearTimeout(this.scheduledOperations[operationId])
    }

    if (typeof currentDelay === 'undefined') {
      currentDelay = this.scheduledOperationsDelays[operationId]
    }

    currentDelay = Math.max(minDelay, currentDelay || 0)

    // console.debug(`[${this.id}] schedule ${operationId} in ${getHms(currentDelay)}`)

    this.scheduledOperations[operationId] = setTimeout(() => {
      // console.debug(`[${this.id}] schedule timer fired`)

      delete this.scheduledOperations[operationId]

      operationFunction()
    }, currentDelay)

    currentDelay *= delayMultiplier || 1

    if (typeof maxDelay === 'number' && minDelay > 0) {
      currentDelay = Math.min(maxDelay, currentDelay)
    }

    return currentDelay
  }

  markPairAsConnected(api, pair) {
    const pendingIndex = api._pending.indexOf(pair)

    if (pendingIndex !== -1) {
      api._pending.splice(pendingIndex, 1)
    } else {
      console.warn(`[${this.id}.markPairAsConnected] ${pair} appears to be NOT connecting anymore (prevent undesired subscription)`)
      return false
    }

    const connectedIndex = api._connected.indexOf(pair)

    if (connectedIndex !== -1) {
      return false
    }

    api._connected.push(pair)

    return true
  }

  markPairAsDisconnected(api, pair) {
    const pendingIndex = api._pending.indexOf(pair)

    if (pendingIndex !== -1) {
      // this shouldn't happen most of the time
      // but unlink(pair) can be called before during a ws.open which is the case we handle here

      console.debug(
        `[${this.id}.markPairAsDisconnected] ${pair} was NOT yet connected to api (prevent unsubscription of non connected pair)`
      )

      api._pending.splice(pendingIndex, 1)

      return false
    }

    const connectedIndex = api._connected.indexOf(pair)

    if (connectedIndex === -1) {
      console.debug(`[${this.id}.markPairAsDisconnected] ${pair} was NOT found in in the _connected list (prevent double unsubscription)`)
      return false
    }

    api._connected.splice(connectedIndex, 1)

    console.debug(`[${this.id}.markPairAsDisconnected] ${pair} removed from _connected list`)

    return true
  }

  queueNextTrades(duration = 100) {
    this.shouldQueueTrades = true

    if (this._unlockTimeout) {
      clearTimeout(this._unlockTimeout)
    }

    this._unlockTimeout = setTimeout(() => {
      this._unlockTimeout = null

      this.shouldQueueTrades = false
    }, duration)
  }

  /**
   * Wait between requests to prevent 429 HTTP errors
   * @returns {Promise<void>}
   */
  waitBeforeContinueRecovery() {
    if (!config.recoveryRequestDelay) {
      return Promise.resolve()
    }

    return sleep(config.recoveryRequestDelay)
  }
}

module.exports = Exchange
