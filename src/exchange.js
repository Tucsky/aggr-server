const EventEmitter = require('events')
const axios = require('axios')
const WebSocket = require('ws')
const fs = require('fs')

const { ID, getHms, ensureDirectoryExists, sleep } = require('./helper')

require('./typedef')

class Exchange extends EventEmitter {
  constructor(options) {
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
     * Operations timeout delay by operationId
     * @type {{[operationId: string]: number]}}
     */
    this.scheduledOperations = {}

    /**
     * Operation timeout delay by operationId
     * @type {{[operationId: string]: number]}}
     */
    this.scheduledOperationsDelays = {}

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

    this.options = Object.assign(
      {
        // default exchanges options
      },
      options || {}
    )
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
  getUrl() {
    return typeof this.options.url === 'function' ? this.options.url.apply(this, arguments) : this.options.url
  }

  /**
   * Link exchange to a pair
   * @param {*} pair
   * @returns {Promise<WebSocket>}
   */
  async link(pair) {
    pair = pair.replace(/[^:]*:/, '')

    if (!this.isMatching(pair)) {
      return Promise.reject(`${this.id} couldn't match with ${pair}`)
    }

    console.debug(`[${this.id}.link] connecting ${pair}`)

    this.resolveApi(pair)
  }

  resolveApi(pair) {
    let api = this.getActiveApiByUrl(this.getUrl(pair))

    if (!api) {
      api = this.createWs(pair)
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

  createWs(pair) {
    const url = this.getUrl(pair)

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
        if (api.readyState < WebSocket.CLOSING) {
          api.close()
        }

        this.disconnecting[api.id].resolver = (success) => (success ? resolve() : reject())
      })

      promiseOfClose = this.disconnecting[api.id].promise
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
   * Will try to fix missing trades using REST api
   * @param {WebSocket} api
   */
  async reconnectApi(api) {
    console.debug(
      `[${this.id}.reconnectApi] reconnect api (url: ${api.url}, _connected: ${api._connected.join(', ')}, _pending: ${api._connected.join(
        ', '
      )})`
    )

    const pairsToReconnect = [...api._pending, ...api._connected]

    // first we reconnect everything
    this.reconnectPairs(pairsToReconnect)
  }

  /**
   * Register a range for async recovery
   * @param {{ pair: string, from: number, to: number }} range to recover
   */
  registerRangeForRecovery(range) {
    this.recoveryRanges.push(range)

    if (!this.busyRecovering) {
      this.recoverNextRange()
    }
  }

  async recoverNextRange(sequencial) {
    if (!this.recoveryRanges.length || (this.busyRecovering && !sequencial)) {
      return
    }

    this.busyRecovering = true

    const range = this.recoveryRanges.shift()
    const missingTime = range.to - range.from

    console.log(
      `[${this.id}.recoverTrades] get missing trades for ${range.pair} (${getHms(missingTime)} of data | starting at ${new Date(range.from)
        .toISOString()
        .split('T')
        .pop()}, ending at ${new Date(range.to).toISOString().split('T').pop()})`
    )

    try {
      const recoveredCount = await this.getMissingTrades(range)
      console.info(
        `[${this.id}.recoverTrades] recovered ${recoveredCount} trades on ${this.id}:${range.pair} (${getHms(
          missingTime - (range.to - range.from)
        )} recovered out of ${getHms(missingTime)} | ${getHms(range.to - range.from)} remaining)`
      )
    } catch (error) {
      console.error(`[${this.id}.recoverTrades] something went wrong while recovering ${range.pair}'s missing trades`, error.message)
    }

    if (!this.recoveryRanges.length) {
      console.log(`[${this.id}] no more ranges to recover`)
      this.busyRecovering = false // release
    } else {
      console.log(`[${this.id}] ${this.recoveryRanges.length} ranges left to recover`)
      return this.recoverNextRange(true)
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

    for (let pair of pairsToReconnect) {
      await this.unlink(this.id + ':' + pair, true)
    }

    const promisesOfSubscriptions = []

    for (let pair of pairsToReconnect) {
      promisesOfSubscriptions.push(this.link(this.id + ':' + pair))
    }

    return Promise.all(promisesOfSubscriptions)
  }

  /**
   * Ensure product are fetched then connect to given pairs
   * @returns {Promise<any>}
   */
  async getProductsAndConnect(pairs) {
    try {
      await this.getProducts()
    } catch (error) {
      this.scheduledOperationsDelays.getProducts = this.schedule(
        () => {
          this.getProductsAndConnect(pairs)
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
        // pair mismatch
      }
    }
  }

  /**
   * Read products from file (products/*exchange id*.json)
   * @returns {Promise<any>} Formated products
   */
  async readProducts() {
    console.debug(`[${this.id}] reading stored products...`)

    return new Promise((resolve, reject) => {
      fs.readFile('products/' + this.id + '.json', (err, raw) => {
        if (err) {
          console.debug(`[${this.id}] no stored products`)
          return resolve(null) // no products returned = will fetch
        }

        try {
          const { expiration, data } = JSON.parse(raw)

          if (!data) {
            throw new Error('invalid exchanges products')
          }

          const now = new Date()

          if (+now > expiration) {
            return resolve(null)
          }

          console.debug(`[${this.id}] using stored products`)

          resolve(data)
        } catch (error) {
          reject(error)
        }
      })
    })
  }

  /**
   * Get products from api endpoint(s) and save to file for 7 days
   * @returns Formated products
   */
  async fetchProducts() {
    if (!this.endpoints || !this.endpoints.PRODUCTS) {
      if (!this.products) {
        this.products = []
      }

      return Promise.resolve()
    }

    let urls = typeof this.endpoints.PRODUCTS === 'function' ? this.endpoints.PRODUCTS() : this.endpoints.PRODUCTS

    if (!Array.isArray(urls)) {
      urls = [urls]
    }

    console.debug(`[${this.id}] fetching products...`, urls)

    let data = []

    for (let url of urls) {
      const action = url.split('|')

      let method = action.length > 1 ? action.shift() : 'GET'
      let target = action[0]

      data.push(
        await axios
          .get(target, {
            method: method,
          })
          .then((response) => response.data)
          .catch((err) => {
            console.error(`[${this.id}] failed to fetch ${target}\n\t->`, err.message)
            throw err
          })
      )
    }

    if (this.scheduledOperationsDelays.getProducts) {
      delete this.scheduledOperationsDelays.getProducts
    }

    if (data.length === 1) {
      data = data[0]
    }

    if (data) {
      const formatedProducts = this.formatProducts(data) || []

      await this.saveProducts(formatedProducts)

      return formatedProducts
    }

    return null
  }

  /**
   * Get exchange products
   * @returns {Promise<void>}
   */
  async getProducts() {
    let formatedProducts

    try {
      formatedProducts = await this.readProducts()
    } catch (error) {
      console.error(`[${this.id}/getProducts] failed to read products`, error)
    }

    if (!formatedProducts) {
      try {
        formatedProducts = await this.fetchProducts()
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

    this.indexSymbols()
  }

  indexSymbols() {
    this.indexedProducts = []

    if (!this.products) {
      return
    }

    if (Array.isArray(this.products)) {
      this.indexedProducts = this.products.slice(0, this.products.length)
    } else if (typeof this.products === 'object') {
      this.indexedProducts = Object.keys(this.products)
    }

    console.log(`[${this.id}.indexSymbols] ${this.indexedProducts.length} products indexed`)

    this.emit('index', this.indexedProducts)
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
  onClose(event, api) {
    if (api._closeWasHandled) {
      // prevent double handling
      return
    }

    if (this.connecting[api.id]) {
      this.connecting[api.id].resolver(false)
      delete this.connecting[api.id]
    }

    if (this.disconnecting[api.id]) {
      this.disconnecting[api.id].resolver(true)
      delete this.disconnecting[api.id]
    }

    const pairs = [...api._pending, ...api._connected]

    console.debug(`[${this.id}] ${pairs.join(',')}'s api closed`)

    this.emit('close', api.id, pairs, event)

    api._closeWasHandled = true
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
    if (!(trades = this.queueControl(trades))) {
      return
    }

    this.emit('trades', trades)

    return true
  }

  /**
   * Emit liquidations to server
   * @param {string} source api id
   * @param {Trade[]} trades
   */
  emitLiquidations(source, trades) {
    if (!(trades = this.queueControl(trades))) {
      return
    }

    this.emit('liquidations', trades)

    return true
  }

  queueControl(trades) {
    if (!trades || !trades.length) {
      return null
    }

    if (this.shouldQueueTrades || this.busyRecovering) {
      if (!this.queuedTrades.length) {
        console.log(`[${this.id}] starts queueing incoming trades`)
      }
      Array.prototype.push.apply(this.queuedTrades, trades)
      return null
    } else if (this.queuedTrades.length) {
      console.log(`[${this.id}] release trades queue (${this.queuedTrades.length} trades)`)
      trades = trades.concat(this.queuedTrades).sort((a, b) => a.timestamp - b.timestamp)
      this.queuedTrades = []
    }

    return trades
  }

  startKeepAlive(api, payload = { event: 'ping' }, every = 30000) {
    if (this.keepAliveIntervals[api.id]) {
      this.stopKeepAlive(api.id)
    }

    console.debug(`[${this.id}] setup keepalive for ws ${api.url} (${api.id})`)

    this.keepAliveIntervals[api.id] = setInterval(() => {
      if (api.readyState === WebSocket.OPEN) {
        api.send(JSON.stringify(payload))
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

    console.debug(`[${this.id}] schedule ${operationId} in ${getHms(currentDelay)}`)

    this.scheduledOperations[operationId] = setTimeout(() => {
      console.debug(`[${this.id}] schedule timer fired`)

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

  async saveProducts(data) {
    const path = 'products/' + this.id + '.json'
    const storage = {
      expiration: +new Date() + 1000 * 60 * 60 * 24 * 2, // 7 days
      data,
    }

    await ensureDirectoryExists(path)

    await new Promise((resolve) => {
      fs.writeFile(path, JSON.stringify(storage), (err) => {
        if (err) {
          console.error(`[${this.id}] failed to save products to ${path}`, err)
        }

        resolve()
      })
    })
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
}

module.exports = Exchange
