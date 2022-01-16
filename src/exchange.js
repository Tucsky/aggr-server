const EventEmitter = require('events')
const axios = require('axios')
const WebSocket = require('ws')
const pako = require('pako')
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
     * Clear reconnection delay timeout by apiUrl
     * @type {{[apiUrl: string]: number]}}
     */
    this.clearReconnectionDelayTimeout = {}

    /**
     * Max connections per apis
     * @type {number}
     */
    this.maxConnectionsPerApi = null

    /**
     * Define if the incoming trades should be queued
     * @type {boolean}
     */
    this.queueTrade = false

    /**
     * Counter of ongoing recovery operations
     */
    this.ongoingRecoveryOperations = 0

    /**
     * Pairs currently
     * @type {{[pair: string]: boolean]}}
     */
    this.recoveringTrades = {}

    /**
     * Trades goes theres while we wait for historical response
     * @type {Trade[]}
     */
    this.queuedTrades = []
    this.lastMessages = []

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

      if (wasBadData) {
        let json = event.data

        try {
          json = JSON.parse(event.data)
        } catch (error) {
          try {
            json = JSON.parse(pako.inflate(event.data, { to: 'string' }))
          } catch (error) {
            try {
              json = JSON.parse(pako.inflateRaw(event.data, { to: 'string' }))
            } catch (error) {
              //
            }
          }
        }

        if (!json) {
          return
        }

        const jsonString = JSON.stringify(json)

        this.lastMessages.push(jsonString)

        if (/(unrecognized|failure|invalid|error|expired|cannot|exceeded|error)/.test(jsonString)) {
          console.error(`[${this.id}] error message intercepted\n`, json)
        }

        if (this.lastMessages.length > 10) {
          this.lastMessages.splice(0, this.lastMessages.length - 10)
        }
      }
    }

    api.onopen = async (event) => {
      if (typeof this.scheduledOperationsDelays[url] !== 'undefined') {
        this.clearReconnectionDelayTimeout[url] = setTimeout(() => {
          delete this.clearReconnectionDelayTimeout[url]
          console.debug(`[${this.id}.createWs] 10s since api opened: clear reconnection delay (${url})`)
          delete this.scheduledOperationsDelays[url]
        }, 10000)
      }

      console.debug(`[${this.id}.onOpen] opened api ${api.id} (pending connections ${api._pending.join(', ')})`)

      if (this.connecting[api.id]) {
        this.connecting[api.id].resolver(true)
        delete this.connecting[api.id]

        await sleep(100)
      }

      this.subscribePendingPairs(api)

      this.onOpen(event, api._connected)
    }

    api.onclose = async (event) => {
      if (this.clearReconnectionDelayTimeout[url]) {
        clearTimeout(this.clearReconnectionDelayTimeout[url])
        delete this.clearReconnectionDelayTimeout[url]
      }

      if (this.connecting[api.id]) {
        this.connecting[api.id].resolver(false)
        delete this.connecting[api.id]
      }

      this.onClose(event, api._connected)

      if (this.disconnecting[api.id]) {
        this.disconnecting[api.id].resolver(true)
        delete this.disconnecting[api.id]
      }

      const pairsToReconnect = [...api._pending, ...api._connected]

      if (pairsToReconnect.length) {
        const pairsToDisconnect = api._connected.slice()

        console.log(`[${this.id}] connection closed unexpectedly`)
        console.log(this.lastMessages)

        if (pairsToDisconnect.length) {
          for (const pair of pairsToDisconnect) {
            await this.unlink(this.id + ':' + pair)
          }
        }

        this.scheduledOperationsDelays[api.url] = this.schedule(
          () => {
            this.reconnectPairs(pairsToReconnect)
          },
          api.url,
          500,
          1.5,
          1000 * 30
        )
      }

      this.lastMessages.splice(0, this.lastMessages.length)
    }

    api.onerror = (event) => {
      this.onError(event, api._connected)
    }

    this.connecting[api.id] = {}

    this.connecting[api.id].promise = new Promise((resolve, reject) => {
      this.connecting[api.id].resolver = (success) => {
        if (success) {
          this.onApiCreated(api)
          resolve(api)
        } else {
          reject()
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
   * @param {{[pair: string]: number}} timestamps last timestamp by pair
   */
  async reconnectApi(api, timestamps) {
    console.debug(
      `[${this.id}.reconnectApi] reconnect api (url: ${api.url}, _connected: ${api._connected.join(', ')}, _pending: ${api._connected.join(
        ', '
      )})`
    )

    const pairsToReconnect = [...api._pending, ...api._connected]

    // first we reconnect everything
    this.reconnectPairs(pairsToReconnect, timestamps)

    // immediately start recovering missing trades (if it can)
    this.recoverTrades(timestamps)
  }

  async reconnectAllApis() {
    for (const api of this.apis) {
      await this.reconnectApi(api)
    }
  }

  async recoverTrades(timestamps, pairs) {
    // check if exchange is compatible
    if (typeof this.getMissingTrades === 'function') {
      console.log(`[${this.id}.reconnectApi] recover missing trades ${pairs ? '(POST open)' : '(PRE open)'}`)

      const to = Date.now()

      let totalRecovered = 0
      let totalPairs = 0

      for (const pair in timestamps) {
        if (this.recoveringTrades[pair] || (pairs && pairs.indexOf(pair) === -1)) {
          continue
        }

        totalPairs++

        this.recoveringTrades[pair] = true

        // immediately stop emiting trades to preserve natural time continuity
        this.ongoingRecoveryOperations++

        const totalMissingTime = to - timestamps[pair]

        console.log(
          `[${this.id}.reconnectApi] get missing trades for ${pair} (${getHms(totalMissingTime)} of data | starting at ${new Date(
            timestamps[pair]
          ).toISOString().split('T').pop()})`
        )

        try {
          const totalRecoveredPair = await this.getMissingTrades(pair, timestamps, to)
          console.info(
            `[${this.id}.getMissingTrades] recovered ${totalRecoveredPair} trades on ${this.id}:${pair} (${getHms(
              totalMissingTime - (to - timestamps[pair])
            )} recovered out of ${getHms(totalMissingTime)} | ${getHms(to - timestamps[pair])} remaining)`
          )
          totalRecovered += totalRecoveredPair
        } catch (error) {
          console.error(`[${this.id}.reconnectApi] something went wrong while recovering ${pair}'s missing trades`, error.message)
        }

        this.recoveringTrades[pair] = false

        // release
        this.ongoingRecoveryOperations--
      }

      console.log(`[${this.id}.reconnectApi] trade recovery ended (recovered ${totalRecovered} trades across ${totalPairs} pairs)`)
    }
  }

  /**
   * Reconnect pairs
   * @param {string[]} pairs (local)
   * @returns {Promise<any>}
   */
  async reconnectPairs(pairs, timestamps) {
    const pairsToReconnect = pairs.slice(0, pairs.length)

    console.info(`[${this.id}.reconnectPairs] reconnect pairs ${pairsToReconnect.join(',')}`)

    for (let pair of pairsToReconnect) {
      await this.unlink(this.id + ':' + pair, true)
    }

    const promisesOfSubscriptions = []
    const promisesOfApiOpens = []

    for (let pair of pairsToReconnect) {
      promisesOfSubscriptions.push(this.link(this.id + ':' + pair))

      if (timestamps) {
        const api = this.getActiveApiByPair(pair, true)

        if (api && this.connecting[api.id] && promisesOfApiOpens.indexOf(this.connecting[api.id].promise) === -1) {
          // api is new and connecting
          promisesOfApiOpens.push(this.connecting[api.id].promise)
        }
      }
    }

    for (let promise of promisesOfApiOpens) {
      // sub to api open for potential recovery
      promise.then((api) => {
        this.recoverTrades(timestamps, api._connected.concat(api._pending))
      })
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
            console.debug(`stored products expired (${now.toISOString()} > ${new Date(expiration).toISOString()})`)
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
   * Fire when a new websocket connection opened
   * @param {Event} event
   * @param {string[]} pairs pairs attached to ws at opening
   */
  onOpen(event, pairs) {
    this.emit('open', event)
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
   * Fire when a new websocket connection reported an error
   * @param {Event} event
   * @param {string[]} pairs
   */
  onError(event, pairs) {
    console.debug(`[${this.id}.onError] ${pairs.join(',')}'s api errored`, event)
    this.emit('err', event)
  }

  /**
   * Fire when a new websocket connection closed
   * @param {Event} event
   * @param {string[]} pairs
   */
  onClose(event, pairs) {
    console.debug(`[${this.id}] ${pairs.join(',')}'s api closed`)
    this.emit('close', event)
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
    if (!trades || !trades.length) {
      return
    }

    if (this.ongoingRecoveryOperations > 0) {
      Array.prototype.push.apply(this.queuedTrades, trades)
      return true
    } else if (this.queuedTrades.length) {
      console.debug(`[${this.id}] release trades queue (${this.queuedTrades.length})`)
      trades = trades.concat(this.queuedTrades).sort((a, b) => a.timestamp - b.timestamp)
      this.queuedTrades = []
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
    if (!trades || !trades.length) {
      return
    }

    this.emit('liquidations', trades)

    return true
  }

  startKeepAlive(api, payload = { event: 'ping' }, every = 30000) {
    if (this.keepAliveIntervals[api.url]) {
      this.stopKeepAlive(api)
    }

    console.debug(`[${this.id}] setup keepalive for ws ${api.url}`)

    this.keepAliveIntervals[api.url] = setInterval(() => {
      if (api.readyState === WebSocket.OPEN) {
        api.send(JSON.stringify(payload))
      }
    }, every)
  }

  stopKeepAlive(api) {
    if (!this.keepAliveIntervals[api.url]) {
      return
    }

    console.debug(`[${this.id}] stop keepalive for ws ${api.url}`)

    clearInterval(this.keepAliveIntervals[api.url])
    delete this.keepAliveIntervals[api.url]
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
}

module.exports = Exchange
