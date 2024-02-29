const config = require('../config')
const socketService = require('./socket')
const persistenceService = require('./persistence')
const EventEmitter = require('events')
const { indexes, getIndex } = require('./connections')
const { getHms, sleep, ago } = require('../helper')
const webPush = require('web-push')

class AlertService extends EventEmitter {
  constructor() {
    super()

    this.enabled = false
    this.alerts = {}
    this.alertEndpoints = {}
    this.alertsQueue = []

    if (config.influxCollectors && config.collect && !config.api) {
      // node is a collector: listen for toggleAlerts op
      socketService.on('toggleAlert', ({ data, questionId }) => {
        this.toggleAlert(data, questionId)
      })
    }

    if (config.collect) {
      this.getAlerts()
    }

    console.log(config.publicVapidKey)
    console.log(config.privateVapidKey)

    if (config.publicVapidKey && config.privateVapidKey) {
      this.enabled = true
      webPush.setVapidDetails(
        'mailto: contact@aggr.trade',
        config.publicVapidKey,
        config.privateVapidKey
      )
    }
    console.log(
      `[alert] service is ${
        this.enabled
          ? 'enabled.'
          : 'disabled. \nPlease run: npm run generateVapidKeys and add the generated keys to config.json.'
      }`
    )
  }

  getRangePrice(n) {
    let dec = n.toString().split('.')

    if (!+dec[0]) {
      dec = 1 / Math.pow(10, dec[1].replace(/(0*).*/, '$1').length + 1)
    } else {
      dec = Math.pow(10, dec[0].length - 2)
    }

    dec = dec || 1

    return Math.floor(n / dec) * dec
  }

  toggleAlert(alert, messageId) {
    if (alert.market.indexOf(':') !== -1) {
      throw new Error('you are using an outdated client, please refresh')
    }

    if (
      !messageId &&
      config.influxCollectors &&
      socketService.clusteredCollectors.length
    ) {
      const collector = socketService.getNodeByMarket(alert.market)

      if (!collector) {
        throw new Error(`unsupported market ${alert.market}`)
      }

      return socketService.ask(collector, 'toggleAlert', alert)
    } else {
      const index = getIndex(alert.market)

      if (!index) {
        if (!messageId) {
          throw new Error(`unsupported market ${alert.market}`)
        }

        return
      }

      if (typeof alert.message === 'string' && alert.message.trim().length) {
        alert.message = alert.message.slice(0, 250)
      } else {
        alert.message = null
      }

      const activeAlert = this.getActiveAlert(alert, index.id)
      const priceOffset =
        index.price && alert.currentPrice ? alert.currentPrice - index.price : 0

      if (!alert.status) {
        if (activeAlert) {
          activeAlert.user = alert.user

          if (typeof alert.newPrice === 'number') {
            this.moveAlert(
              activeAlert,
              {
                price: alert.newPrice,
                message: alert.message
              },
              priceOffset
            )
          } else if (alert.unsubscribe) {
            this.unregisterAlert(activeAlert, index.id)
          }
        } else if (!alert.unsubscribe) {
          this.registerAlert(alert, index.id, priceOffset)
        }
      }

      const status = {
        markets: index.markets,
        alert: activeAlert,
        priceOffset
      }

      if (messageId) {
        socketService.answer(socketService.clusterSocket, messageId, status)
      }

      return status
    }
  }

  getActiveAlert(alert, market) {
    if (!this.alerts[market]) {
      return null
    }

    const rangePrice = this.getRangePrice(alert.price)

    if (!this.alerts[market][rangePrice]) {
      return null
    }

    return this.alerts[market][rangePrice].find(
      activeAlert =>
        activeAlert.endpoint === alert.endpoint &&
        activeAlert.price === alert.price
    )
  }

  registerAlert(alert, market, priceOffset) {
    if (!this.alerts[market]) {
      this.alerts[market] = {}
    }

    if (!this.alertEndpoints[alert.endpoint]) {
      this.alertEndpoints[alert.endpoint] = {
        user: alert.user,
        endpoint: alert.endpoint,
        keys: alert.keys
      }
    }

    if (typeof alert.newPrice === 'number') {
      alert.price = alert.newPrice
    }

    const rangePrice = this.getRangePrice(alert.price)

    if (!this.alerts[market][rangePrice]) {
      this.alerts[market][rangePrice] = []
    }

    const now = Date.now()

    this.alertEndpoints[alert.endpoint].timestamp = now

    console.log(
      `[alert/${alert.user}] create alert ${market} @${
        alert.price
      } (ajusted to ${alert.price - (priceOffset || 0)})`
    )

    if (alert.message) {
      console.log(`\t 💬 ${alert.message}`)
    }

    this.emit('change', {
      market: market,
      price: alert.price,
      user: alert.user,
      type: 'add'
    })

    this.alerts[market][rangePrice].push({
      endpoint: alert.endpoint,
      market: market,
      price: alert.price,
      priceCompare: alert.price - (priceOffset || 0),
      message: alert.message,
      origin: alert.origin,
      timestamp: now
    })

    return true
  }

  unregisterAlert(alert, market, wasAutomatic) {
    const rangePrice = this.getRangePrice(alert.price)

    if (!this.alerts[market][rangePrice]) {
      return
    }

    const index = this.alerts[market][rangePrice].indexOf(alert)

    this.scheduleAlertsCleanup(alert, market)

    if (index !== -1) {
      let user
      if (this.alertEndpoints[alert.endpoint]) {
        user = this.alertEndpoints[alert.endpoint].user
      }

      if (wasAutomatic) {
        console.log(
          `[alert/${user || 'unknown'}] server removed ${market} @${
            alert.price
          }`
        )
      } else {
        console.log(
          `[alert/${user || 'unknown'}] user removed alert ${market} @${
            alert.price
          }`
        )
      }

      if (!alert.triggered && user) {
        this.emit('change', {
          market: market,
          price: alert.price,
          user: user,
          type: 'remove'
        })
      }

      this.alerts[market][rangePrice].splice(index, 1)

      return true
    }

    return false
  }

  moveAlert(activeAlert, newAlert, priceOffset) {
    const rangePrice = this.getRangePrice(activeAlert.price)

    if (!this.alerts[activeAlert.market][rangePrice]) {
      return
    }

    const index =
      this.alerts[activeAlert.market][rangePrice].indexOf(activeAlert)

    if (index !== -1) {
      this.alerts[activeAlert.market][rangePrice].splice(index, 1)

      console.log(
        `[alert/${activeAlert.user}] move alert on ${activeAlert.market} @${
          activeAlert.price
        } -> ${newAlert.price} (ajusted to ${
          activeAlert.price - (priceOffset || 0)
        })`
      )

      const now = Date.now()

      this.emit('change', {
        market: activeAlert.market,
        previousPrice: activeAlert.price,
        price: newAlert.price,
        user: activeAlert.user,
        type: 'move'
      })

      activeAlert.price = newAlert.price
      if (newAlert.message && newAlert.message !== activeAlert.message) {
        console.log(`\t 💬 ${activeAlert.message}`)
        activeAlert.message = newAlert.message
      }
      activeAlert.priceCompare = activeAlert.price - (priceOffset || 0)
      activeAlert.timestamp = now

      const newRangePrice = this.getRangePrice(newAlert.price)

      if (!this.alerts[activeAlert.market][newRangePrice]) {
        this.alerts[activeAlert.market][newRangePrice] = []
      }

      this.alerts[activeAlert.market][newRangePrice].push(activeAlert)
    }
  }

  async scheduleAlertsCleanup(alert, market) {
    setTimeout(() => {
      const rangePrice = this.getRangePrice(alert.price)

      if (this.alerts[market]) {
        if (
          this.alerts[market][rangePrice] &&
          !this.alerts[market][rangePrice].length
        ) {
          console.log(
            `[alert/cleanup] no more alerts for ${market} in the ${rangePrice} region`
          )
          delete this.alerts[market][rangePrice]
        }

        if (!Object.keys(this.alerts[market])) {
          console.log(`[alert/cleanup] no more alerts for ${market}`)
          delete this.alerts[market]
        }
      }
    }, 100)
  }

  async getAlerts() {
    this.alertEndpoints =
      (await persistenceService.get('alerts-endpoints')) || {}

    const now = Date.now()

    for (const endpoint in this.alertEndpoints) {
      if (
        this.alertEndpoints[endpoint].timestamp &&
        now - this.alertEndpoints[endpoint].timestamp >
          config.alertEndpointExpiresAfter
      ) {
        console.warn(
          `[alert/get] removed expired endpoint (last updated ${ago(
            this.alertEndpoints[endpoint].timestamp
          )} ago)`
        )
        delete this.alertEndpoints[endpoint]
      }
    }

    console.log(
      `[alert/get] retrieved ${
        Object.keys(this.alertEndpoints).length
      } alert user(s)`
    )

    let totalCount = 0
    let pairsCount = 0
    let isolatedCount = 0

    for (const index of indexes) {
      const marketAlerts =
        (await persistenceService.get('alerts-' + index.id)) || {}

      let hasValidAlerts = false

      for (const rangePrice in marketAlerts) {
        for (let i = 0; i < marketAlerts[rangePrice].length; i++) {
          const alert = marketAlerts[rangePrice][i]
          if (!this.alertEndpoints[alert.endpoint]) {
            console.warn(
              `[alert/get] removed unattached alert on ${index.id} @${
                alert.price
              } (last updated ${ago(alert.timestamp)} ago)`
            )
            marketAlerts[rangePrice].splice(i--, 1)
            continue
          }
        }

        const count = marketAlerts[rangePrice].length

        if (count) {
          if (count > 1) {
            console.log(
              `[alert/get] ${index.id} has ${count} alerts in the ${rangePrice} region`
            )
          } else {
            isolatedCount++
          }

          hasValidAlerts = true
          totalCount += count
        } else {
          delete marketAlerts[rangePrice]
        }
      }

      if (hasValidAlerts) {
        this.alerts[index.id] = marketAlerts
      }

      pairsCount++

      if (isolatedCount) {
        console.log(
          `[alert/get] ${index.id} has ${isolatedCount} isolated price ranges (ranges with only 1 alert each)`
        )
      }
    }

    console.log(`[alert] total ${totalCount} alerts across ${pairsCount} pairs`)

    this._persistAlertsTimeout = setTimeout(
      this.persistAlerts.bind(this),
      1000 * 60 * 30 + Math.random() * 1000 * 60 * 30
    )
  }

  async persistAlerts(isExiting = false) {
    if (!config.collect) {
      return
    }

    clearTimeout(this._persistAlertsTimeout)

    const now = Date.now()

    const otherAlertEndpoints =
      (await persistenceService.get('alerts-endpoints')) || {}

    for (const endpoint in otherAlertEndpoints) {
      if (this.alertEndpoints[endpoint]) {
        otherAlertEndpoints[endpoint].timestamp = Math.max(
          this.alertEndpoints[endpoint].timestamp || 0,
          otherAlertEndpoints[endpoint].timestamp || 0
        )
      }

      if (
        !otherAlertEndpoints[endpoint].timestamp ||
        now - otherAlertEndpoints[endpoint].timestamp <
          config.alertEndpointExpiresAfter
      ) {
        this.alertEndpoints[endpoint] = otherAlertEndpoints[endpoint]
      }
    }

    console.log(
      `[alert/persist] save ${
        Object.keys(this.alertEndpoints).length
      } alert user(s)`
    )

    try {
      await persistenceService.set('alerts-endpoints', this.alertEndpoints)
    } catch (error) {
      console.error(
        '[alert/persist] persistence error (saving endpoints)',
        error.message
      )
    }

    let totalCount = 0
    let pairsCount = 0

    for (const market in this.alerts) {
      let count = 0

      for (const rangePrice in this.alerts[market]) {
        count += this.alerts[market][rangePrice].length
      }

      if (count) {
        totalCount += count
        pairsCount++

        console.log(
          `[alert/persist] save ${count} alert(s) on ${market} (across ${
            Object.keys(this.alerts[market]).length
          } ranges)`
        )

        try {
          await persistenceService.set('alerts-' + market, this.alerts[market])
        } catch (error) {
          console.error(
            '[alert/persist] persistence error (saving alerts)',
            error.message
          )
        }
      } else {
        try {
          await persistenceService.delete('alerts-' + market)
        } catch (error) {
          console.error(
            '[alert/persist] persistence error (removing alerts pair)',
            error.message
          )
        }
      }
    }

    console.log(
      `[alert/persist] ${totalCount} alerts across ${pairsCount} pairs`
    )

    if (!isExiting) {
      this._persistAlertsTimeout = setTimeout(
        this.persistAlerts.bind(this),
        1000 * 60 * 30 + Math.random() * 1000 * 60 * 30
      )
    }
  }

  queueAlert(alert, market, timestamp, direction) {
    if (!this.alertEndpoints[alert.endpoint]) {
      console.error(
        `[alert/send] attempted to send alert without matching endpoint`,
        alert
      )
      return
    }

    const elapsedTime = timestamp - alert.timestamp

    alert.triggered = true

    let message
    let title

    if (alert.message) {
      if (direction > 0) {
        title = `${market} 📈`
      } else if (direction < 0) {
        title = `${market} 📉`
      } else {
        title = market
      }
      message = alert.message
    } else {
      title = market
      if (direction > 0) {
        message = `📈 ${alert.price}`
      } else if (direction < 0) {
        message = `📉 ${alert.price}`
      } else {
        message = `Price crossed ${alert.price}`
      }
    }

    const payload = JSON.stringify({
      title,
      body: message,
      origin: alert.origin,
      price: alert.price,
      message: alert.message,
      market: market,
      direction
    })

    this.emit('change', {
      market: market,
      price: alert.price,
      user: this.alertEndpoints[alert.endpoint].user,
      type: 'triggered'
    })

    this.alertsQueue.push({
      attempts: 0,
      market,
      price: alert.price,
      elapsedTime,
      subscription: this.alertEndpoints[alert.endpoint],
      payload
    })

    if (!this.isProcessingQueue) {
      this.processQueue()
    }
  }

  async processQueue() {
    if (!this.alertsQueue.length) {
      this.isProcessingQueue = false
      console.log(`[alert.queue] no more alerts in queue`)
      return
    }

    this.isProcessingQueue = true

    const queuedAlert = this.alertsQueue.shift()
    queuedAlert.attempts++

    try {
      console.log(
        `[alert/send/${queuedAlert.subscription.user}.${
          queuedAlert.attempts
        }] send alert ${queuedAlert.market} @${
          queuedAlert.price
        } (after ${getHms(queuedAlert.elapsedTime)})`
      )
      await this.sendAlert(queuedAlert.subscription, queuedAlert.payload)
      queuedAlert.succeeded = true
    } catch (error) {
      if (error instanceof webPush.WebPushError) {
        console.error(
          `[alert/send/${queuedAlert.subscription.user}.${queuedAlert.attempts}] ${error.message}\n\t-> ${error.statusCode}: ${error.body}`
        )

        if (error.statusCode > 400 && error.statusCode < 499) {
          queuedAlert.attempts = 5
          // delete user
        }
      } else {
        console.error(
          `[alert/send/${queuedAlert.subscription.user}.${queuedAlert.attempts}] failed to send push notification`,
          error.message
        )
      }

      /**
       * @todo: only retry on network-related error
       */
      if (queuedAlert.attempts > 4) {
        console.error(
          `[alert/send/${queuedAlert.subscription.user}.${queuedAlert.attempts}] won't be sent (too many attempts)`
        )
      } else {
        this.alertsQueue.push(queuedAlert)
      }
    }

    await sleep(1000)
    this.processQueue()
  }

  sendAlert(subscription, payload) {
    return webPush.sendNotification(subscription, payload, {
      vapidDetails: {
        subject: 'mailto: contact@aggr.trade',
        publicKey: config.publicVapidKey,
        privateKey: config.privateVapidKey
      },
      contentEncoding: 'aes128gcm'
    })
  }

  /**
   * Send notification to every price alerts crossing given high/low range on a given index
   * @param {string} indexName local symbol index
   * @param {number} high index high range
   * @param {number} low index low range
   */
  async checkPriceCrossover(market, high, low, direction) {
    const rangePriceHigh = this.getRangePrice(high)
    const rangePriceLow = this.getRangePrice(low)

    await this.checkInRangePriceCrossover(
      market,
      high,
      low,
      direction,
      rangePriceLow
    )

    if (rangePriceLow !== rangePriceHigh) {
      await this.checkInRangePriceCrossover(
        market,
        high,
        low,
        direction,
        rangePriceHigh
      )
    }
  }

  async checkInRangePriceCrossover(market, high, low, direction, rangePrice) {
    if (!this.alerts[market] || !this.alerts[market][rangePrice]) {
      return
    }

    const now = Date.now()
    for (let i = 0; i < this.alerts[market][rangePrice].length; i++) {
      const alert = this.alerts[market][rangePrice][i]

      if (now - alert.timestamp < config.backupInterval) {
        continue
      }

      const isTriggered =
        alert.priceCompare >= low && alert.priceCompare <= high

      if (isTriggered) {
        console.log(
          `[alert/checkPriceCrossover] ${alert.price} (ajusted to ${alert.priceCompare}) ${market} ${low} <-> ${high}`
        )
        this.queueAlert(alert, market, now, direction)

        if (this.unregisterAlert(alert, market, true)) {
          i--
        }
      }
    }
  }
}

module.exports = new AlertService()
