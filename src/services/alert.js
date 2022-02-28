const config = require('../config')
const socketService = require('./socket')
const persistenceService = require('./persistence')
const EventEmitter = require('events')
const { indexes, getIndex } = require('./connections')
const { getHms, sleep } = require('../helper')
const webPush = require('web-push')

class AlertService extends EventEmitter {
  constructor() {
    super()

    this.alerts = {}
    this.alertEndpoints = {}

    if (config.influxCollectors && config.collect && !config.api) {
      if (config.privateVapidKey && (typeof config.id === 'undefined' || config.id === null)) {
        console.error(`[alerts] push subscriptions won't be persisted (NEED ID COLLECTORS)`)
      }

      // node is a collector: listen for toggleAlerts op
      socketService.on('toggleAlert', this.toggleAlert.bind(this))
    }

    if (config.collect) {
      this.getAlerts()
    }
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

  toggleAlert(alert, fromCluster = false) {
    if (alert.market.indexOf(':') !== -1) {
      throw new Error('you are using an outdated client, please refresh')
    }

    if (!fromCluster && config.influxCollectors && socketService.clusteredCollectors.length) {
      const collector = socketService.getNodeByMarket(alert.market)

      if (!collector) {
        throw new Error('unsupported market')
      }

      collector.write(
        JSON.stringify({
          op: 'toggleAlert',
          data: alert,
        }) + '#'
      )
    } else {
      const index = getIndex(alert.market)

      if (!index) {
        if (!fromCluster) {
          throw new Error('unsupported market')
        }

        return
      }

      const activeAlert = this.getActiveAlert(alert, index.id)

      if (activeAlert) {
        activeAlert.user = alert.user

        if (typeof alert.newPrice === 'number') {
          this.moveAlert(activeAlert, index.id, alert.newPrice)
        } else if (alert.unsubscribe) {
          this.unregisterAlert(activeAlert, index.id)
        }

        return
      } else if (!alert.unsubscribe) {
        this.registerAlert(alert, index.id)
      }
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
      (activeAlert) => activeAlert.endpoint === alert.endpoint && activeAlert.price === alert.price
    )
  }

  registerAlert(alert, market) {
    if (!this.alerts[market]) {
      this.alerts[market] = {}
    }

    if (!this.alertEndpoints[alert.endpoint]) {
      this.alertEndpoints[alert.endpoint] = {
        user: alert.user,
        endpoint: alert.endpoint,
        keys: alert.keys,
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
      `[alert/${alert.user}] create alert ${market} @ ${alert.price}`
    )

    this.emit('change', {
      market: market,
      price: alert.price,
      user: alert.user,
      type: 'add',
    })

    this.alerts[market][rangePrice].push({
      endpoint: alert.endpoint,
      market: market,
      price: alert.price,
      origin: alert.origin,
      timestamp: now,
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
        console.log(`[alert/${user || 'unknown'}] server removed ${market} @ ${alert.price}`)
      } else {
        console.log(`[alert/${user || 'unknown'}] user removed alert ${market} @ ${alert.price}`)
      }

      if (!alert.triggered && user) {
        this.emit('change', {
          market: market,
          price: alert.price,
          user: user,
          type: 'remove',
        })
      }

      this.alerts[market][rangePrice].splice(index, 1)

      return true
    }

    return false
  }

  moveAlert(alert, market, newPrice) {
    const rangePrice = this.getRangePrice(alert.price)

    if (!this.alerts[market][rangePrice]) {
      return
    }

    const index = this.alerts[market][rangePrice].indexOf(alert)

    if (index !== -1) {
      this.alerts[market][rangePrice].splice(index, 1)

      console.log(`[alert/${alert.user}] move alert on ${market} @ ${alert.price} -> ${newPrice}`)

      const now = Date.now()

      alert.price = newPrice
      alert.timestamp = now

      const newRangePrice = this.getRangePrice(newPrice)

      if (!this.alerts[market][newRangePrice]) {
        this.alerts[market][newRangePrice] = []
      }

      this.alerts[market][newRangePrice].push(alert)
    }
  }

  async scheduleAlertsCleanup(alert, market) {
    setTimeout(() => {
      const rangePrice = this.getRangePrice(alert.price)

      if (this.alerts[market]) {
        if (this.alerts[market][rangePrice] && !this.alerts[market][rangePrice].length) {
          console.log(
            `[alert/cleanup] no more alerts for ${market} in the ${rangePrice} region`
          )
          delete this.alerts[market][rangePrice]
        }

        if (!Object.keys(this.alerts[market])) {
          console.log(
            `[alert/cleanup] no more alerts for ${market}`
          )
          delete this.alerts[market]
        }
      }
    }, 100)
  }

  async getAlerts() {
    this.alertEndpoints = (await persistenceService.get(config.id + '-alerts-endpoints')) || {}

    const now = Date.now()
    let expired = 0

    for (const endpoint in this.alertEndpoints) {
      if (this.alertEndpoints[endpoint].timestamp && now - this.alertEndpoints[endpoint].timestamp > config.alertEndpointExpiresAfter) {
        
        delete this.alertEndpoints[endpoint]
        expired++
      }
    }

    if (expired) {
      console.log(`[alert/get] removed ${expired} expired endpoint${expired > 1 ? 's' : ''}`)
    }

    console.log(`[alert/get] this node handle ${Object.keys(this.alertEndpoints).length} alert user(s)`)

    let totalCount = 0
    let pairsCount = 0

    for (const index of indexes) {
      let marketAlerts = (await persistenceService.get(config.id + '-alerts-' + index.id)) || {}

      if (!Object.keys(marketAlerts).length) {
        console.log(`[alert] migrates ${index.id} market alerts to index based alerts`)

        for (const market of index.markets) {
          const oldMarketsAlerts = (await persistenceService.get(config.id + '-alerts-' + market)) || []
  
          if (oldMarketsAlerts.length) {
            console.log(`[alert] converted ${oldMarketsAlerts.length} ${market}'s alerts (into ${index.id})`)
            await persistenceService.delete(config.id + '-alerts-' + market)

            marketAlerts = oldMarketsAlerts.reduce((acc, alert) => {
              const rangePrice = this.getRangePrice(alert.price)
    
              if (!acc[rangePrice]) {
                acc[rangePrice] = []
              }
    
              acc[rangePrice].push(alert)
    
              return acc
            }, marketAlerts)
          }
        }
      }

      let hasValidAlerts = false
      let unlinkedAlerts = 0

      for (const rangePrice in marketAlerts) {
        for (let i = 0; i < marketAlerts[rangePrice].length; i++) {
          const alert = marketAlerts[rangePrice][i]
          if (!this.alertEndpoints[alert.endpoint]) {
            unlinkedAlerts++
            marketAlerts[rangePrice].splice(i--, 1)
            continue
          }
        }

        if (unlinkedAlerts) {
          console.warn(`[alert/get] ${index.id} had ${unlinkedAlerts} unattached alert (no matching endpoint)`)
        }
        
        if (marketAlerts[rangePrice].length) {
          console.log(`[alert/get] ${index.id} has ${marketAlerts[rangePrice].length} alerts in the ${rangePrice} region`)
          hasValidAlerts = true
          totalCount += marketAlerts[rangePrice].length
        } else {
          delete marketAlerts[rangePrice]
        }
      }

      if (hasValidAlerts) {
        this.alerts[index.id] = marketAlerts
      }

      pairsCount++
    }

    console.log(`[alert] total ${totalCount} alerts across ${pairsCount} pairs`)

    this._persistAlertsTimeout = setTimeout(this.peristAlerts.bind(this), 1000 * 60 * 30 + Math.random() * 1000 * 60 * 30)
  }

  async peristAlerts(isExiting = false) {
    if (!config.collect) {
      return
    }

    clearTimeout(this._persistAlertsTimeout)

    const now = Date.now()

    const otherAlertEndpoints = (await persistenceService.get(config.id + '-alerts-endpoints') || {})

    for (const endpoint in otherAlertEndpoints) {
      if (this.alertEndpoints[endpoint]) {
        otherAlertEndpoints[endpoint].timestamp = Math.max(this.alertEndpoints[endpoint].timestamp || 0, otherAlertEndpoints[endpoint].timestamp || 0)
      }

      if (!otherAlertEndpoints[endpoint].timestamp || now - otherAlertEndpoints[endpoint].timestamp < config.alertEndpointExpiresAfter) {
        this.alertEndpoints[endpoint] = otherAlertEndpoints[endpoint]
      }
    }

    try {
      await persistenceService.set(config.id + '-alerts-endpoints', this.alertEndpoints)
    } catch (error) {
      console.error('[alert/persist] persistence error (saving endpoints)', error.message)
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

        try {
          await persistenceService.set(config.id + '-alerts-' + market, this.alerts[market])
        } catch (error) {
          console.error('[alert/persist] persistence error (saving alerts)', error.message)
        }
      } else {
        try {
          await persistenceService.delete(config.id + '-alerts-' + market)
        } catch (error) {
          console.error('[alert/persist] persistence error (removing alerts pair)', error.message)
        }
      }
    }

    console.log(`[alert/persist] save alerts in persistence (${totalCount} alerts across ${pairsCount} pairs)`)

    if (!isExiting) {
      this._persistAlertsTimeout = setTimeout(this.peristAlerts.bind(this), 1000 * 60 * 30 + Math.random() * 1000 * 60 * 30)
    }
  }

  sendAlert(alert, market, elapsedTime) {
    if (!this.alertEndpoints[alert.endpoint]) {
      console.error(`[alert/send] attempted to send alert without matching endpoint`, alert)
      return
    }

    console.log(`[alert/send/${this.alertEndpoints[alert.endpoint].user}] send alert ${market} @ ${alert.price} (${getHms(elapsedTime)} after)`)

    alert.triggered = true

    const payload = JSON.stringify({
      title: `${market}`,
      body: `Price crossed ${alert.price}`,
      origin: alert.origin,
      price: alert.price,
      market: market,
    })

    this.emit('change', {
      market: market,
      price: alert.price,
      user: this.alertEndpoints[alert.endpoint].user,
      type: 'triggered',
    })

    return webPush
      .sendNotification(this.alertEndpoints[alert.endpoint], payload, {
        vapidDetails: {
          subject: 'mailto: contact@aggr.trade',
          publicKey: config.publicVapidKey,
          privateKey: config.privateVapidKey,
        },
        contentEncoding: 'aes128gcm',
      })
      .then(() => {
        return sleep(100)
      })
      .catch((err) => {
        console.error(
          `[alert/send] push notification failure\n\t`,
          err.message,
          '| payload :',
          JSON.parse(payload),
          this.alertEndpoints[alert.endpoint]
        )
        console.error(JSON.parse(payload))
      })
  }

  /**
   * Send notification to every price alerts crossing given high/low range on a given index
   * @param {string} indexName local symbol index
   * @param {number} high index high range
   * @param {number} low index low range
   */
   checkPriceCrossover(market, high, low) {
    const rangePriceHigh = this.getRangePrice(high)
    const rangePriceLow = this.getRangePrice(low)

    this.checkInRangePriceCrossover(market, high, low, rangePriceLow)

    if (rangePriceLow !== rangePriceHigh) {
      this.checkInRangePriceCrossover(market, high, low, rangePriceHigh)
    }
  }

  checkInRangePriceCrossover(market, high, low, rangePrice) {
    if (!this.alerts[market] || !this.alerts[market][rangePrice]) {
      return
    }

    const inRangeLength = this.alerts[market][rangePrice].length

    console.log(
      `[alert] check ${market}'s crossovers in the ${rangePrice} region (${inRangeLength} alert${inRangeLength > 1 ? 's' : ''})`
    )

    const now = Date.now()

    for (let i = 0; i < this.alerts[market][rangePrice].length; i++) {
      const alert = this.alerts[market][rangePrice][i]

      if (now - alert.timestamp < config.influxTimeframe) {
        continue
      }

      const isTriggered = alert.price <= high && alert.price >= low
      const isExpired = !isTriggered && alert.timestamp + config.alertExpiresAfter < now

      if (isTriggered || isExpired) {
        if (isTriggered) {
          this.sendAlert(alert, market, now - alert.timestamp)
        }

        if (this.unregisterAlert(alert, market, true)) {
          i--
        }
      }
    }
  }
}

module.exports = new AlertService()
