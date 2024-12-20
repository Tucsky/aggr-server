const fs = require('fs')
const tx2 = require('tx2')

console.log('PID: ', process.pid)

const config = require('./src/config')
const Server = require('./src/server')
const alertService = require('./src/services/alert')
const { saveConnections } = require('./src/services/connections')
const socketService = require('./src/services/socket')
const FilesStorage = require('./src/storage/files')

/* Load available exchanges
 */

if (!config.exchanges || !config.exchanges.length) {
  config.exchanges = []
  fs.readdirSync('./src/exchanges/').forEach(file => {
    ;/\.js$/.test(file) && config.exchanges.push(file.replace(/\.js$/, ''))
  })
}

const exchanges = []

for (let name of config.exchanges) {
  const exchange = new (require('./src/exchanges/' + name))()

  config.exchanges[config.exchanges.indexOf(name)] = exchange.id

  exchanges.push(exchange)
}

/* Start server
 */

const server = new Server(exchanges)

/* Backup server on SIGINT
 */
process.on('SIGINT', async function () {
  console.log('\nSIGINT')

  if (!server.canExit()) {
    return
  }

  if (config.collect) {
    if (alertService.enabled) {
      try {
        await alertService.persistAlerts()
        console.log(`[exit] saved alerts ✓`)
      } catch (error) {
        console.error(`[exit] failed to save alerts`, error.message)
      }
    }

    if (config.persistConnections) {
      try {
        await saveConnections(true)
        console.log(`[exit] saved connections ✓`)
      } catch (error) {
        console.error(`[exit] failed to save connections`, error.message)
      }
    }

    try {
      await server.backupTrades(true)
      console.log(`[exit] saved trades ✓`)
    } catch (error) {
      console.error(`[exit] failed to save trades`, error.message)
    }
  }

  try {
    await socketService.close()
    console.log(`[exit] closed sockets ✓`)
  } catch (error) {
    console.error(`[exit] failed to close sockets`, error.message)
  }

  console.log('[init] goodbye')

  process.exit()
})

if (process.env.pmx) {
  tx2.action('connect', function (markets, reply) {
    server
      .connect(markets.split(','))
      .then(result => {
        reply(result.join(', '))
      })
      .catch(err => {
        reply(`FAILED to connect ${markets} (${err.message})`)
      })
  })

  tx2.action('disconnect', function (markets, reply) {
    server
      .disconnect(markets.split(','))
      .then(result => {
        reply(result.join(', '))
      })
      .catch(err => {
        reply(`FAILED to disconnect ${markets} (${err.message})`)
      })
  })

  tx2.action('recover', async function (params, reply) {
    if (!params || params.split(' ').length < 3) {
      reply('Invalid parameters. Expected format: <exchange:pair> <from> <to>')
      return
    }

    const [market, from, to] = params.split(' ')
    const [id, pair] = market.match(/([^:]*):(.*)/).slice(1, 3)
    const exchange = server.exchanges.find(
      e => e?.id === id
    )

    if (!exchange) {
      reply(`Unknown exchange ${id}`)
      return
    }

    if (typeof exchange?.getMissingTrades === 'function') {
      try {
        const range = {
          pair,
          from: +new Date(from),
          to: +new Date(to)
        }

        /**
         * @type {FilesStorage}
         */
        const fileStorage = server.storages.find(s => s.constructor.name === 'FilesStorage')

        if (fileStorage) {
          fileStorage.clearRange(market, range.from, range.to)
        }

        const recoveredCount = await exchange.getMissingTrades(range)

        if (recoveredCount) {
          reply(`${recoveredCount} trades recovered`)
        } else {
          reply(`no trade were recovered`)
        }
      } catch (error) {
        const message = `[${id}.recoverTrades] something went wrong while recovering ${pair}'s missing trades`
        console.error(message, error.message)
        reply(message, error.message)
      }
    } else {
      reply(`Can't getMissingTrades on ${id}`)
    }
  })

  tx2.action('triggeralert', function (user, reply) {
    // offline webpush testing
    try {
      const result = server.triggerAlert(user)
      reply(result)
    } catch (error) {
      console.error('FAILED to trigger alert', user, error.message)
    }
  })
}
