const fs = require('fs')

console.log('PID: ', process.pid)

const config = require('./src/config')
const Server = require('./src/server')
const alertService = require('./src/services/alert')
const socketService = require('./src/services/socket')

/* Load available exchanges
 */

if (!config.exchanges || !config.exchanges.length) {
  config.exchanges = []

  fs.readdirSync('./src/exchanges/').forEach((file) => {
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

  if (config.collect) {
    try {
      await alertService.persistAlerts()
      console.log(`[exit] saved alerts ✓`)
    } catch (error) {
      console.error(`[exit] failed to save alerts`, error.message)
    }
  
    try {
      await server.backupTrades(true)
      console.log(`[exit] saved last trades ✓`)
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
