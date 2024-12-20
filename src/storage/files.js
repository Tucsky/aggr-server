const fs = require('fs')
const zlib = require('zlib')
const {
  groupTrades,
  ensureDirectoryExists,
  getHms,
  humanFileSize
} = require('../helper')
const config = require('../config')

class FilesStorage {
  constructor() {
    this.name = this.constructor.name
    this.format = 'trade'

    /** @type {{[timestamp: string]: {stream: fs.WriteStream, timestamp: number}}} */
    this.writableStreams = {}

    if (!config.filesInterval) {
      config.filesInterval = 3600000 // 1h file default
    }

    if (!fs.existsSync(config.filesLocation)) {
      fs.mkdirSync(config.filesLocation)
    }

    console.log(
      `[storage/${this.name}] destination folder: ${config.filesLocation}`
    )
  }

  /**
   * Construit le nom du fichier a partir d'une date
   * BTCUSD_2018-12-01-22
   *
   * @param {Date} identifier ex bitmex:XBTUSD, kraken:XBT/USD etc
   * @param {Date} date
   * @returns {string}
   * @memberof FilesStorage
   */
  getBackupFilename(identifier, date) {
    let [, exchange, pair] = identifier.match(/([^:]*):(.*)/)

    pair = pair.replace(/[/:]/g, '-')

    const folderPart = `${config.filesLocation}/${exchange}/${pair}`
    const datePart = `${date.getUTCFullYear()}-${(
      '0' +
      (date.getUTCMonth() + 1)
    ).slice(-2)}-${('0' + date.getUTCDate()).slice(-2)}`

    let file = `${folderPart}/${datePart}`

    if (config.filesInterval < 1000 * 60 * 60 * 24) {
      file += `-${('0' + date.getUTCHours()).slice(-2)}`
    }

    if (config.filesInterval < 1000 * 60 * 60) {
      file += `-${('0' + date.getUTCMinutes()).slice(-2)}`
    }

    if (config.filesInterval < 1000 * 60) {
      file += `-${('0' + date.getUTCSeconds()).slice(-2)}`
    }

    return file.replace(/\s+/g, '')
  }

  async addWritableStream(identifier, ts) {
    const date = new Date(+ts)
    const path = this.getBackupFilename(identifier, date)

    try {
      await ensureDirectoryExists(path)
    } catch (error) {
      console.error(
        `[storage/${this.name}] failed to create target directory ${path}`,
        error
      )
    }

    const stream = fs.createWriteStream(path, { flags: 'a' })

    this.writableStreams[identifier + ts] = {
      timestamp: +ts,
      stream
    }

    stream.on('error', err => {
      console.error(
        `[storage/${this.name}] ${path} stream encountered an error\n\t${err.message}`
      )
    })

    console.debug(
      `[storage/${
        this.name
      }] created writable stream ${date.toUTCString()} => ${path}`
    )
  }

  reviewStreams() {
    const now = +new Date()

    for (let id in this.writableStreams) {
      // close 5 min (config.filesCloseAfter) after file expiration
      if (
        now >
        this.writableStreams[id].timestamp +
          config.filesInterval +
          config.filesCloseAfter
      ) {
        const path = this.writableStreams[id].stream.path

        console.debug(`[storage/${this.name}] close writable stream ${id}`)

        try {
          this.writableStreams[id].stream.end()
        } catch (error) {
          console.error(
            `[storage/${this.name}] failed to 'end' writable stream ${id}`,
            error
          )
        }

        delete this.writableStreams[id]

        if (config.filesGzipAfterUse) {
          try {
            this.gzipFileAndRemoveRaw(path)
          } catch (error) {
            console.error(
              `[storage/${this.name}] failed to gzip & remove original file "${path}"`,
              error
            )
          }
        }
      }
    }
  }

  gzipFileAndRemoveRaw(path) {
    let rawPath = path

    if (/.gz$/.test(rawPath)) {
      rawPath = rawPath.replace(/\.gz$/, '')
    }

    return new Promise(resolve => {
      fs.createReadStream(rawPath)
        .pipe(zlib.createGzip())
        .pipe(fs.createWriteStream(`${rawPath}.gz`))
        .on('finish', () => {
          console.debug(`[storage/${this.name}] gziped ${rawPath}`)
          fs.unlink(rawPath, err => {
            if (err) {
              console.error(
                `[storage/${this.name}] failed to remove original file after gzip compression (${rawPath})`
              )
            }

            resolve()
          })
        })
        .on('error', err => {
          console.debug(
            `[storage/${this.name}] error while removing/compressing trade file ${rawPath}\n\t${err.message}`
          )
        })
    })
  }

  prepareTrades(trades) {
    const groups = groupTrades(trades, false)

    const output = Object.keys(groups).reduce((obj, pair) => {
      obj[pair] = {}
      return obj
    }, {})

    for (let identifier in groups) {
      for (let i = 0; i < groups[identifier].length; i++) {
        const trade = groups[identifier][i]

        const ts =
          Math.floor(trade[0] / config.filesInterval) * config.filesInterval

        if (!output[identifier][ts]) {
          output[identifier][ts] = {
            from: trade[0],
            to: trade[0],
            data: ''
          }
        }

        output[identifier][ts].data += trade.join(' ') + '\n'
        output[identifier][ts].to = trade[0]
      }
    }

    return output
  }

  save(trades) {
    return new Promise(resolve => {
      const output = this.prepareTrades(trades)

      const promises = []

      for (let identifier in output) {
        for (let ts in output[identifier]) {
          promises.push(
            new Promise(resolve => {
              let promiseOfWritableStram = Promise.resolve()

              if (!this.writableStreams[identifier + ts]) {
                promiseOfWritableStram = this.addWritableStream(identifier, ts)
              }

              promiseOfWritableStram.then(() => {
                const stream = this.writableStreams[identifier + ts].stream

                if (!stream) {
                  console.error(
                    `[storage/${this.name}] ${identifier}'s stream already closed`
                  )
                  return resolve()
                }

                const waitDrain = !stream.write(
                  output[identifier][ts].data,
                  err => {
                    if (err) {
                      console.error(
                        `[storage/${this.name}] stream.write encountered an error\n\t${err}`
                      )
                    }

                    if (!waitDrain) {
                      resolve()
                    }
                  }
                )

                if (waitDrain) {
                  let drainTimeout = setTimeout(() => {
                    console.log(
                      `[storage/${this.name}] ${identifier}'s stream drain timeout fired`
                    )
                    drainTimeout = null
                    resolve()
                  }, 5000)

                  stream.once('drain', () => {
                    if (drainTimeout) {
                      clearTimeout(drainTimeout)
                      resolve()
                    } else {
                      console.log(
                        `[storage/${this.name}] ${identifier}'s stream drain callback received`
                      )
                    }
                  })
                }
              })
            })
          )
        }
      }

      Promise.all(promises).then(() => resolve())
    }).then(success => {
      this.reviewStreams()

      return success
    })
  }

  async insert(trades) {
    const output = this.prepareTrades(trades)

    for (const market in output) {
      for (const fileTimestamp in output[market]) {
        const firstTradeTimestampInsert = output[market][fileTimestamp].from
        const lastTradeTimestampInsert = output[market][fileTimestamp].to
        const date = new Date(+fileTimestamp)
        let path = this.getBackupFilename(market, date)

        try {
          await ensureDirectoryExists(path)
        } catch (error) {
          console.error(
            `[storage/${this.name}] failed to create target directory ${path}`,
            error
          )
        }

        let stat = await this.statFile(path)

        if (!stat) {
          path += '.gz'
          stat = await this.statFile(path)
        }

        console.log(
          `[storage/file] insert ${getHms(
            lastTradeTimestampInsert - firstTradeTimestampInsert
          )} of trades into ${path} (${
            stat ? 'file exists' : "file doesn't exists"
          })`
        )

        if (stat) {
          // edit existing file

          // get trades arr from existing file
          let tradesFile = (await this.readFile(path)).split('\n')

          if (tradesFile[tradesFile.length - 1] === '') {
            tradesFile.pop() // remove end of file nl
          }

          // trades arr to insert
          const tradesInsert = output[market][fileTimestamp].data.split('\n')

          if (tradesInsert[tradesInsert.length - 1] === '') {
            tradesInsert.pop() // remove end of file nl
          }

          const firstTradeTimestampFile = +tradesFile[0].replace(/ .*/, '')
          const lastTradeTimestampFile = +tradesFile[
            tradesFile.length - 1
          ].replace(/ .*/, '')

          if (firstTradeTimestampFile > lastTradeTimestampInsert) {
            // insert before
            console.log(`\t prepend ${tradesInsert.length} trades (index 0)`)
            tradesFile = tradesInsert.concat(tradesFile)
          } else if (lastTradeTimestampFile < firstTradeTimestampInsert) {
            // insert after
            console.log(
              `\t append ${tradesInsert.length} trades (index ${tradesFile.length})`
            )
            tradesFile = tradesFile.concat(tradesInsert)
          } else {
            // insert in the middle
            let replaceRange = null

            // remove inner trades from file
            for (let i = 0; i < tradesFile.length; i++) {
              const tradeTimestamp = +tradesFile[i].replace(/ .*/, '')

              if (
                replaceRange === null &&
                tradeTimestamp >= output[market][fileTimestamp].from
              ) {
                replaceRange = {
                  start: i,
                  end: i
                }
              } else if (
                replaceRange &&
                (tradeTimestamp > output[market][fileTimestamp].to ||
                  i === tradesFile.length - 1)
              ) {
                replaceRange.end = i
                break
              }
            }

            if (!replaceRange || replaceRange.end < replaceRange.start) {
              throw new Error('invalid replace range')
            }

            console.log(
              `\tinsert ${tradesInsert.length} trades at index ${
                replaceRange.start
              } (while splicing ${
                replaceRange.end - replaceRange.start + 1
              } trades from original)`
            )

            const change =
              replaceRange.end - replaceRange.start - tradesInsert.length
            console.log(
              `\t merge ${tradesInsert.length} into original (net change ${
                change > 0 ? '+' : ''
              }${change} at index ${replaceRange.start})`
            )

            tradesFile.splice(
              replaceRange.start,
              replaceRange.end - replaceRange.start,
              ...tradesInsert
            )
          }

          await this.writeFile(path, tradesFile.join('\n') + '\n')
        } else {
          // new file
          console.log(`\t create new file`)
          await this.writeFile(path, output[market][fileTimestamp].data)
        }
      }
    }
  }

  statFile(path) {
    return new Promise(resolve => {
      fs.stat(path, (err, stat) => {
        if (err) {
          err.code !== 'ENOENT' && console.log('failed to stat', path, err)
          resolve(false)
        }

        resolve(stat)
      })
    })
  }

  readFile(path) {
    return new Promise((resolve, reject) => {
      fs.readFile(path, (err, data) => {
        if (err) {
          console.error(`[storage/file] failed to read file`, path)
          throw err
        }

        if (/\.gz$/.test(path)) {
          zlib.gunzip(data, (err, buffer) => {
            if (err) {
              console.error(`[storage/file] failed to unzip file`, path)
              return reject(err)
            }
            resolve(buffer.toString())
          })
        } else {
          resolve(data.toString())
        }
      })
    })
  }

  writeFile(path, content) {
    let rawPath = path

    if (/.gz$/.test(rawPath)) {
      rawPath = rawPath.replace(/\.gz$/, '')
    }

    return new Promise((resolve, reject) => {
      fs.writeFile(rawPath, content, (err, data) => {
        if (err) {
          console.error(`[storage/file] failed to write file`, path)
          throw err
        }

        if (/\.gz$/.test(path)) {
          resolve(this.gzipFileAndRemoveRaw(path))
        } else {
          resolve()
        }
      })
    })
  }

  async clearRange(identifier, from, to) {
    const startInterval =
      Math.floor(from / config.filesInterval) * config.filesInterval
    const endInterval =
      Math.floor(to / config.filesInterval) * config.filesInterval

    for (
      let ts = startInterval;
      ts <= endInterval;
      ts += config.filesInterval
    ) {
      const basePath = this.getBackupFilename(identifier, new Date(ts))

      let stat = await this.statFile(basePath)
      let finalPath = basePath
      if (!stat) {
        // Try gz
        stat = await this.statFile(basePath + '.gz')
        if (stat) {
          finalPath = basePath + '.gz'
        } else {
          // No file at all, skip
          continue
        }
      }

      let fileContent = await this.readFile(finalPath)
      if (!fileContent) {
        continue
      }

      let lines = fileContent.split('\n')
      if (lines[lines.length - 1] === '') {
        // Remove trailing empty line if any
        lines.pop()
      }

      // Parse and sort by timestamp (just in case, though they are likely already sorted)
      // Format: "timestamp price volume side"
      lines = lines.map(line => line.trim()).filter(line => line !== '')
      lines.sort((a, b) => {
        const aTs = Number(a.split(' ')[0])
        const bTs = Number(b.split(' ')[0])
        return aTs - bTs
      })

      // Filter out trades that intersect with [from, to]
      lines = lines.filter(line => {
        const tradeTs = Number(line.split(' ')[0])
        return tradeTs < from || tradeTs > to
      })

      // Write the updated content back to the file
      const newContent = lines.length ? lines.join('\n') + '\n' : ''
      await this.writeFile(finalPath, newContent)
    }
  }

  fetch() {
    // unsupported
    console.error(
      '[storage/file] historical data request not supported by this storage type (raw trade files)'
    )
    return Promise.resolve({
      format: this.format,
      results: []
    })
  }
}

module.exports = FilesStorage
