![aggr-server](https://i.imgur.com/slF3jDy.png)

Autonomous multi market trades monitoring, storing and resampling solution.

## How to install
1. Clone the repo and get into the working dir

```bash
git clone https://github.com/Tucsky/aggr-server
```

```bash
cd aggr-server
```

2. Install dependencies

```bash
npm install
```

3. If you want to configure server using json, move exemple as "config.json" inside root directory and edit configuration.

```bash
cp config.json.example config.json
```

```bash
nano config.json
```

3. Run server

```bash
node index
```

## Configuration
All settings are optional and can be changed in the [server configuration file](config.json.example) (rename config.json.example into config.json).

```js
// see [server configuration file](src/config.js) for all server options
```


All options can be set using CLI
- Setting port `node index port=3001`
- Setting port & pair `node index port=3002 pairs="COINBASE:ETH-USD"`
- Setting port & multiple pairs `node index port=3002 pair="COINBASE:ETH-USD,BINANCE:ethusdt"`

You may use specific config file using the `config` argument :

```bash
node index config=custom.config.json
```
## Working with clusters

When watching hundred of markets you may want to run multiple instances of this project.

This server is now designed to work with multiple *collectors* instances + one *api* node
- A collector is dedicated to listening for trades and storing the data of a given set of markets (using influxDB)
- A api node serves the data to the client, using influxDB as a main source but *WILL* query the collectors in order to ensure ALL data is send including the one *NOT YET* stored in influxdb

Say you have 2 config files using influx storage : 
- one for the api node (api set to true, collect set to false)
- one for the collectors nodes (api false and collect true)
Both with `influxCollectors` enabled

Then use with 1 api instance and 2 collectors

```bash
node index config=api.config.json
```

```bash
node index config=collector.config.json pairs="COINBASE:ETH-USD,BITSTAMP:ethusdt"
```

```bash
node index config=collector.config.json pairs="COINBASE:BTC-USD,BITSTAMP:btcusdt"
```

## How to install: Docker

```
➜ docker-compose build
➜ docker-compose up -d
```
This will give you a running server on <http://127.0.0.1:3000> with mounted `./data` volume.

See `./env` file for some basic configuration.

Watch logs using `docker logs -f st-server`.

Uncomment `influx` part in `docker-compose.yml` and set `STORAGE=influx` in `.env` to start using influxdb as a storage.

## If you like what is being done here, consider supporting this project !
ETH [0xe3c893cdA4bB41fCF402726154FB4478Be2732CE](https://etherscan.io/address/0xe3c893cdA4bB41fCF402726154FB4478Be2732CE)<br>
BTC [3PK1bBK8sG3zAjPBPD7g3PL14Ndux3zWEz](bitcoin:3PK1bBK8sG3zAjPBPD7g3PL14Ndux3zWEz)<br>
XMR 48NJj3RJDo33zMLaudQDdM8G6MfPrQbpeZU2YnRN2Ep6hbKyYRrS2ZSdiAKpkUXBcjD2pKiPqXtQmSZjZM7fC6YT6CMmoX6<br>
COINBASE
https://commerce.coinbase.com/checkout/c58bd003-5e47-4cfb-ae25-5292f0a0e1e8