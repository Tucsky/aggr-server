## How to install
1. Clone the repo

```bash
git clone https://github.com/Tucsky/aggr-server api/
```

2. Install dependencies

```bash
npm install
```

3. Run server

```bash
node index
```

...

## How to install: Docker

```
➜ docker-compose build
➜ docker-compose up -d
```
This will give you a running server on <http://127.0.0.1:3000> with mounted `./data` volume.
See `./env` file for some basic configuration.
Watch logs using `docker logs -f st-server`.
Uncomment `influx` part in `docker-compose.yml` and set `STORAGE=influx` in `.env` to start using influxdb as a storage.

5. Profit !

## Configuration
All settings are optional and can be changed in the [server configuration file](config.json.example) (rename config.json.example into config.json as the real config file is untracked on github).

```js
// see [server configuration file](src/config.js) for all server options
```

All options can be set using CLI
- Setting port `node index port=3001`
- Setting port & pair `node index port=3002 pairs="COINBASE:ETH-USD"`
- Setting port & multiple pairs `node index port=3002 pair="COINBASE:ETH-USD,BINANCE:ethusdt"`

*Like whats been done here ?* Donate<br>
[3NuLQsrphzgKxTBU3Vunj87XADPvZqZ7gc](bitcoin:3NuLQsrphzgKxTBU3Vunj87XADPvZqZ7gc)
