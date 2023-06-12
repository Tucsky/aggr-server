# Aggr server

Autonomous multi market trades monitoring, storing and resampling solution

![aggr-server](https://i.imgur.com/slF3jDy.png)

## How to install

To install the application, follow these steps:

1. Clone the repository and navigate to the working directory:

    ```bash
    git clone https://github.com/Tucsky/aggr-server
    cd aggr-server
    ```

2. Install the dependencies:

    ```bash
    npm install
    ```

3. If desired, customize the server settings using JSON configuration. Move the example file to the root directory and edit it:

    ```bash
    cp config.json.example config.json
    nano config.json
    ```

4. Run server

    ```bash
    node index
    ```

    Note: If you haven't created the config.json file during step 3, the application will detect it  and automatically create it for you.

## Configuration

All settings are optional and can be customized in the [server configuration file](config.json.example).

See [server configuration file](src/config.js) for all server options available.

Users can set configuration options using the CLI. Examples of the command-line arguments are:

- Setting port:
  
  ```bash
    node index port=3001
  ```

- Setting port & pair:
  
  ```bash
        node index port=3002 pairs="COINBASE:ETH-USD"
  ````

- Setting port & multiple pairs:

    ```bash
        node index port=3002 pair="COINBASE:ETH-USD,BINANCE:ethusdt"
    ````

You may use specific config file using the `config` argument:

```bash
    node index config=custom.config.json
```

## InfluxDB

This application supports InfluxDB v1.8.X. You can download it from the software editor website or use Docker to set it up.

To start using InfluxDB, set `storage=[ 'influx' ]` in `config.json`. You can also save the data in files at the same time you are recording it into InfluxDB by setting `storage=[ 'files', 'influx' ]`.

Please note that InfluxDB auth is disabled, and connecting to a remote InfluxDB has not yet been tested.

## Installation with Docker

To install the application with Docker, follow these steps:

Create a config file based on the existing template living in the project's root folder:
`cp config.json.example config.json`

Modify the Dockerfile accordingly by replacing the following line:

```txt
COPY  config.json.example ${WORKDIR}
```

by

```txt
COPY  config.json ${WORKDIR}
```

If you skip this step, you'll be running `aggr-server` in its default setup.

## Running Docker

We provide several `docker-compose` services that you could run as containers to help you setting up and running this app. Besides, the main application, all other services are commented out. You will have to uncomment the appropriate lines in order to be able to run them.

The three services provided are:

1. 'server': the main application container
2. 'influx': a container that allows you to store collected data in an InfluxDB database
3. 'chronograf': a container that allows you to easily visualize the data stored in InfluxDB

Here are 3 of the most likely scenarios:

1. Running `aggr-server` on Docker without a database.

    Change directory to the project's root folder and type in terminal:

    ```bash
        // create a docker image for the main application from `.Dockerfile`
        ➜ docker-compose build
        ➜ docker-compose up -d
    ```

    Note: if you haven't ran the application yet or haven't created a `config.json`, the  `docker-compose build` will fail. See [## How to install
    ](## How to install)

    This will give you a running server on <http://127.0.0.1:3000> with mounted `./data` volume for persistence.

    Default configuration should allow you to run this set up without further configuration. See `./env` file for basic configuration.

2. Running `aggr-server` locally with an InfluxDB v1.8 database on Docker.

    - Uncomment  `influx` service in `docker-compose.yml`
    - In `config.json` file, add 'influx' to `storage` array
    - In `.env` file,  uncomment `INFLUX_PORT` and `INFLUX_HOST`

    ```bash
        // run database container - build is not needed
        docker-compose up influx
        
        // run server
        node index
    ```

3. Running `aggr-server` and an InfluxDB v1.8 database on Docker.

    - Uncomment  `influx` service in `docker-compose.yml`
    - In `config.json`, add 'influx' to `storage` array
    - In `.env`,  uncomment `INFLUX_PORT` and `INFLUX_HOST`.

    ```bash
        // build main application image
        docker-compose build

        // run main application and database containers
        docker-compose up
    ```

## Working with Clusters

When monitoring hundreds of markets, users may want to run multiple instances of this project. This server is designed to work with multiple collectors instances plus one API node:

- A collector is dedicated to listening for trades and storing the data of a given set of markets (using InfluxDB).
- An API node serves the data to the client, using InfluxDB as a main source but *WIll* query the collectors to ensure all data is sent, including the one *NOT YET* stored in InfluxDB.

To run the application with multiple instances:

1. Create two configuration files using Influx storage: one for the API node (with api set to true and collect set to false) and one for the collectors nodes (with api set to false and collect set to true). Both files should have influxCollectors enabled.
2. Use the following command to run one API instance and two collectors

    ```bash
    // API node
    node index config=api.config.json

    // collector A
    node index config=collector.config.json pairs="COINBASE:ETH-USD,BITSTAMP:ethusdt"

    // collector B
    node index config=collector.config.json pairs="COINBASE:BTC-USD,BITSTAMP:btcusdt"
    ```

## Troubleshooting

### Port already in use

If you get an error message `Error: listen EADDRINUSE :::3000`, it means that the server is already running on this port.`
You can kill the process by running `kill $(lsof -t -i:3000)`.

### Docker container logs

You can check the logs of your Docker containers by running `docker-compose logs -f`.

## Contributing

We welcome contributions to this project. If you find any issues, please report them on the GitHub issues page. Pull requests are also welcome.

## If you like what is being done here, consider supporting this project

ETH [0xe3c893cdA4bB41fCF402726154FB4478Be2732CE](https://etherscan.io/address/0xe3c893cdA4bB41fCF402726154FB4478Be2732CE)\
BTC [3PK1bBK8sG3zAjPBPD7g3PL14Ndux3zWEz](bitcoin:3PK1bBK8sG3zAjPBPD7g3PL14Ndux3zWEz)\
XMR 48NJj3RJDo33zMLaudQDdM8G6MfPrQbpeZU2YnRN2Ep6hbKyYRrS2ZSdiAKpkUXBcjD2pKiPqXtQmSZjZM7fC6YT6CMmoX6\
COINBASE
<https://commerce.coinbase.com/checkout/c58bd003-5e47-4cfb-ae25-5292f0a0e1e8>
