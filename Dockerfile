FROM  mhart/alpine-node:12.1 as builder
COPY  package*.json /
RUN   set ex && npm install --production

FROM  mhart/alpine-node:slim-12.1

ARG   WORKDIR
ARG   PORT
ARG   FILES_LOCATION
ARG   INFLUX_URL
ARG   STORAGE
ARG   CONFIG
ARG   CONFIG_BOL

ENV   PORT $PORT
ENV   WORKDIR $WORKDIR
ENV   FILES_LOCATION $FILES_LOCATION
ENV   INFLUX_URL $INFLUX_URL
ENV   STORAGE $STORAGE
ENV   CONFIG $CONFIG
ENV   CONFIG_CMD=${CONFIG_BOL:+config=}

WORKDIR /$WORKDIR

RUN   apk add --no-cache tini

COPY  --from=builder /node_modules  ${WORKDIR}/node_modules
COPY  src ${WORKDIR}/src
COPY  index.js ${WORKDIR}
COPY  config.json.example ${WORKDIR}
COPY  ${CONFIG} ${WORKDIR}

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["sh", "-c", "node index ${CONFIG_CMD}${CONFIG}"]