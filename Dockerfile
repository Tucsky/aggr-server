FROM  node:20.2.0-alpine3.17 as builder
COPY  package*.json /
RUN   set ex && npm install --production

FROM  node:20.2.0-slim

ARG   WORKDIR

ENV   WORKDIR $WORKDIR

WORKDIR /$WORKDIR

RUN   apk add --no-cache tini

COPY  --from=builder /node_modules  ${WORKDIR}/node_modules
COPY  src ${WORKDIR}/src
COPY  index.js ${WORKDIR}
COPY  config.json ${WORKDIR}

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/usr/bin/node", "index"]