FROM  node:current-alpine3.18 as builder

COPY  package*.json /
RUN   set ex && npm install --production

FROM  node:20.2.0-slim

WORKDIR /$WORKDIR

RUN apt-get update && \
	apt-get install -y tini && \
	npm install pm2 -g

COPY  --from=builder /node_modules $WORKDIR/node_modules
COPY  src ${WORKDIR}/src
COPY  index.js ${WORKDIR}
COPY  config.json ${WORKDIR}
COPY  ecosystem.config.js ${WORKDIR}

EXPOSE ${PORT}

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD [ "pm2-runtime", "ecosystem.config.js"]