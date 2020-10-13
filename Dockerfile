FROM node:12.18
COPY . /app
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/node", "/app/index.js"]
