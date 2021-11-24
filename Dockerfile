FROM node:14
COPY . /app
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/node", "/app/index.js"]
