FROM node:22.22.0-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json tsconfig.build.json tsconfig.demo.json ./
COPY scripts ./scripts
COPY src ./src
COPY demo ./demo
RUN npm run build

# Demo scripts are compiled in a separate stage and are not part of the
# production image.
FROM build AS demo-build
RUN npm run build:demo

FROM node:22.22.0-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --from=build /app/dist ./dist
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
USER node
EXPOSE 9464
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["node", "dist/index.js"]

# Demo stage. Compose profile services set the command explicitly; the
# `kafka-lens` service builds the `runtime` target so it keeps the server CMD.
FROM runtime AS demo
COPY --from=demo-build /app/dist-demo ./dist-demo
ENTRYPOINT []
