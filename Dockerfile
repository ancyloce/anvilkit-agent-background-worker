# anvilkit-agent-background-worker: built from this repository alone (the
# build context is the repository root; nothing from the parent checkout is
# read). The generated contract consumers (@anvilkit/generated-clients) are a
# git-hosted dependency of the contracts repository pinned by commit and
# integrity in pnpm-lock.yaml; the events schema the relay validates
# messages against (events/events.schema.json) comes from the contracts
# repository as a named build context, for example a checkout of the pinned
# commit:
#   docker build --build-context contracts=../../../contracts -t anvilkit-agent-background-worker .
# The image takes only the document of the pinned commit: its digest is
# checked below. One image, two entries: `worker` (the default) and
# `relay` (run beside each owner by that owner's chart). No lifecycle
# script runs in the installs (pnpm-workspace.yaml).
FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df AS build
WORKDIR /src
RUN npm install -g pnpm@12.3.4
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json build.mjs ./
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY src ./src
RUN pnpm run build
RUN pnpm install --frozen-lockfile --ignore-scripts --prod

FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df
WORKDIR /anvilkit/background-worker
# The runtime runs node only: the base image's npm, npx and corepack are
# removed so their bundled packages (and their advisories) are not shipped.
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack \
      /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
COPY --from=build /src/node_modules ./node_modules
COPY --from=build /src/dist ./dist
COPY package.json ./package.json
COPY config.yaml /etc/anvilkit/anvilkit-agent-background-worker/config.yaml
COPY --from=contracts events/events.schema.json /anvilkit/contracts/events/events.schema.json
# events/events.schema.json of contracts commit 1307fb26895509b690e315bab3dd180ad3a0c4e6
# (tag go/v0.1.9, the commit @anvilkit/generated-clients is pinned to; line
# endings normalized: a CRLF checkout carries the same document).
ARG CONTRACT_SHA256=89c5569ccf1d5cf6c3e12cf4ba4d71036fe0334044d06fa3a6c69052eb0c0aff
RUN test "$(tr -d '\r' < /anvilkit/contracts/events/events.schema.json | sha256sum | cut -d' ' -f1)" = "$CONTRACT_SHA256" \
 || { echo "events/events.schema.json is not the document of the pinned contracts revision" >&2; exit 1; }
ENV ANVILKIT_BACKGROUND_WORKER_CONFIG=/etc/anvilkit/anvilkit-agent-background-worker/config.yaml \
    ANVILKIT_BACKGROUND_WORKER_CONTRACTS_DIR=/anvilkit/contracts \
    NODE_ENV=production
USER 65532:65532
ENTRYPOINT ["node", "/anvilkit/background-worker/dist/main.js"]
CMD ["worker"]
