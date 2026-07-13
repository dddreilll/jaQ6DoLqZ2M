# fusion-cdh-store

Sample **store app** that consumes CDH data-sync datasets using the official
[NestJS RabbitMQ transport](https://docs.nestjs.com/microservices/rabbitmq)
(`@nestjs/microservices`, `Transport.RMQ`) — the working implementation of
**`fusion-cdh-api/docs/store-consumer-rmq-guide.md`**. Read the guide first;
this repo is the code to copy from.

## What it demonstrates

| Guide section | Where |
|---|---|
| Consumer options (queue, DLX arg, `wildcards`, `noAck: false`) | [src/main.ts](src/main.ts) |
| Required inbound deserializer (raw message → `{pattern, data}`) | [src/data-sync/sync-message.deserializer.ts](src/data-sync/sync-message.deserializer.ts) |
| The two handler patterns (`dataset.*.global` + `dataset.*.store.<code>`), manual ack | [src/data-sync/data-sync.controller.ts](src/data-sync/data-sync.controller.ts) |
| Ack client (`wildcards` + `noAssert` + raw-body serializer) | [src/app.module.ts](src/app.module.ts) |
| Applying rules: version guard, hash verify, snapshot/partial, gap handling | [src/data-sync/dataset-applier.service.ts](src/data-sync/dataset-applier.service.ts) |
| "Version + data in the same transaction" (atomic file write here; use your DB) | [src/data-sync/local-dataset.store.ts](src/data-sync/local-dataset.store.ts) |

## Run

```bash
cp .env.example .env   # set STORE_CODE + CDH_RABBITMQ_URL
npm install
npm run start:dev
```

Applied datasets land in `data/<datasetType>.json` (version + records
together). Acks are published back to head office and appear in its
`store_sync_state` table.

A real store app replaces `LocalDatasetStore` with its own database and keeps
everything else structurally identical.
