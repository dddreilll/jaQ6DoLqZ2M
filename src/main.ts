// MUST be first: @EventPattern decorator args read process.env.STORE_CODE at
// import time, before Nest (or ConfigModule) would ever load the .env file.
import 'dotenv/config';

import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { CDH_DLX, CDH_EXCHANGE } from './data-sync/data-sync.contracts';
import { ResilientServerRMQ } from './data-sync/resilient-server-rmq';
import { SyncMessageDeserializer } from './data-sync/sync-message.deserializer';

async function bootstrap(): Promise<void> {
  const storeCode = process.env.STORE_CODE;
  if (!storeCode) {
    throw new Error('STORE_CODE is required (set it in .env)');
  }

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      strategy: new ResilientServerRMQ({
        urls: [process.env.CDH_RABBITMQ_URL!],
        queue: `q.store.${storeCode}`,
        queueOptions: {
          durable: true,
          arguments: { 'x-dead-letter-exchange': CDH_DLX },
        },
        exchange: CDH_EXCHANGE,
        exchangeType: 'topic',
        wildcards: true, // handler patterns become queue bindings + dispatch
        noAck: false, // manual ack — handlers ack/nack every message
        prefetchCount: 10,
        deserializer: new SyncMessageDeserializer(),
      }),
    },
  );
  app.enableShutdownHooks();
  await app.listen();
  new Logger('Bootstrap').log(
    `Store ${storeCode} consuming q.store.${storeCode} on ${CDH_EXCHANGE}`,
  );
}

void bootstrap();
