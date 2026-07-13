import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { CDH_EXCHANGE, DATASYNC_ACK_CLIENT } from './data-sync/data-sync.contracts';
import { DataSyncController } from './data-sync/data-sync.controller';
import { DatasetApplierService } from './data-sync/dataset-applier.service';
import { LocalDatasetStore } from './data-sync/local-dataset.store';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: DATASYNC_ACK_CLIENT,
        transport: Transport.RMQ,
        options: {
          urls: [process.env.CDH_RABBITMQ_URL!],
          exchange: CDH_EXCHANGE,
          // emit(pattern, ...) publishes to the exchange with the pattern as
          // routing key — exactly what the ack contract needs.
          wildcards: true,
          // don't assert a queue of our own for a publish-only client
          noAssert: true,
          persistent: true,
          // REQUIRED override: send the ack as a raw JSON body, not Nest's
          // { pattern, data } wrapper (head office parses raw acks).
          serializer: { serialize: (packet) => packet.data },
        },
      },
    ]),
  ],
  controllers: [DataSyncController],
  providers: [LocalDatasetStore, DatasetApplierService],
})
export class AppModule {}
