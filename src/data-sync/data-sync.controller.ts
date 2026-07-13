import { Controller, Inject, Logger } from '@nestjs/common';
import {
  ClientProxy,
  Ctx,
  EventPattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';
import {
  buildAckRoutingKey,
  DATASYNC_ACK_CLIENT,
  SyncAck,
  SyncAckStatus,
  SyncMessage,
} from './data-sync.contracts';
import { DatasetApplierService, DatasetGapError } from './dataset-applier.service';

// Decorator arguments are evaluated when this file is imported, so STORE_CODE
// must already be in process.env — main.ts loads dotenv before anything else.
const STORE_CODE = process.env.STORE_CODE!;

/**
 * The two handler patterns double as queue bindings (wildcards: true):
 * every GLOBAL dataset plus this store's own STORE-scoped datasets.
 * Never bind `dataset.#` — it would receive every other store's data too.
 */
@Controller()
export class DataSyncController {
  private readonly logger = new Logger(DataSyncController.name);

  constructor(
    @Inject(DATASYNC_ACK_CLIENT) private readonly ackClient: ClientProxy,
    private readonly applier: DatasetApplierService,
  ) {}

  @EventPattern('dataset.*.global')
  async onGlobalDataset(
    @Payload() message: SyncMessage,
    @Ctx() context: RmqContext,
  ) {
    return this.handle(message, context);
  }

  @EventPattern(`dataset.*.store.${STORE_CODE}`)
  async onStoreDataset(
    @Payload() message: SyncMessage,
    @Ctx() context: RmqContext,
  ) {
    return this.handle(message, context);
  }

  // noAck: false — every path must end in exactly one ack or nack.
  private async handle(message: SyncMessage, context: RmqContext) {
    const channel = context.getChannelRef();
    const rawMessage = context.getMessage();
    try {
      const outcome = await this.applier.apply(message);
      channel.ack(rawMessage);
      if (outcome === 'applied') {
        await this.sendAck(message, 'APPLIED');
      }
    } catch (error) {
      channel.nack(rawMessage, false, false); // requeue=false → dead-letter exchange
      if (error instanceof DatasetGapError) {
        // Gaps are recovered with a fresh snapshot, not reported as failures.
        this.logger.warn(`${error.message} — request a snapshot from head office`);
        return;
      }
      const reason = error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to apply ${message.datasetType} v${message.version}: ${reason}`,
      );
      await this.sendAck(message, 'FAILED', reason);
    }
  }

  private async sendAck(
    message: SyncMessage,
    status: SyncAckStatus,
    error?: string,
  ): Promise<void> {
    const ack: SyncAck = {
      storeCode: STORE_CODE,
      datasetType: message.datasetType,
      version: message.version,
      status,
      contentHash: message.contentHash,
      ...(error ? { error } : {}),
    };
    // emit() is a cold Observable — nothing is published until subscribed.
    await lastValueFrom(
      this.ackClient.emit(
        buildAckRoutingKey(message.datasetType, STORE_CODE),
        ack,
      ),
    );
  }
}
