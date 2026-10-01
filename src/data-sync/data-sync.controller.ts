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
      const result = await this.applier.apply(message);
      channel.ack(rawMessage);
      // A skip still sends APPLIED — head office may have missed the original
      // ack. Report the version the store holds, never message.version: an
      // older replay would otherwise move head office's applied version back.
      await this.confirmApplied(
        message.datasetType,
        result.version,
        result.contentHash,
      );
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
      await this.sendAck(
        message.datasetType,
        message.version,
        'FAILED',
        message.contentHash,
        reason,
      );
    }
  }

  /**
   * APPLIED is sent after the message is already acked, so a publish failure
   * must not fall into the FAILED path (that would nack an acked message and
   * report a healthy store as failed). A lost ack is recoverable: the next
   * redelivery of the same version re-confirms it via the skip path.
   */
  private async confirmApplied(
    datasetType: string,
    version: number,
    contentHash?: string,
  ): Promise<void> {
    try {
      await this.sendAck(datasetType, version, 'APPLIED', contentHash);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Could not send APPLIED ack for ${datasetType} v${version}: ${reason}`,
      );
    }
  }

  private async sendAck(
    datasetType: string,
    version: number,
    status: SyncAckStatus,
    contentHash?: string,
    error?: string,
  ): Promise<void> {
    const ack: SyncAck = {
      storeCode: STORE_CODE,
      datasetType,
      version,
      status,
      ...(contentHash ? { contentHash } : {}),
      ...(error ? { error } : {}),
    };
    // emit() is a cold Observable — nothing is published until subscribed.
    await lastValueFrom(
      this.ackClient.emit(buildAckRoutingKey(datasetType, STORE_CODE), ack),
    );
  }
}
