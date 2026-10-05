import { Controller, Inject, Logger } from '@nestjs/common';
import {
  ClientProxy,
  Ctx,
  EventPattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';
import { Channel, Message } from 'amqplib';
import { lastValueFrom } from 'rxjs';
import {
  buildAckRoutingKey,
  DATASYNC_ACK_CLIENT,
  SyncAck,
  SyncAckStatus,
  SyncMessage,
} from './data-sync.contracts';
import {
  ApplyResult,
  DatasetApplierService,
  DatasetGapError,
} from './dataset-applier.service';

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

  // noAck: false — every path must end in exactly one ack or nack. A message
  // that reaches a handler was read, so it ends in ack, failures included;
  // only the deserializer nacks (a message that can't be read at all).
  private async handle(message: SyncMessage, context: RmqContext) {
    const channel = context.getChannelRef() as Channel;
    const rawMessage = context.getMessage() as Message;
    let result: ApplyResult;
    try {
      result = await this.applier.apply(message);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      if (error instanceof DatasetGapError) {
        // Head office shows the store as failed and resends a full snapshot.
        this.logger.warn(`${reason} — reporting FAILED`);
      } else {
        this.logger.error(
          `Failed to apply ${message.datasetType} v${message.version}: ${reason}`,
        );
      }
      await this.reportFailure(message, reason, channel, rawMessage);
      return;
    }
    channel.ack(rawMessage);
    // A skip still confirms, as SKIPPED — head office may have missed the
    // original ack. Report the version the store holds, never message.version:
    // an older replay would otherwise move head office's applied version back.
    await this.confirm(
      message.datasetType,
      result.outcome === 'skipped' ? 'SKIPPED' : 'APPLIED',
      result.version,
      result.contentHash,
    );
  }

  /**
   * Read but not applied (an apply error or a gap): send the FAILED ack with
   * the reason first, then ack the message, so a crash in between redelivers
   * it instead of losing the failure. Never nack it — the FAILED ack is the
   * report. Only if that ack can't be sent is the message nacked, so RabbitMQ
   * dead-letters it and head office still sees a failure.
   */
  private async reportFailure(
    message: SyncMessage,
    reason: string,
    channel: Channel,
    rawMessage: Message,
  ): Promise<void> {
    try {
      await this.sendAck(
        message.datasetType,
        message.version,
        'FAILED',
        message.contentHash,
        reason,
      );
      channel.ack(rawMessage);
    } catch (error) {
      this.logger.error(
        `Could not send FAILED ack for ${message.datasetType} v${message.version}, dead-lettering instead: ${error instanceof Error ? error.message : String(error)}`,
      );
      channel.nack(rawMessage, false, false); // requeue=false → dead-letter exchange
    }
  }

  /**
   * APPLIED and SKIPPED are sent after the message is already acked, so a
   * publish failure must not fall into the FAILED path (that would report a
   * healthy store as failed). A lost ack is recoverable: the next redelivery
   * of the same version re-confirms it via the skip path.
   */
  private async confirm(
    datasetType: string,
    status: 'APPLIED' | 'SKIPPED',
    version: number,
    contentHash?: string,
  ): Promise<void> {
    try {
      await this.sendAck(datasetType, version, status, contentHash);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Could not send ${status} ack for ${datasetType} v${version}: ${reason}`,
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
