import { Logger } from '@nestjs/common';
import { ServerRMQ } from '@nestjs/microservices';

/**
 * @nestjs/microservices' ServerRMQ consumes on the raw amqplib channel
 * (bypassing amqp-connection-manager's own ChannelWrapper.consume(), which
 * auto-resubscribes on broker-initiated cancellation). When the underlying
 * queue is deleted out-of-band (ops action, another consumer, a management
 * tool), RabbitMQ sends a `basic.cancel` — amqplib surfaces this as a `null`
 * message — and ServerRMQ.handleMessage() just returns, leaving the consumer
 * permanently dead with no error or log anywhere. This subclass re-runs
 * setupChannel() (reassert queue, rebind, re-consume) on that signal instead.
 */
export class ResilientServerRMQ extends ServerRMQ {
  private readonly resilientLogger = new Logger(ResilientServerRMQ.name);

  override async handleMessage(
    message: Record<string, any> | null,
    channel: any,
  ): Promise<void> {
    if (message === null || message === undefined) {
      this.resilientLogger.warn(
        `Consumer for "${this.queue}" was cancelled by the broker (queue deleted or recreated out-of-band) — re-subscribing`,
      );
      try {
        await this.setupChannel(channel, () => {});
      } catch (err) {
        this.resilientLogger.error(
          `Failed to re-subscribe to "${this.queue}" after broker cancel: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
      return;
    }

    return super.handleMessage(message, channel);
  }
}
