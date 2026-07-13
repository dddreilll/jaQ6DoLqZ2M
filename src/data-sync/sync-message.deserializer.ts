import { Deserializer } from '@nestjs/microservices';
import { SyncMessage } from './data-sync.contracts';

/**
 * REQUIRED override. Nest's default deserializer expects a `{ pattern, data }`
 * wrapper; CDH messages are raw JSON, so without this every message resolves
 * to `pattern: undefined`, matches no handler, and is nacked to the
 * dead-letter exchange.
 *
 * The pattern is reconstructed from the message itself so Nest's wildcard
 * matching (`wildcards: true`) can dispatch to the right @EventPattern
 * handler. Anything that isn't a message keeps `pattern: undefined` and
 * dead-letters — which is the correct fate for foreign messages.
 */
export class SyncMessageDeserializer implements Deserializer {
  deserialize(value: unknown) {
    const message = value as SyncMessage | null;
    if (message && typeof message === 'object' && message.datasetType) {
      const pattern =
        message.scope === 'GLOBAL'
          ? `dataset.${message.datasetType}.global`
          : `dataset.${message.datasetType}.store.${message.scopeId}`;
      return { pattern, data: message };
    }
    return { pattern: undefined, data: value };
  }
}
