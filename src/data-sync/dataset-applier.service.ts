import { Injectable, Logger } from '@nestjs/common';
import {
  computeContentHash,
  PartialPayload,
  SyncMessage,
} from './data-sync.contracts';
import { LocalDatasetStore } from './local-dataset.store';

export type ApplyOutcome = 'applied' | 'skipped';

/** What the store holds after apply() — the version/hash its ack must report. */
export interface ApplyResult {
  outcome: ApplyOutcome;
  version: number;
  contentHash?: string;
}

/**
 * A PARTIAL arrived on the wrong base version. Never applied: the controller
 * reports it as a FAILED ack with this message as the reason (then acks it),
 * and head office recovers the store with a fresh snapshot.
 */
export class DatasetGapError extends Error {
  constructor(datasetType: string, expectedBase: number | null, actual: number) {
    super(
      `Gap on ${datasetType}: partial expects base v${expectedBase} but store is at v${actual}`,
    );
  }
}

/**
 * Implements the guide's applying rules (section 5). The pipeline is
 * at-least-once, so everything here is idempotent behind the version guard.
 */
@Injectable()
export class DatasetApplierService {
  private readonly logger = new Logger(DatasetApplierService.name);

  constructor(private readonly store: LocalDatasetStore) {}

  async apply(message: SyncMessage): Promise<ApplyResult> {
    const current = await this.store.read(message.datasetType);
    const appliedVersion = current?.version ?? 0;

    // Rule 1: replay / older version — idempotent skip.
    if (message.version <= appliedVersion) {
      this.logger.log(
        `Skip ${message.datasetType} v${message.version} (already at v${appliedVersion})`,
      );
      return {
        outcome: 'skipped',
        version: appliedVersion,
        contentHash: current?.contentHash,
      };
    }

    // Recommended integrity check: reject payloads that don't match the hash.
    const actualHash = computeContentHash(message.payload);
    if (actualHash !== message.contentHash) {
      throw new Error(
        `contentHash mismatch on ${message.datasetType} v${message.version}`,
      );
    }

    let records: unknown;
    if (message.mode === 'SNAPSHOT') {
      // Rule 2: snapshot — replace wholesale.
      records = message.payload;
    } else if (message.previousVersion === appliedVersion) {
      // Rule 3: partial on the exact applied base — merge.
      records = this.mergePartial(current?.records, message);
    } else {
      // Rule 4: partial on any other base — never apply.
      throw new DatasetGapError(
        message.datasetType,
        message.previousVersion,
        appliedVersion,
      );
    }

    // Version + data land in one atomic write (the "same transaction" rule).
    await this.store.write({
      datasetType: message.datasetType,
      version: message.version,
      contentHash: message.contentHash,
      appliedAt: new Date().toISOString(),
      records,
    });
    this.logger.log(
      `Applied ${message.datasetType} v${message.version} (${message.mode})`,
    );
    return {
      outcome: 'applied',
      version: message.version,
      contentHash: message.contentHash,
    };
  }

  private mergePartial(currentRecords: unknown, message: SyncMessage): unknown {
    const partial = message.payload as PartialPayload;

    // Wrapper payloads (e.g. { service, createdBy, paymentTypes: [...] }):
    // merge inside partial.recordsField, leave the other wrapper fields as-is.
    if (partial.recordsField) {
      const wrapper = currentRecords as Record<string, unknown> | null;
      const array = wrapper?.[partial.recordsField];
      if (!wrapper || !Array.isArray(array)) {
        throw new Error(
          `Cannot apply partial for ${message.datasetType}: stored snapshot has no "${partial.recordsField}" array`,
        );
      }
      return {
        ...wrapper,
        [partial.recordsField]: this.mergeRecords(array, partial, message.datasetType),
      };
    }

    if (!Array.isArray(currentRecords)) {
      throw new Error(
        `Cannot apply partial for ${message.datasetType}: no array snapshot to merge into`,
      );
    }
    return this.mergeRecords(
      currentRecords as Record<string, unknown>[],
      partial,
      message.datasetType,
    );
  }

  private mergeRecords(
    current: Record<string, unknown>[],
    partial: PartialPayload,
    datasetType: string,
  ): Record<string, unknown>[] {
    const byKey = new Map<string | number, Record<string, unknown>>();
    for (const record of current) {
      byKey.set(this.keyOf(record, partial, datasetType), record);
    }
    for (const record of partial.upserts ?? []) {
      byKey.set(this.keyOf(record, partial, datasetType), record);
    }
    for (const key of partial.deletes ?? []) {
      byKey.delete(key);
    }
    return [...byKey.values()];
  }

  private keyOf(
    record: Record<string, unknown>,
    partial: PartialPayload,
    datasetType: string,
  ): string | number {
    const key = partial.keyField
      ? record[partial.keyField]
      : (record.id ?? record.code);
    if (key === undefined || key === null) {
      throw new Error(
        `Partial record for ${datasetType} has no "${partial.keyField ?? 'id/code'}" key`,
      );
    }
    return key as string | number;
  }
}
