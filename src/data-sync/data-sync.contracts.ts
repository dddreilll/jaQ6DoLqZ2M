/**
 * CDH data-sync wire contract — see fusion-cdh-api/docs/store-consumer-rmq-guide.md.
 * Keep in sync with head office (`@contracts/data-sync` in fusion-cdh-api).
 */
import { createHash } from 'crypto';

export const CDH_EXCHANGE = 'cdh.datasync';
export const CDH_DLX = 'cdh.datasync.dlx';
export const DATASYNC_ACK_CLIENT = 'DATASYNC_ACK_CLIENT';

export type DatasetScope = 'GLOBAL' | 'STORE';
export type SyncMode = 'SNAPSHOT' | 'PARTIAL';
export type SyncAckStatus = 'APPLIED' | 'FAILED';

/** The versioned message every store receives. Raw JSON body — no Nest wrapper. */
export interface SyncMessage<TPayload = unknown> {
  datasetType: string;
  scope: DatasetScope;
  scopeId: string | null;
  version: number;
  previousVersion: number | null;
  mode: SyncMode;
  contentHash: string;
  schemaVersion: number;
  issuedAt: string;
  issuedBy: string;
  payload: TPayload;
}

/**
 * PARTIAL payload shape. `recordsField` names the array inside the stored
 * snapshot wrapper to merge into (absent → the snapshot itself is the array);
 * `keyField` names each record's identity field (absent → `id` then `code`).
 */
export interface PartialPayload<TRecord = Record<string, unknown>> {
  recordsField?: string;
  keyField?: string;
  upserts?: TRecord[];
  deletes?: Array<string | number>;
}

/** Sent back to head office after every apply attempt (raw JSON body). */
export interface SyncAck {
  storeCode: string;
  datasetType: string;
  version: number;
  status: SyncAckStatus;
  contentHash?: string;
  error?: string;
}

export function buildAckRoutingKey(
  datasetType: string,
  storeCode: string,
): string {
  return `dataset.ack.${datasetType}.${storeCode}`;
}

/**
 * Deterministic content hash (stable key ordering) — same algorithm as head
 * office, used to verify payload integrity before applying.
 */
export function computeContentHash(payload: unknown): string {
  return createHash('sha256').update(stableStringify(payload)).digest('hex');
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value) ?? 'null';
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }
  const record = value as Record<string, unknown>;
  const entries = Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`);
  return `{${entries.join(',')}}`;
}
