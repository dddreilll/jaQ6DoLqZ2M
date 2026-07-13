import { Injectable } from '@nestjs/common';
import { mkdir, readFile, rename, writeFile } from 'fs/promises';
import { dirname, join } from 'path';

export interface StoredDataset {
  datasetType: string;
  version: number;
  contentHash: string;
  appliedAt: string;
  records: unknown;
}

/**
 * Local persistence for applied datasets. One JSON file per dataset type
 * holding BOTH the applied version and the data — the atomic tmp+rename write
 * is this sample's equivalent of the guide's "update the stored version and
 * the dataset in the same local transaction". A real store app would replace
 * this with its own database (and a real transaction).
 */
@Injectable()
export class LocalDatasetStore {
  private readonly dataDir = process.env.DATA_DIR ?? './data';

  async read(datasetType: string): Promise<StoredDataset | null> {
    try {
      return JSON.parse(await readFile(this.fileFor(datasetType), 'utf8'));
    } catch {
      return null;
    }
  }

  async write(dataset: StoredDataset): Promise<void> {
    const file = this.fileFor(dataset.datasetType);
    await mkdir(dirname(file), { recursive: true });
    const tmp = `${file}.tmp`;
    await writeFile(tmp, JSON.stringify(dataset, null, 2));
    await rename(tmp, file);
  }

  private fileFor(datasetType: string): string {
    return join(
      this.dataDir,
      `${datasetType.replace(/[^A-Za-z0-9_-]/g, '_')}.json`,
    );
  }
}
