export interface TableMetadata {
  tableId: string;
  databaseId: string;
  currentMetadata: string;
  metadataHistory: MetadataVersion[];
  metadataLocation: string;
  snapshots: string;
  partitions: string | null;
  currentSnapshotId: number;
}

export interface MetadataVersion {
  version: number;
  file: string;
  timestamp: number;
  location: string;
}

export interface Snapshot {
  'snapshot-id': number;
  'timestamp-ms': number;
  operation: string;
  summary: Record<string, string>;
  'parent-snapshot-id'?: number;
}

export interface Partition {
  partition: Record<string, any>;
  record_count: number;
  file_count: number;
  spec_id: number;
}
