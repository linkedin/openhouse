export interface Table {
  tableId: string;
  databaseId: string;
  clusterId?: string;
  tableUri?: string;
  tableUUID?: string;
  tableLocation?: string;
  tableVersion?: string;
  tableCreator?: string;
  schema?: string;
  lastModifiedTime?: number;
  creationTime?: number;
  tableProperties?: Record<string, string>;
  timePartitioning?: any;
  clustering?: any;
  policies?: {
    retention?: any;
    sharingEnabled?: boolean;
    columnTags?: any;
    replication?: any;
    history?: any;
    lockState?: any;
  };
  tableType?: string;
}
