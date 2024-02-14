package com.linkedin.openhouse.tables.api.validator;

import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;

public interface IcebergSnapshotsApiValidator {
  /**
   * Validate request body for snapshots PUT.
   *
   * @param clusterId Id of target cluster
   * @param databaseId Id of target database
   * @param tableId Id of target table
   * @param icebergSnapshotsRequestBody Request body for putting Iceberg snapshots
   */
  void validatePutSnapshots(
      String clusterId,
      String databaseId,
      String tableId,
      IcebergSnapshotsRequestBody icebergSnapshotsRequestBody);
}
