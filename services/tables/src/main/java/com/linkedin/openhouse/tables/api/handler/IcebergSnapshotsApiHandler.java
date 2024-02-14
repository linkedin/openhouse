package com.linkedin.openhouse.tables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;

/**
 * Interface layer between REST and Iceberg Snapshots backend. The implementation is injected into
 * the Service Controller.
 */
public interface IcebergSnapshotsApiHandler {

  ApiResponse<GetTableResponseBody> putIcebergSnapshots(
      String databaseId,
      String tableId,
      IcebergSnapshotsRequestBody icebergSnapshotRequestBody,
      String tableCreator);
}
