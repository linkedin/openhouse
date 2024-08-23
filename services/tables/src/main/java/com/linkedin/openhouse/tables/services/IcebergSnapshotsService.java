package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.model.TableDto;
import org.springframework.data.util.Pair;

/** Service layer for loading Iceberg {@link org.apache.iceberg.Snapshot} provided by client. */
public interface IcebergSnapshotsService {

  /**
   * @return pair of {@link TableDto} object and flag if the table was created.
   */
  Pair<TableDto, Boolean> putIcebergSnapshots(
      String databaseId,
      String tableId,
      IcebergSnapshotsRequestBody icebergSnapshotRequestBody,
      String tableCreator);
}
