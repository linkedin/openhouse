package com.linkedin.openhouse.hts.catalog.data;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.util.Pair;

/**
 * Internal Helper class to read/ write {@link IcebergRow}s to an iceberg {@link Table}
 *
 * <p>- Implements functionality to get/put/delete/search {@link IcebergRow}s in iceberg table. -
 * Ensures rows in the table are uniquely identified by {@link IcebergRowPrimaryKey}. - utilizes
 * {@link IcebergDataUtils} to create appropriate DataFiles and DeleteFile - Interfaces with
 * Iceberg's {@link Table} api and performs {@link org.apache.iceberg.RowDelta} operations.
 *
 * <p>Assumptions: - Table provided to the get/put/delete/search api has at least one snapshot and
 * is a V2 table.
 */
@Slf4j
public class GenericIcebergRowReadersWriters<
    IR extends IcebergRow, IRPK extends IcebergRowPrimaryKey> {

  public IR put(Table table, IR icebergRow) {
    Snapshot snapshot = table.currentSnapshot();
    Optional<IR> existingIcebergRow =
        get(table, snapshot, (IRPK) icebergRow.getIcebergRowPrimaryKey());
    if (existingIcebergRow.isPresent()
        && !existingIcebergRow
            .get()
            .getRecord()
            .getField(existingIcebergRow.get().getVersionColumnName())
            .equals(icebergRow.getCurrentVersion())) {
      throw new CommitFailedException(
          String.format(
              "Metadata has changed, can you try again? no longer at version : %s, instead the current "
                  + "version is : %s",
              icebergRow.getCurrentVersion(), existingIcebergRow.get().getCurrentVersion()));
    }

    Pair<DataFile, IcebergRow> dataFileAndRow =
        IcebergDataUtils.createRowDeltaDataFileWithNextVersion(table, icebergRow);
    DeleteFile deleteFile =
        IcebergDataUtils.createRowDeltaDeleteFile(table, icebergRow.getIcebergRowPrimaryKey());

    table
        .newRowDelta()
        .addDeletes(deleteFile)
        .addRows(dataFileAndRow.first())
        .conflictDetectionFilter(icebergRow.getIcebergRowPrimaryKey().getSearchExpression())
        .validateFromSnapshot(snapshot.snapshotId())
        .validateNoConflictingDeleteFiles()
        .validateNoConflictingDataFiles()
        .commit();

    return (IR) dataFileAndRow.second();
  }

  private Optional<IR> get(Table table, Snapshot snapshot, IRPK primaryKey) {
    List<Record> records =
        Lists.newArrayList(
            IcebergGenerics.read(table)
                .useSnapshot(snapshot.snapshotId())
                .where(primaryKey.getSearchExpression())
                .build()
                .iterator());
    if (records.size() > 1) {
      throw new RuntimeException(
          String.format(
              "Corrupt metadata in house table %s, more than 1 entry for primary key: %s",
              table.name(), primaryKey.getRecord().toString()));
    } else if (records.size() == 1) {
      return Optional.of((IR) primaryKey.buildIcebergRow(records.get(0)));
    }
    log.debug(
        "Didn't find requested entity with primary key: {}", primaryKey.getRecord().toString());
    return Optional.empty();
  }

  public Optional<IR> get(Table table, IRPK primaryKey) {
    return get(table, table.currentSnapshot(), primaryKey);
  }

  public void delete(Table table, IRPK primaryKey) {
    Snapshot snapshot = table.currentSnapshot();
    Optional<IR> icebergRow = get(table, snapshot, primaryKey);

    if (!icebergRow.isPresent()) {
      throw new NotFoundException(
          String.format(
              "Row does not exist in house table %s, couldn't delete row identified by primary key: %s",
              table.name(), primaryKey.getRecord().toString()));
    }
    DeleteFile deleteFile = IcebergDataUtils.createRowDeltaDeleteFile(table, primaryKey);
    table
        .newRowDelta()
        .addDeletes(deleteFile)
        .conflictDetectionFilter(primaryKey.getSearchExpression())
        .validateFromSnapshot(snapshot.snapshotId())
        .validateNoConflictingDeleteFiles()
        .validateNoConflictingDataFiles()
        .commit();
  }

  public Iterable<IR> searchByPartialId(Table table, IRPK icebergRowPartialKey) {
    List<Record> records =
        Lists.newArrayList(
            IcebergGenerics.read(table)
                .where(IcebergDataUtils.createPartialSearchExpression(icebergRowPartialKey))
                .build()
                .iterator());
    return records.stream()
        .map(r -> (IR) icebergRowPartialKey.buildIcebergRow(r))
        .collect(Collectors.toList());
  }
}
