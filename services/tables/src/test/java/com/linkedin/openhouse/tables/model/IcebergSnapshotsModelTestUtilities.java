package com.linkedin.openhouse.tables.model;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;
import static com.linkedin.openhouse.common.test.schema.ResourceIoHelper.getSchemaJsonFromResource;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.springframework.test.web.servlet.MvcResult;

/** Utilities methods to help constructing model objects in testing iceberg-snapshot API. */
public final class IcebergSnapshotsModelTestUtilities {
  private IcebergSnapshotsModelTestUtilities() {
    // Utility class noop constructor.
  }

  public static long snapshotIdFromTableLoc(String tableLoc) {
    return obtainSnapshotFromTableLoc(tableLoc).snapshotId();
  }

  public static Snapshot obtainSnapshotFromTableLoc(String tableLoc) {
    return new HadoopTables().load(tableLoc).currentSnapshot();
  }

  public static List<Snapshot> obtainSnapshotsFromTableLoc(String tableLoc) {
    return Lists.newArrayList(new HadoopTables().load(tableLoc).snapshots().iterator());
  }

  public static Map<String, String> obtainSnapshotRefsFromSnapshot(String jsonSnapshot) {
    Map<String, String> snapshotRefs = new HashMap<>();
    Snapshot snapshot = SnapshotParser.fromJson(jsonSnapshot);
    SnapshotRef snapshotRef = SnapshotRef.branchBuilder(snapshot.snapshotId()).build();
    snapshotRefs.put(SnapshotRef.MAIN_BRANCH, SnapshotRefParser.toJson(snapshotRef));
    return snapshotRefs;
  }

  public static DataFile createDummyDataFile(String dataPath, PartitionSpec partitionSpec)
      throws IOException {
    Files.write(Paths.get(dataPath), Lists.newArrayList(), StandardCharsets.UTF_8);
    return DataFiles.builder(partitionSpec)
        .withPath(dataPath)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  public static PartitionSpec getPartitionSpec(GetTableResponseBody getTableResponseBody) {
    return getPartitionSpec(buildTableDto(getTableResponseBody));
  }

  /** Build {@link PartitionSpec} based upon {@link TableDto} object. */
  public static PartitionSpec getPartitionSpec(TableDto tableDto) {
    PartitionSpec.Builder partitionSpec =
        PartitionSpec.builderFor(getSchemaFromSchemaJson(tableDto.getSchema()));
    if (tableDto.getTimePartitioning() == null) {
      return PartitionSpec.unpartitioned();
    } else {
      switch (tableDto.getTimePartitioning().getGranularity()) {
        case DAY:
          partitionSpec.day(tableDto.getTimePartitioning().getColumnName());
          break;
        case HOUR:
          partitionSpec.hour(tableDto.getTimePartitioning().getColumnName());
          break;
        case MONTH:
          partitionSpec.month(tableDto.getTimePartitioning().getColumnName());
          break;
        case YEAR:
          partitionSpec.year(tableDto.getTimePartitioning().getColumnName());
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return partitionSpec.build();
  }

  public static IcebergSnapshotsRequestBody preparePutSnapshotsWithAppendRequest(
      MvcResult mvcResult,
      GetTableResponseBody getTableResponseBody,
      String baseTableVersion,
      Catalog catalog,
      List<DataFile> dataFiles)
      throws Exception {
    return preparePutSnapshotsWithAppendRequest(
        mvcResult, getTableResponseBody, baseTableVersion, catalog, dataFiles, null, false);
  }

  /**
   * A helper method to quickly craft a {@link IcebergSnapshotsRequestBody} from {@link
   * GetTableResponseBody} or {@link MvcResult}, based on whether there's a need to maintain
   * get-modify-put semantic. Additionally we need a instance of {@link Catalog} and a list of
   * {@link DataFile}
   */
  public static IcebergSnapshotsRequestBody preparePutSnapshotsWithAppendRequest(
      MvcResult mvcResult,
      GetTableResponseBody getTableResponseBody,
      String baseTableVersion,
      Catalog catalog,
      List<DataFile> dataFiles,
      FileIO fileIo,
      boolean ignorePreviousMveResult)
      throws Exception {

    CreateUpdateTableRequestBody createUpdateTableRequestBody =
        ignorePreviousMveResult
            ? buildCreateUpdateTableRequestBody(getTableResponseBody)
            : buildCreateUpdateTableRequestBody(mvcResult);
    TableIdentifier tableIdentifier =
        TableIdentifier.of(getTableResponseBody.getDatabaseId(), getTableResponseBody.getTableId());

    List<Snapshot> snapshotsToPut = new ArrayList<>();
    Snapshot appendedSnapshot;
    try {
      Table table = catalog.loadTable(tableIdentifier);
      snapshotsToPut.addAll(Lists.newArrayList(table.snapshots()));
      AppendFiles appendFiles = table.newAppend();
      dataFiles.forEach(appendFiles::appendFile);
      appendedSnapshot = appendFiles.apply();
    } catch (NoSuchTableException e) {
      String snapshotJson =
          getSchemaJsonFromResource(
              IcebergSnapshotsModelTestUtilities.class, "dummy_snapshot_serialized.json");
      appendedSnapshot = SnapshotParser.fromJson(snapshotJson);
    }
    snapshotsToPut.add(appendedSnapshot);
    return IcebergSnapshotsRequestBody.builder()
        .baseTableVersion(baseTableVersion)
        .jsonSnapshots(
            snapshotsToPut.stream().map(SnapshotParser::toJson).collect(Collectors.toList()))
        .snapshotRefs(obtainSnapshotRefsFromSnapshot(SnapshotParser.toJson(appendedSnapshot)))
        .createUpdateTableRequestBody(createUpdateTableRequestBody)
        .build();
  }
}
