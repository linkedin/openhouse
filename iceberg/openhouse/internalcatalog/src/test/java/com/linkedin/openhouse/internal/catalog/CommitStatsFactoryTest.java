package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CommitStatsFactoryTest {

  private static final TableIdentifier ID = TableIdentifier.of("db", "tbl");

  /** Real (snapshot-less) metadata for gate + identity tests. */
  private static TableMetadata metadataWith(Map<String, String> extraProps) {
    Map<String, String> props = new HashMap<>();
    props.put("format-version", "2");
    props.putAll(extraProps);
    return TableMetadata.newTableMetadata(
        new Schema(Types.NestedField.required(1, "data", Types.StringType.get())),
        PartitionSpec.unpartitioned(),
        "file:/tmp/db/tbl",
        props);
  }

  /** Mocked metadata with a controlled current-snapshot summary for mapping tests. */
  private static TableMetadata mockMetadataWithSummary(Map<String, String> summary) {
    Map<String, String> props = new HashMap<>();
    props.put(getCanonicalFieldName("tableUUID"), "uuid-123");
    props.put(getCanonicalFieldName("tableVersion"), "v3");
    Snapshot snapshot = Mockito.mock(Snapshot.class);
    Mockito.when(snapshot.summary()).thenReturn(summary);
    TableMetadata md = Mockito.mock(TableMetadata.class);
    Mockito.when(md.properties()).thenReturn(props);
    Mockito.when(md.location()).thenReturn("file:/tmp/db/tbl");
    Mockito.when(md.currentSnapshot()).thenReturn(snapshot);
    return md;
  }

  @Test
  void extractReturnsEmptyWithoutUuid() {
    Assertions.assertFalse(
        CommitStatsFactory.extract(ID, metadataWith(ImmutableMap.of())).isPresent());
    Assertions.assertFalse(CommitStatsFactory.extract(ID, null).isPresent());
  }

  @Test
  void extractPopulatesIdentityAndPropertiesOnlyWhenNoSnapshot() {
    TableMetadata md =
        metadataWith(ImmutableMap.of(getCanonicalFieldName("tableUUID"), "uuid-123"));
    Optional<CommitStats> stats = CommitStatsFactory.extract(ID, md);
    Assertions.assertTrue(stats.isPresent());
    CommitStats cs = stats.get();
    Assertions.assertEquals("uuid-123", cs.getTableUuid());
    Assertions.assertEquals("db", cs.getDatabaseName());
    Assertions.assertEquals("tbl", cs.getTableName());
    Assertions.assertEquals("file:/tmp/db/tbl", cs.getTableLocation());
    // No current snapshot => snapshot/delta metrics are null (properties-only publish).
    Assertions.assertNull(cs.getNumCurrentFiles());
    Assertions.assertNull(cs.getTableSizeBytes());
    Assertions.assertNull(cs.getNumFilesAdded());
    Assertions.assertNotNull(cs.getTableProperties());
  }

  @Test
  void extractMapsSnapshotSummaryMetrics() {
    Map<String, String> summary =
        ImmutableMap.<String, String>builder()
            .put(SnapshotSummary.TOTAL_DATA_FILES_PROP, "42")
            .put(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "1000")
            .put(SnapshotSummary.ADDED_FILES_PROP, "5")
            .put(SnapshotSummary.DELETED_FILES_PROP, "2")
            .put(SnapshotSummary.ADDED_FILE_SIZE_PROP, "500")
            .put(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "200")
            .build();
    CommitStats cs = CommitStatsFactory.extract(ID, mockMetadataWithSummary(summary)).orElseThrow();
    Assertions.assertEquals(42L, cs.getNumCurrentFiles());
    Assertions.assertEquals(1000L, cs.getTableSizeBytes());
    Assertions.assertEquals(5L, cs.getNumFilesAdded());
    Assertions.assertEquals(2L, cs.getNumFilesDeleted());
    Assertions.assertEquals(500L, cs.getAddedSizeBytes());
    Assertions.assertEquals(200L, cs.getDeletedSizeBytes());
    Assertions.assertEquals("v3", cs.getTableVersion());
  }

  @Test
  void extractToleratesMalformedOrMissingSummaryValues() {
    Map<String, String> summary =
        ImmutableMap.of(SnapshotSummary.TOTAL_DATA_FILES_PROP, "not-a-number");
    CommitStats cs = CommitStatsFactory.extract(ID, mockMetadataWithSummary(summary)).orElseThrow();
    Assertions.assertNull(cs.getNumCurrentFiles());
    Assertions.assertNull(cs.getTableSizeBytes());
  }
}
