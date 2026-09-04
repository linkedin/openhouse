package com.linkedin.openhouse.internal.catalog;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConfigurableCommitStatsCollectionGateTest {

  private static final TableIdentifier ID = TableIdentifier.of("db_foo", "tbl");

  private static TableMetadata metadataWith(Map<String, String> extraProps) {
    Map<String, String> props = new HashMap<>();
    props.put("format-version", "2");
    props.putAll(extraProps);
    return TableMetadata.newTableMetadata(
        new Schema(Types.NestedField.required(1, "data", Types.StringType.get())),
        PartitionSpec.unpartitioned(),
        "file:/tmp/db_foo/tbl",
        props);
  }

  @Test
  void blankFilterDisablesDatabaseLevelAndFallsBackToProperty() {
    ConfigurableCommitStatsCollectionGate gate = new ConfigurableCommitStatsCollectionGate("");
    Assertions.assertFalse(gate.isEnabled(ID, metadataWith(new HashMap<>())));
    Assertions.assertFalse(gate.isEnabled(ID, null));
    // Per-table override still works when the DB filter is blank.
    Assertions.assertTrue(
        gate.isEnabled(
            ID,
            metadataWith(
                Map.of(
                    ConfigurableCommitStatsCollectionGate.COMMIT_STATS_COLLECTION_ENABLED_PROP,
                    "true"))));
  }

  @Test
  void nullFilterBehavesLikeBlank() {
    ConfigurableCommitStatsCollectionGate gate = new ConfigurableCommitStatsCollectionGate(null);
    Assertions.assertFalse(gate.isEnabled(ID, metadataWith(new HashMap<>())));
  }

  @Test
  void databaseFilterMatchEnablesWithoutProperty() {
    ConfigurableCommitStatsCollectionGate gate =
        new ConfigurableCommitStatsCollectionGate("(u_openhouse|db_foo)");
    Assertions.assertTrue(gate.isEnabled(ID, metadataWith(new HashMap<>())));
  }

  @Test
  void databaseFilterMissDoesNotEnable() {
    ConfigurableCommitStatsCollectionGate gate =
        new ConfigurableCommitStatsCollectionGate("(u_openhouse|db_bar)");
    Assertions.assertFalse(gate.isEnabled(ID, metadataWith(new HashMap<>())));
  }

  @Test
  void databaseFilterIsFullStringMatch() {
    // "db_fo" must not match database "db_foo" (full-string semantics).
    ConfigurableCommitStatsCollectionGate gate = new ConfigurableCommitStatsCollectionGate("db_fo");
    Assertions.assertFalse(gate.isEnabled(ID, metadataWith(new HashMap<>())));
  }

  @Test
  void wildcardFilterEnablesAllDatabases() {
    ConfigurableCommitStatsCollectionGate gate = new ConfigurableCommitStatsCollectionGate(".*");
    Assertions.assertTrue(gate.isEnabled(ID, metadataWith(new HashMap<>())));
    Assertions.assertTrue(
        gate.isEnabled(TableIdentifier.of("anything", "t"), metadataWith(new HashMap<>())));
  }

  @Test
  void starKeywordEnablesAllDatabases() {
    ConfigurableCommitStatsCollectionGate gate = new ConfigurableCommitStatsCollectionGate("*");
    Assertions.assertTrue(gate.isEnabled(ID, metadataWith(new HashMap<>())));
    Assertions.assertTrue(
        gate.isEnabled(TableIdentifier.of("anything", "t"), metadataWith(new HashMap<>())));
  }

  @Test
  void allKeywordEnablesAllDatabasesCaseInsensitive() {
    for (String value : new String[] {"all", "ALL", "All", " all "}) {
      ConfigurableCommitStatsCollectionGate gate = new ConfigurableCommitStatsCollectionGate(value);
      Assertions.assertTrue(gate.isEnabled(ID, metadataWith(new HashMap<>())), "value=" + value);
    }
  }

  @Test
  void invalidRegexDisablesDatabaseLevelGating() {
    // Unbalanced group is invalid; gate must not throw and must disable DB-level gating.
    ConfigurableCommitStatsCollectionGate gate =
        new ConfigurableCommitStatsCollectionGate("(db_foo");
    Assertions.assertFalse(gate.isEnabled(ID, metadataWith(new HashMap<>())));
  }
}
