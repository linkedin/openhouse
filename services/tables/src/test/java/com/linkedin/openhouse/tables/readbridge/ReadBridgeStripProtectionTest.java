package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.common.test.schema.ResourceIoHelper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.SnapshotRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReadBridgeStripProtectionTest {

  private static final String SCHEMA_WITH_DEFAULT = schema("schema_with_default.json");
  private static final String SCHEMA_WITH_DUMMY_DEFAULT = schema("schema_with_dummy_default.json");
  private static final String SCHEMA_WITHOUT_DEFAULT = schema("schema_without_default.json");
  private static final String SCHEMA_WITHOUT_COUNTRY = schema("schema_without_country.json");
  private static final String NESTED_WITH_DEFAULT = schema("schema_nested_with_default.json");
  private static final String SCHEMA_UNSTAMPED_WRITER_DEFAULTS =
      schema("schema_unstamped_writer_defaults.json");

  private static final String ENABLED_PROP =
      ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID
          + TableFeatureToggle.ENABLED_PROPERTY_SUFFIX;
  private static final String METADATA_LOCATION =
      "file:/data/openhouse/db/tbl-uuid/00001-x.metadata.json";

  private static final TableFeatureToggle ALL_ON =
      new TableFeatureToggle() {
        @Override
        public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
          return true;
        }
      };

  private static final ColumnDefaultsSource FIELD_2 =
      tableDto -> Collections.singletonMap(2, TextNode.valueOf("US"));

  @Test
  public void noneSource_stillStripsInitialDefault() {
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT, overwrite(10));
    ReadBridgeStripProtection protection = protection(ColumnDefaultsSource.NONE);

    TableDto prepared = protection.prepare(ramped(SCHEMA_WITHOUT_DEFAULT), incoming);
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void overwriteWithoutInitialDefault_rejectedWhenRamped() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, overwrite(10));

    UnsupportedClientOperationException thrown =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_REWRITE"));
    Assertions.assertTrue(thrown.getMessage().contains("country (field-id 2)"));
    Assertions.assertTrue(thrown.getMessage().contains("Spark 3.1"));
  }

  @Test
  public void overwriteWithDummyInitialDefault_rejected() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITH_DUMMY_DEFAULT, overwrite(10));

    UnsupportedClientOperationException thrown =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_REWRITE"));
    Assertions.assertTrue(thrown.getMessage().contains("matching initial-default"));
    Assertions.assertTrue(thrown.getMessage().contains("country (field-id 2)"));
  }

  @Test
  public void overwriteWithMatchingInitialDefault_stripsBeforeReturning() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT, overwrite(10));

    TableDto prepared = protection.prepare(existing, incoming);
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
    Assertions.assertTrue(prepared.getSchema().contains("\"id\":2"));
  }

  @Test
  public void appendWithoutInitialDefault_allowedWhenRamped() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, append(10));

    Assertions.assertSame(incoming, protection.prepare(existing, incoming));
  }

  @Test
  public void historicalOverwriteDoesNotGateCurrentAppend() {
    ReadBridgeStripProtection protection = protection(FIELD_2, 1L);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming =
        ramped(SCHEMA_WITHOUT_DEFAULT, snapshots(overwriteJson(1), appendJson(10)), refs(10));

    Assertions.assertSame(incoming, protection.prepare(existing, incoming));
  }

  @Test
  public void branchOverwriteAddedAlongsideMainAppend_rejected() {
    ReadBridgeStripProtection protection = protection(FIELD_2, 1L);
    Map<String, String> refs = refs(11);
    refs.put("feature", ref(20, "branch"));
    TableDto incoming =
        ramped(
            SCHEMA_WITHOUT_DEFAULT,
            snapshots(appendJson(1), appendJson(11), overwriteJson(20)),
            refs);

    UnsupportedClientOperationException thrown =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> protection.prepare(ramped(SCHEMA_WITHOUT_DEFAULT), incoming));
    Assertions.assertTrue(thrown.getMessage().startsWith("COLUMN_DEFAULT_REWRITE"));
  }

  @Test
  public void stagedWapOverwriteWithoutRef_rejected() {
    ReadBridgeStripProtection protection = protection(FIELD_2, 1L);
    TableDto incoming =
        ramped(SCHEMA_WITHOUT_DEFAULT, snapshots(appendJson(1), overwriteJson(20)), refs(1));

    UnsupportedClientOperationException thrown =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> protection.prepare(ramped(SCHEMA_WITHOUT_DEFAULT), incoming));
    Assertions.assertTrue(thrown.getMessage().startsWith("COLUMN_DEFAULT_REWRITE"));
  }

  @Test
  public void branchAndTagAtPersistedOverwriteHead_notRewrite() {
    ReadBridgeStripProtection protection = protection(FIELD_2, 1L);
    Map<String, String> refs = refs(1);
    refs.put("feature", ref(1, "branch"));
    refs.put("release", ref(1, "tag"));
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, snapshots(overwriteJson(1)), refs);

    Assertions.assertSame(incoming, protection.prepare(ramped(SCHEMA_WITHOUT_DEFAULT), incoming));
  }

  /** A failed catalog read fails the write closed, as itself: the request is not at fault. */
  @Test
  public void unreadablePersistedSnapshots_failsClosedAsItself() {
    IllegalStateException unavailable = new IllegalStateException("catalog unavailable");
    ReadBridgeStripProtection protection =
        new ReadBridgeStripProtection(
            new ReadBridgeConfigResolver(FIELD_2, ALL_ON),
            key -> {
              throw unavailable;
            });
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, overwrite(10));

    Assertions.assertSame(
        unavailable,
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> protection.prepare(ramped(SCHEMA_WITHOUT_DEFAULT), incoming)));
  }

  @Test
  public void replaceCommitWithoutInitialDefault_rejectedWhenRamped() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(optIn())
            .replaceCommit(true)
            .build();

    Assertions.assertThrows(
        UnsupportedClientOperationException.class, () -> protection.prepare(existing, incoming));
  }

  @Test
  public void unrampedOverwriteWithoutInitialDefault_allowed() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    Map<String, String> optedOut = Collections.singletonMap(ENABLED_PROP, "false");
    TableDto existing =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(optedOut)
            .build();
    TableDto incoming =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(optedOut)
            .jsonSnapshots(Collections.singletonList(overwriteJson(10)))
            .snapshotRefs(refs(10))
            .build();

    Assertions.assertSame(incoming, protection.prepare(existing, incoming));
  }

  @Test
  public void optOutSchemaOnly_doesNotType1() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(Collections.singletonMap(ENABLED_PROP, "false"))
            .build();

    Assertions.assertSame(incoming, protection.prepare(existing, incoming));
  }

  @Test
  public void optOutOverwriteWithoutOverlay_stillType2() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(Collections.singletonMap(ENABLED_PROP, "false"))
            .jsonSnapshots(Collections.singletonList(overwriteJson(10)))
            .snapshotRefs(refs(10))
            .build();

    Assertions.assertThrows(
        UnsupportedClientOperationException.class, () -> protection.prepare(existing, incoming));
  }

  @Test
  public void createWithOverlay_stripsStampedIds() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT);

    TableDto prepared = protection.prepare(null, incoming);
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void unstampedWriterDefault_stripped() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto prepared = protection.prepare(null, ramped(SCHEMA_UNSTAMPED_WRITER_DEFAULTS));

    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void unrampedWithInitialDefault_stillStrips() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    Map<String, String> optedOut = Collections.singletonMap(ENABLED_PROP, "false");
    TableDto existing =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(optedOut)
            .build();
    TableDto incoming =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITH_DEFAULT)
            .tableProperties(optedOut)
            .build();

    TableDto prepared = protection.prepare(existing, incoming);
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void nestedStampedDefault_stripped() {
    ColumnDefaultsSource nested = tableDto -> Collections.singletonMap(10, TextNode.valueOf("US"));
    ReadBridgeStripProtection protection = protection(nested);
    TableDto prepared = protection.prepare(null, ramped(NESTED_WITH_DEFAULT));

    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
    Assertions.assertTrue(prepared.getSchema().contains("\"id\":10"));
  }

  @Test
  public void droppingLiveDefault_rejected() {
    ColumnDefaultsSource fromProp =
        tableDto -> {
          String raw =
              tableDto.getTableProperties() == null
                  ? null
                  : tableDto.getTableProperties().get("default-field");
          if (raw == null) {
            return Collections.emptyMap();
          }
          return Collections.singletonMap(Integer.parseInt(raw), TextNode.valueOf("US"));
        };
    ReadBridgeStripProtection protection = protection(fromProp);
    Map<String, String> previousProps = new HashMap<>();
    previousProps.put(ENABLED_PROP, "true");
    previousProps.put("default-field", "2");
    TableDto existing =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(previousProps)
            .build();
    TableDto incoming =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(optIn())
            .build();

    UnsupportedClientOperationException thrown =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_REMOVED"));
    Assertions.assertTrue(thrown.getMessage().contains("country (field-id 2)"));
    Assertions.assertTrue(thrown.getMessage().contains("cannot be removed or changed"));
  }

  @Test
  public void droppingColumnThatHadDefault_allowed() {
    ColumnDefaultsSource fromProp =
        tableDto -> {
          String raw =
              tableDto.getTableProperties() == null
                  ? null
                  : tableDto.getTableProperties().get("default-field");
          if (raw == null) {
            return Collections.emptyMap();
          }
          return Collections.singletonMap(Integer.parseInt(raw), TextNode.valueOf("US"));
        };
    ReadBridgeStripProtection protection = protection(fromProp);
    Map<String, String> previousProps = new HashMap<>();
    previousProps.put(ENABLED_PROP, "true");
    previousProps.put("default-field", "2");
    TableDto existing =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(previousProps)
            .build();
    TableDto incoming =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_COUNTRY)
            .tableProperties(optIn())
            .build();

    Assertions.assertSame(incoming, protection.prepare(existing, incoming));
  }

  @Test
  public void schemaOnlyUpdate_doesNotGate() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT);

    Assertions.assertSame(incoming, protection.prepare(existing, incoming));
  }

  /** A buggy source fails the write closed, as itself rather than as anyone's unusable default. */
  @Test
  public void sourceBug_failsClosedAsItself() {
    IllegalStateException bug = new IllegalStateException("encoder exploded");
    ReadBridgeStripProtection protection =
        protection(
            tableDto -> {
              throw bug;
            });
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, overwrite(10));

    Assertions.assertSame(
        bug,
        Assertions.assertThrows(
            IllegalStateException.class, () -> protection.prepare(existing, incoming)));
  }

  /**
   * A default the stored table declares but cannot apply is the server's fault, not the write's.
   */
  @Test
  public void storedUnusableDefault_failsAsServerError() {
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, append(10));
    ColumnDefaultException reported = unusableDefault();
    ReadBridgeStripProtection protection = protection(rejecting(existing, reported));

    TableConfigUnavailableException thrown =
        Assertions.assertThrows(
            TableConfigUnavailableException.class, () -> protection.prepare(existing, incoming));
    Assertions.assertSame(reported, thrown.getCause());
  }

  /** A default the write declares but cannot apply is the request's fault. */
  @Test
  public void incomingUnusableDefault_failsAsRequestError() {
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, append(10));
    ColumnDefaultException reported = unusableDefault();
    ReadBridgeStripProtection protection = protection(rejecting(incoming, reported));

    UnsupportedClientOperationException thrown =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("column country"));
    Assertions.assertSame(reported, thrown.getCause());
  }

  @Test
  public void unreadableSchema_failsClosedWhenRampedRewrite() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped("{", overwrite(10));

    UnsupportedClientOperationException thrown =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_UNUSABLE"));
    Assertions.assertTrue(thrown.getMessage().contains("unreadable json"));
    Assertions.assertTrue(thrown.getMessage().contains(METADATA_LOCATION));
  }

  @Test
  public void unreadableSnapshots_failsClosedWhenRamped() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .tableLocation(METADATA_LOCATION)
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(optIn())
            .jsonSnapshots(Collections.singletonList("not-a-snapshot"))
            .build();

    UnsupportedClientOperationException thrown =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_UNUSABLE"));
    Assertions.assertTrue(thrown.getMessage().contains("unreadable snapshot"));
    Assertions.assertTrue(thrown.getMessage().contains(METADATA_LOCATION));
  }

  private static String schema(String resourceName) {
    try {
      return ResourceIoHelper.getSchemaJsonFromResource(
          ReadBridgeStripProtectionTest.class, "readbridge/" + resourceName);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** {@code persisted}: snapshot ids already committed to the table, i.e. history. */
  private static ReadBridgeStripProtection protection(
      ColumnDefaultsSource source, Long... persisted) {
    Set<Long> persistedIds = new HashSet<>(Arrays.asList(persisted));
    return new ReadBridgeStripProtection(
        new ReadBridgeConfigResolver(source, ALL_ON), key -> persistedIds);
  }

  /** Rejects only {@code declaring}; every other table has a usable default on field 2. */
  private static ColumnDefaultsSource rejecting(
      TableDto declaring, ColumnDefaultException reported) {
    return tableDto -> {
      if (tableDto == declaring) {
        throw reported;
      }
      return Collections.singletonMap(2, TextNode.valueOf("US"));
    };
  }

  private static ColumnDefaultException unusableDefault() {
    return new ColumnDefaultException("column country: default is not a string");
  }

  private static TableDto ramped(String schema) {
    return TableDto.builder()
        .databaseId("db")
        .tableId("tbl")
        .tableLocation(METADATA_LOCATION)
        .schema(schema)
        .tableProperties(optIn())
        .build();
  }

  private static TableDto ramped(String schema, String jsonSnapshot) {
    return ramped(schema, Collections.singletonList(jsonSnapshot), refsFrom(jsonSnapshot));
  }

  private static TableDto ramped(
      String schema, java.util.List<String> jsonSnapshots, Map<String, String> refs) {
    return TableDto.builder()
        .databaseId("db")
        .tableId("tbl")
        .tableLocation(METADATA_LOCATION)
        .schema(schema)
        .tableProperties(optIn())
        .jsonSnapshots(jsonSnapshots)
        .snapshotRefs(refs)
        .build();
  }

  private static Map<String, String> optIn() {
    return Collections.singletonMap(ENABLED_PROP, "true");
  }

  private static String append(long snapshotId) {
    return snapshotJson(snapshotId, "append");
  }

  private static String overwrite(long snapshotId) {
    return snapshotJson(snapshotId, "overwrite");
  }

  private static String appendJson(long snapshotId) {
    return snapshotJson(snapshotId, "append");
  }

  private static String overwriteJson(long snapshotId) {
    return snapshotJson(snapshotId, "overwrite");
  }

  private static java.util.List<String> snapshots(String... json) {
    return Arrays.asList(json);
  }

  private static Map<String, String> refs(long snapshotId) {
    Map<String, String> refs = new HashMap<>();
    refs.put(SnapshotRef.MAIN_BRANCH, "{\"snapshot-id\":" + snapshotId + ",\"type\":\"branch\"}");
    return refs;
  }

  private static String ref(long snapshotId, String type) {
    return "{\"snapshot-id\":" + snapshotId + ",\"type\":\"" + type + "\"}";
  }

  private static Map<String, String> refsFrom(String jsonSnapshot) {
    if (jsonSnapshot.contains("\"snapshot-id\":10")
        || jsonSnapshot.contains("\"snapshot-id\" : 10")) {
      return refs(10);
    }
    return refs(1);
  }

  private static String snapshotJson(long snapshotId, String operation) {
    return "{\"snapshot-id\":"
        + snapshotId
        + ",\"timestamp-ms\":1,\"summary\":{\"operation\":\""
        + operation
        + "\"},\"manifest-list\":\"file:/tmp/m.avro\",\"schema-id\":0}";
  }
}
