package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.openhouse.common.test.schema.ResourceIoHelper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
  public void noneSource_stillStripsInitialDefault() throws ColumnDefaultException {
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT, overwrite(10));
    ReadBridgeStripProtection protection =
        new ReadBridgeStripProtection(
            new ReadBridgeConfigResolver(ColumnDefaultsSource.NONE, ALL_ON));

    TableDto prepared = protection.prepare(ramped(SCHEMA_WITHOUT_DEFAULT), incoming);
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void overwriteWithoutInitialDefault_rejectedWhenRamped() throws ColumnDefaultException {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, overwrite(10));

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_REWRITE"));
    Assertions.assertTrue(thrown.getMessage().contains("country (field-id 2)"));
    Assertions.assertTrue(thrown.getMessage().contains("Spark 3.1"));
  }

  @Test
  public void overwriteWithDummyInitialDefault_rejected() throws ColumnDefaultException {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITH_DUMMY_DEFAULT, overwrite(10));

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_REWRITE"));
    Assertions.assertTrue(thrown.getMessage().contains("matching initial-default"));
    Assertions.assertTrue(thrown.getMessage().contains("country (field-id 2)"));
  }

  @Test
  public void overwriteWithMatchingInitialDefault_stripsBeforeReturning()
      throws ColumnDefaultException {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT, overwrite(10));

    TableDto prepared = protection.prepare(existing, incoming);
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
    Assertions.assertTrue(prepared.getSchema().contains("\"id\":2"));
  }

  @Test
  public void appendWithoutInitialDefault_allowedWhenRamped() throws ColumnDefaultException {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, append(10));

    Assertions.assertSame(incoming, protection.prepare(existing, incoming));
  }

  @Test
  public void historicalOverwriteDoesNotGateCurrentAppend() throws ColumnDefaultException {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming =
        ramped(SCHEMA_WITHOUT_DEFAULT, snapshots(overwriteJson(1), appendJson(10)), refs(10));

    Assertions.assertSame(incoming, protection.prepare(existing, incoming));
  }

  @Test
  public void replaceCommitWithoutInitialDefault_rejectedWhenRamped()
      throws ColumnDefaultException {
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
        ColumnDefaultException.class, () -> protection.prepare(existing, incoming));
  }

  @Test
  public void unrampedOverwriteWithoutInitialDefault_allowed() throws ColumnDefaultException {
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
  public void optOutSchemaOnly_doesNotType1() throws ColumnDefaultException {
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
  public void optOutOverwriteWithoutOverlay_stillType2() throws ColumnDefaultException {
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
        ColumnDefaultException.class, () -> protection.prepare(existing, incoming));
  }

  @Test
  public void createWithOverlay_stripsStampedIds() throws ColumnDefaultException {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT);

    TableDto prepared = protection.prepare(null, incoming);
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void unstampedWriterDefault_stripped() throws ColumnDefaultException {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto prepared = protection.prepare(null, ramped(SCHEMA_UNSTAMPED_WRITER_DEFAULTS));

    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void unrampedWithInitialDefault_stillStrips() throws ColumnDefaultException {
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
  public void nestedStampedDefault_stripped() throws ColumnDefaultException {
    ColumnDefaultsSource nested = tableDto -> Collections.singletonMap(10, TextNode.valueOf("US"));
    ReadBridgeStripProtection protection = protection(nested);
    TableDto prepared = protection.prepare(null, ramped(NESTED_WITH_DEFAULT));

    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
    Assertions.assertTrue(prepared.getSchema().contains("\"id\":10"));
  }

  @Test
  public void droppingLiveDefault_rejected() throws ColumnDefaultException {
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

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_REMOVED"));
    Assertions.assertEquals(ColumnDefaultException.Operation.REMOVED, thrown.getOperation());
    Assertions.assertTrue(thrown.getMessage().contains("country (field-id 2)"));
    Assertions.assertTrue(thrown.getMessage().contains("cannot be removed or changed"));
  }

  @Test
  public void droppingColumnThatHadDefault_allowed() throws ColumnDefaultException {
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
  public void schemaOnlyUpdate_doesNotGate() throws ColumnDefaultException {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT);

    Assertions.assertSame(incoming, protection.prepare(existing, incoming));
  }

  @Test
  public void sourceThrow_failsClosed() throws ColumnDefaultException {
    ColumnDefaultsSource exploding =
        tableDto -> {
          throw new IllegalStateException("encoder exploded");
        };
    ReadBridgeStripProtection protection = protection(exploding);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, overwrite(10));

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_UNUSABLE"));
    Assertions.assertTrue(thrown.getMessage().contains("encoder exploded"));
    Assertions.assertTrue(thrown.getMessage().contains(METADATA_LOCATION));
  }

  @Test
  public void unreadableSchema_failsClosedWhenRampedRewrite() throws ColumnDefaultException {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped("{", overwrite(10));

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> protection.prepare(existing, incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_UNUSABLE"));
    Assertions.assertTrue(thrown.getMessage().contains("unreadable json"));
    Assertions.assertTrue(thrown.getMessage().contains(METADATA_LOCATION));
  }

  @Test
  public void unreadableSnapshots_failsClosedWhenRamped() throws ColumnDefaultException {
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

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> protection.prepare(existing, incoming));
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

  private static ReadBridgeStripProtection protection(ColumnDefaultsSource source) {
    return new ReadBridgeStripProtection(new ReadBridgeConfigResolver(source, ALL_ON));
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
