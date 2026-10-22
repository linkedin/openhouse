package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.common.test.schema.ResourceIoHelper;
import com.linkedin.openhouse.internal.catalog.PreCommitContext;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.model.TableDto;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReadBridgeStripProtectionTest {

  private static final String SCHEMA_WITH_DEFAULT = schema("schema_with_default.json");
  private static final String SCHEMA_WITHOUT_DEFAULT = schema("schema_without_default.json");
  private static final String SCHEMA_WITHOUT_COUNTRY = schema("schema_without_country.json");
  private static final String NESTED_WITH_DEFAULT = schema("schema_nested_with_default.json");
  private static final String SCHEMA_UNSTAMPED_WRITER_DEFAULTS =
      schema("schema_unstamped_writer_defaults.json");

  private static final String ENABLED_PROP =
      ReadBridgeConfigResolver.COLUMN_DEFAULTS_CAPABILITY_PROP;
  private static final String METADATA_LOCATION =
      "file:/data/openhouse/db/tbl-uuid/00001-x.metadata.json";
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");
  private static final String REMEDY =
      "Update the client, or use a supported client: spark with openhouse-java-client 0.5.100 or"
          + " newer.";

  private static final ColumnDefaultsSource FIELD_2 =
      tableDto -> Collections.singletonMap(2, TextNode.valueOf("US"));

  @Test
  public void noDefaultsSource_stillStripsInitialDefault() {
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT, overwrite(10));
    ReadBridgeStripProtection protection = protection(tableDto -> Collections.emptyMap());

    TableDto prepared =
        Assertions.assertDoesNotThrow(
            () -> commit(protection, ramped(SCHEMA_WITHOUT_DEFAULT), incoming, true));
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void overwriteWithoutInitialDefault_rejectedWhenRamped() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, overwrite(10));

    ColumnDefaultUnawareCommitException thrown =
        Assertions.assertThrows(
            ColumnDefaultUnawareCommitException.class,
            () -> commit(protection, existing, incoming, false));
    Assertions.assertEquals(
        "CLIENT_CAPABILITY_MISSING: db.tbl requires the column-defaults capability, which this"
            + " client does not have (X-Client-Name: spark, User-Agent:"
            + " openhouse-java-client/0.5.99). "
            + REMEDY,
        thrown.getMessage());
  }

  /** The client decides Type 2, not the {@code initial-default} it sent. */
  @Test
  public void overwriteWithMatchingInitialDefault_rejectedFromUnawareClient() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT, overwrite(10));

    ColumnDefaultUnawareCommitException thrown =
        Assertions.assertThrows(
            ColumnDefaultUnawareCommitException.class,
            () -> commit(protection, existing, incoming, false));
    Assertions.assertTrue(thrown.getMessage().startsWith("CLIENT_CAPABILITY_MISSING: db.tbl "));
  }

  @Test
  public void overwriteWithMatchingInitialDefault_stripsBeforeReturning() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT, overwrite(10));

    TableDto prepared =
        Assertions.assertDoesNotThrow(() -> commit(protection, existing, incoming, true));
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
    Assertions.assertTrue(prepared.getSchema().contains("\"id\":2"));
  }

  @Test
  public void appendWithoutInitialDefault_allowedFromDefaultAwareClient() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, append(10));

    Assertions.assertDoesNotThrow(() -> commit(protection, existing, incoming, true));
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
            .build();

    Assertions.assertDoesNotThrow(() -> commit(protection, existing, incoming, false));
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

    Assertions.assertDoesNotThrow(() -> commit(protection, existing, incoming, false));
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
            .build();

    Assertions.assertThrows(
        ColumnDefaultUnawareCommitException.class,
        () -> commit(protection, existing, incoming, false));
  }

  @Test
  public void createWithOverlay_stripsStampedIds() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT);

    TableDto prepared =
        Assertions.assertDoesNotThrow(() -> commit(protection, null, incoming, true));
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void unstampedWriterDefault_stripped() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto prepared =
        Assertions.assertDoesNotThrow(
            () -> commit(protection, null, ramped(SCHEMA_UNSTAMPED_WRITER_DEFAULTS), true));

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

    TableDto prepared =
        Assertions.assertDoesNotThrow(() -> commit(protection, existing, incoming, false));
    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void nestedStampedDefault_stripped() {
    ColumnDefaultsSource nested = tableDto -> Collections.singletonMap(10, TextNode.valueOf("US"));
    ReadBridgeStripProtection protection = protection(nested);
    TableDto prepared =
        Assertions.assertDoesNotThrow(
            () -> commit(protection, null, ramped(NESTED_WITH_DEFAULT), true));

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

    ColumnDefaultRemovedException thrown =
        Assertions.assertThrows(
            ColumnDefaultRemovedException.class,
            () -> commit(protection, existing, incoming, true));
    Assertions.assertEquals(
        "COLUMN_DEFAULT_REMOVED: db.tbl still has a column default on country (field-id 2), and"
            + " this commit omitted it. "
            + REMEDY,
        thrown.getMessage());
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

    Assertions.assertDoesNotThrow(() -> commit(protection, existing, incoming, true));
  }

  @Test
  public void schemaOnlyUpdate_doesNotGate() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT);

    Assertions.assertDoesNotThrow(() -> commit(protection, existing, incoming, false));
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
            IllegalStateException.class, () -> commit(protection, existing, incoming, false)));
  }

  /**
   * A default the stored table declares but cannot apply is the server's fault, not the write's.
   */
  @Test
  public void storedUnusableDefault_failsAsServerError() {
    TableDto existing = unusable(ramped(SCHEMA_WITHOUT_DEFAULT));
    TableDto incoming = ramped(SCHEMA_WITHOUT_DEFAULT, append(10));
    ColumnDefaultException reported = unusableDefault();
    ReadBridgeStripProtection protection = protection(rejecting(reported));

    TableConfigUnavailableException thrown =
        Assertions.assertThrows(
            TableConfigUnavailableException.class,
            () -> commit(protection, existing, incoming, true));
    Assertions.assertSame(reported, thrown.getCause());
  }

  /**
   * A default the write declares but cannot apply rejects the write, carrying the source's
   * exception as the cause.
   */
  @Test
  public void incomingUnusableDefault_rejectedWithTheSourceException() {
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = unusable(ramped(SCHEMA_WITHOUT_DEFAULT, append(10)));
    ColumnDefaultException reported = unusableDefault();
    ReadBridgeStripProtection protection = protection(rejecting(reported));

    Assertions.assertSame(
        reported,
        Assertions.assertThrows(
                ColumnDefaultUnusableException.class,
                () -> commit(protection, existing, incoming, true))
            .getCause());
  }

  @Test
  public void unreadableSchema_failsClosed() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped("{", overwrite(10));

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> commit(protection, existing, incoming, false));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_UNUSABLE"));
    Assertions.assertTrue(thrown.getMessage().contains("unreadable json"));
    Assertions.assertTrue(thrown.getMessage().contains(METADATA_LOCATION));
  }

  @Test
  public void withoutInitialDefault_leavesTheSchemaUnchanged() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto prepared =
        Assertions.assertDoesNotThrow(
            () -> commit(protection, null, ramped(SCHEMA_WITHOUT_DEFAULT), true));

    Assertions.assertEquals(SCHEMA_WITHOUT_DEFAULT, prepared.getSchema());
  }

  /**
   * A missing intermediate schema is rejected rather than passed through: the commit would
   * otherwise skip it and persist an incomplete schema history.
   */
  @Test
  public void missingIntermediateSchema_failsClosed() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto incoming =
        ramped(SCHEMA_WITH_DEFAULT)
            .toBuilder()
            .newIntermediateSchemas(Arrays.asList(SCHEMA_WITH_DEFAULT, ""))
            .build();

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> commit(protection, null, incoming, true));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_UNUSABLE"));
    Assertions.assertTrue(thrown.getMessage().contains("missing schema"));
  }

  /** A create has no stored defaults to protect, but its declared defaults must still apply. */
  @Test
  public void createDeclaringUnusableDefault_rejected() {
    TableDto incoming = unusable(ramped(SCHEMA_WITHOUT_DEFAULT, append(10)));
    ColumnDefaultException reported = unusableDefault();
    ReadBridgeStripProtection protection = protection(rejecting(reported));

    Assertions.assertSame(
        reported,
        Assertions.assertThrows(
                ColumnDefaultUnusableException.class,
                () -> commit(protection, null, incoming, true))
            .getCause());
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
    return new ReadBridgeStripProtection(new ReadBridgeConfigResolver(source));
  }

  /**
   * Prepares {@code incoming} and checks the commit it makes on {@code existing}, from a Spark
   * client the gate admits ({@code 0.5.100}) or one release too old. A request that carries
   * snapshots commits snapshots.
   */
  private static TableDto commit(
      ReadBridgeStripProtection protection,
      TableDto existing,
      TableDto incoming,
      boolean defaultAwareClient)
      throws ColumnDefaultException {
    TableDto prepared = protection.prepare(incoming);
    String userAgent =
        defaultAwareClient ? "openhouse-java-client/0.5.100" : "openhouse-java-client/0.5.99";
    protection.enforce(
        new PreCommitContext(
            TABLE,
            Optional.ofNullable(existing).map(ReadBridgeStripProtectionTest::metadata),
            metadata(prepared),
            ImmutableMap.copyOf(prepared.getTableProperties()),
            prepared.getJsonSnapshots() != null),
        new ColumnDefaultClientGate(
            "0.5.100",
            new CatalogRequestContext() {
              @Override
              public Optional<String> clientName() {
                return Optional.of("spark");
              }

              @Override
              public Optional<String> userAgent() {
                return Optional.of(userAgent);
              }
            }));
    return prepared;
  }

  /** {@code table} as catalog metadata: the current version, or the one a commit proposes. */
  private static TableMetadata metadata(TableDto table) {
    Schema schema = SchemaParser.fromJson(table.getSchema());
    return TableMetadata.buildFromEmpty()
        .setLocation("file:/data/openhouse/db/tbl-uuid")
        .setCurrentSchema(schema, schema.highestFieldId())
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .setProperties(table.getTableProperties())
        .build();
  }

  /** Rejects tables marked {@link #unusable}; every other table has a usable default on field 2. */
  private static ColumnDefaultsSource rejecting(ColumnDefaultException reported) {
    return tableDto -> {
      if (tableDto.getTableProperties().containsKey("unusable")) {
        throw reported;
      }
      return Collections.singletonMap(2, TextNode.valueOf("US"));
    };
  }

  /** Marks {@code table} for {@link #rejecting}. */
  private static TableDto unusable(TableDto table) {
    Map<String, String> properties = new HashMap<>(table.getTableProperties());
    properties.put("unusable", "true");
    return table.toBuilder().tableProperties(properties).build();
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
    return ramped(schema)
        .toBuilder()
        .jsonSnapshots(Collections.singletonList(jsonSnapshot))
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

  private static String overwriteJson(long snapshotId) {
    return snapshotJson(snapshotId, "overwrite");
  }

  private static String snapshotJson(long snapshotId, String operation) {
    return "{\"snapshot-id\":"
        + snapshotId
        + ",\"timestamp-ms\":1,\"summary\":{\"operation\":\""
        + operation
        + "\"},\"manifest-list\":\"file:/tmp/m.avro\",\"schema-id\":0}";
  }
}
