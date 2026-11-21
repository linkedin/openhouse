package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.common.exception.CommitRejectedException;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.common.test.schema.ResourceIoHelper;
import com.linkedin.openhouse.internal.catalog.PendingCommit;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.model.TableDto;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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
      "Use a client that declares column-defaults in X-OpenHouse-Client-Capabilities.";

  private static final ColumnDefaultsSource FIELD_2 =
      tableDto -> Collections.singletonMap(2, TextNode.valueOf("US"));

  /** Declares {@code country} (field 2) as the table's {@code country-default}, if it sets one. */
  private static final ColumnDefaultsSource COUNTRY_FROM_PROPERTY =
      tableDto -> {
        String value = tableDto.getTableProperties().get("country-default");
        return value == null
            ? Collections.emptyMap()
            : Collections.singletonMap(2, TextNode.valueOf(value));
      };

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

    CommitRejectedException thrown =
        Assertions.assertThrows(
            CommitRejectedException.class, () -> commit(protection, existing, incoming, false));
    Assertions.assertEquals(
        "CLIENT_CAPABILITY_MISSING: db.tbl requires the column-defaults capability, which this"
            + " client does not declare (X-Client-Name: spark, User-Agent: (none),"
            + " X-OpenHouse-Client-Capabilities: (none)). "
            + REMEDY,
        thrown.getMessage());
  }

  /** The client decides Type 2, not the {@code initial-default} it sent. */
  @Test
  public void overwriteWithMatchingInitialDefault_rejectedFromUnawareClient() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = ramped(SCHEMA_WITH_DEFAULT, overwrite(10));

    CommitRejectedException thrown =
        Assertions.assertThrows(
            CommitRejectedException.class, () -> commit(protection, existing, incoming, false));
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

  /**
   * {@link ColumnDefaultCommitPolicy} refuses an opt-out before this check runs. Without the
   * policy, Type 1 still refuses an opt-out that drops a live default.
   */
  @Test
  public void optOutDroppingLiveDefault_rejected() {
    ReadBridgeStripProtection protection = protection(FIELD_2);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(Collections.singletonMap(ENABLED_PROP, "false"))
            .build();

    CommitRejectedException thrown =
        Assertions.assertThrows(
            CommitRejectedException.class, () -> commit(protection, existing, incoming, true));
    Assertions.assertTrue(thrown.getMessage().startsWith("COLUMN_DEFAULT_REMOVED: "));
    Assertions.assertTrue(thrown.getMessage().contains("on country (field-id 2)"));
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

    CommitRejectedException thrown =
        Assertions.assertThrows(
            CommitRejectedException.class, () -> commit(protection, existing, incoming, true));
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
  public void changingLiveDefault_rejected() {
    ReadBridgeStripProtection protection = protection(COUNTRY_FROM_PROPERTY);
    TableDto existing = withCountryDefault(ramped(SCHEMA_WITHOUT_DEFAULT), "US");
    TableDto incoming = withCountryDefault(ramped(SCHEMA_WITHOUT_DEFAULT), "FR");

    CommitRejectedException thrown =
        Assertions.assertThrows(
            CommitRejectedException.class, () -> commit(protection, existing, incoming, true));
    Assertions.assertTrue(thrown.getMessage().startsWith("COLUMN_DEFAULT_CHANGED: "));
    Assertions.assertTrue(thrown.getMessage().contains("on country (field-id 2)"));
  }

  /**
   * After the opt-in, a column gets a default only in the commit that adds it, as in Iceberg, even
   * when the table has no default to protect yet.
   */
  @Test
  public void firstDefaultOnExistingColumn_rejected() {
    ReadBridgeStripProtection protection = protection(COUNTRY_FROM_PROPERTY);
    TableDto existing = ramped(SCHEMA_WITHOUT_DEFAULT);
    TableDto incoming = withCountryDefault(ramped(SCHEMA_WITHOUT_DEFAULT), "US");

    CommitRejectedException thrown =
        Assertions.assertThrows(
            CommitRejectedException.class, () -> commit(protection, existing, incoming, true));
    Assertions.assertTrue(thrown.getMessage().startsWith("COLUMN_DEFAULT_CHANGED: "));
    Assertions.assertTrue(thrown.getMessage().contains("on country (field-id 2)"));
  }

  @Test
  public void defaultOnColumnTheCommitAdds_allowed() {
    ReadBridgeStripProtection protection = protection(COUNTRY_FROM_PROPERTY);
    TableDto existing = ramped(SCHEMA_WITHOUT_COUNTRY);
    TableDto incoming = withCountryDefault(ramped(SCHEMA_WITHOUT_DEFAULT), "US");

    Assertions.assertDoesNotThrow(() -> commit(protection, existing, incoming, true));
  }

  /** The commit that opts a table in may declare a default on a column the table already has. */
  @Test
  public void defaultOnExistingColumnAtOptIn_allowed() {
    ReadBridgeStripProtection protection = protection(COUNTRY_FROM_PROPERTY);
    TableDto existing =
        TableDto.builder()
            .databaseId("db")
            .tableId("tbl")
            .schema(SCHEMA_WITHOUT_DEFAULT)
            .tableProperties(Collections.emptyMap())
            .build();
    TableDto incoming = withCountryDefault(ramped(SCHEMA_WITHOUT_DEFAULT), "US");

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
                CommitRejectedException.class, () -> commit(protection, existing, incoming, true))
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
                CommitRejectedException.class, () -> commit(protection, null, incoming, true))
            .getCause());
  }

  /**
   * Clients apply a default to every schema version that has its column. One that does not fit the
   * column's type in an older version would fail every load of the table.
   */
  @Test
  public void defaultThatDoesNotFitAnOlderSchemaVersion_rejected() {
    ReadBridgeStripProtection protection =
        protection(tableDto -> Collections.singletonMap(2, LongNode.valueOf(8_000_000_000L)));

    CommitRejectedException thrown =
        Assertions.assertThrows(
            CommitRejectedException.class,
            () -> commitOnPromotedColumn(protection, optIn(), optIn()));
    Assertions.assertTrue(thrown.getMessage().startsWith("COLUMN_DEFAULT_UNUSABLE: "));
    Assertions.assertTrue(
        thrown.getMessage().contains("db.tbl (TYPE_MISMATCH, column n, field ID 2)"));
    Assertions.assertTrue(
        thrown.getMessage().contains("int, the column's type in schema version 0"));
  }

  @Test
  public void defaultThatFitsEverySchemaVersion_allowed() {
    ReadBridgeStripProtection protection =
        protection(tableDto -> Collections.singletonMap(2, LongNode.valueOf(9L)));

    Assertions.assertDoesNotThrow(() -> commitOnPromotedColumn(protection, optIn(), optIn()));
  }

  /**
   * Promoting a float column to double renders its unchanged default anew, as a source that
   * converts defaults to the column's type does: 0.1 becomes 0.10000000149011612. That is no
   * change.
   */
  @Test
  public void promotionRenderingAnUnchangedDefaultAnew_allowed() {
    ReadBridgeStripProtection protection =
        protection(
            tableDto -> {
              boolean isFloat =
                  Types.FloatType.get()
                      .equals(SchemaParser.fromJson(tableDto.getSchema()).findType(2));
              return Collections.singletonMap(
                  2, isFloat ? FloatNode.valueOf(0.1f) : DoubleNode.valueOf(0.1f));
            });

    Assertions.assertDoesNotThrow(
        () ->
            commit(
                protection,
                ramped(SchemaParser.toJson(schemaWith("score", Types.FloatType.get()))),
                ramped(SchemaParser.toJson(schemaWith("score", Types.DoubleType.get()))),
                true));
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
   * client that declares the {@code column-defaults} capability or declares nothing. A request that
   * carries snapshots commits snapshots.
   */
  private static TableDto commit(
      ReadBridgeStripProtection protection,
      TableDto existing,
      TableDto incoming,
      boolean defaultAwareClient)
      throws ColumnDefaultException {
    TableDto prepared = protection.prepare(incoming);
    protection.enforce(
        new PendingCommit(
            TABLE,
            Optional.ofNullable(existing).map(ReadBridgeStripProtectionTest::metadata),
            metadata(prepared),
            ImmutableMap.copyOf(prepared.getTableProperties()),
            prepared.getJsonSnapshots() != null),
        sparkClient(defaultAwareClient));
    return prepared;
  }

  /**
   * Commits {@code properties}, from a default-aware client, to a table with {@code baseProperties}
   * whose column {@code n} (field 2) is an int in schema version 0 and a long in schema version 1,
   * the current one.
   */
  private static void commitOnPromotedColumn(
      ReadBridgeStripProtection protection,
      Map<String, String> baseProperties,
      Map<String, String> properties) {
    Schema intColumn = schemaWith("n", Types.IntegerType.get());
    Schema promoted = schemaWith("n", Types.LongType.get());
    TableMetadata metadata =
        TableMetadata.buildFrom(
                TableMetadata.newTableMetadata(
                    intColumn,
                    PartitionSpec.unpartitioned(),
                    "file:/data/openhouse/db/tbl-uuid",
                    baseProperties))
            .setCurrentSchema(promoted, promoted.highestFieldId())
            .build();
    protection.enforce(
        new PendingCommit(
            TABLE, Optional.of(metadata), metadata, ImmutableMap.copyOf(properties), false),
        sparkClient(true));
  }

  /** A schema of {@code id} (field 1) and {@code column} (field 2), which has {@code type}. */
  private static Schema schemaWith(String column, Type type) {
    return new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, column, type));
  }

  /** A Spark client that declares the {@code column-defaults} capability or declares nothing. */
  private static ClientCapabilityGate sparkClient(boolean defaultAwareClient) {
    return new ClientCapabilityGate(
        new CatalogRequestContext() {
          @Override
          public Optional<String> clientName() {
            return Optional.of("spark");
          }

          @Override
          public Set<String> clientCapabilities() {
            return defaultAwareClient
                ? Collections.singleton(ClientCapabilityGate.COLUMN_DEFAULTS)
                : Collections.emptySet();
          }
        });
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

  /** Sets the {@code country-default} that {@link #COUNTRY_FROM_PROPERTY} declares. */
  private static TableDto withCountryDefault(TableDto table, String value) {
    Map<String, String> properties = new HashMap<>(table.getTableProperties());
    properties.put("country-default", value);
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
