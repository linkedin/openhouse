package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.common.test.schema.ResourceIoHelper;
import com.linkedin.openhouse.internal.catalog.TableCommit;
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

  private static final String ENABLED_PROP = ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP;
  private static final String METADATA_LOCATION =
      "file:/data/openhouse/db/tbl-uuid/00001-x.metadata.json";
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");

  private static final ColumnDefaultsSource FIELD_2 =
      tableDto -> Collections.singletonMap(2, TextNode.valueOf("US"));

  /** Stamps field 2 when the table's {@code default-field} property names it. */
  private static final ColumnDefaultsSource FROM_PROPERTY =
      tableDto -> {
        String raw = tableDto.getTableProperties().get("default-field");
        return raw == null
            ? Collections.emptyMap()
            : Collections.singletonMap(Integer.parseInt(raw), TextNode.valueOf("US"));
      };

  @Test
  public void prepare_stripsInitialDefaults() throws Exception {
    TableDto prepared = protection(FIELD_2).prepare(incoming(SCHEMA_WITH_DEFAULT, optIn()));

    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
    Assertions.assertTrue(prepared.getSchema().contains("\"id\":2"));
  }

  @Test
  public void prepare_stripsNestedInitialDefaults() throws Exception {
    TableDto prepared = protection(FIELD_2).prepare(incoming(NESTED_WITH_DEFAULT, optIn()));

    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void prepare_withoutInitialDefaults_leavesTheSchemaUnchanged() throws Exception {
    TableDto prepared = protection(FIELD_2).prepare(incoming(SCHEMA_WITHOUT_DEFAULT, optIn()));

    Assertions.assertEquals(SCHEMA_WITHOUT_DEFAULT, prepared.getSchema());
  }

  @Test
  public void prepare_stripsUnstampedWriterDefaults() throws Exception {
    TableDto prepared =
        protection(FIELD_2).prepare(incoming(SCHEMA_UNSTAMPED_WRITER_DEFAULTS, optIn()));

    Assertions.assertFalse(prepared.getSchema().contains("initial-default"));
  }

  @Test
  public void prepare_unreadableSchema_failsClosed() {
    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () -> protection(FIELD_2).prepare(incoming("{", optIn())));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_UNUSABLE"));
    Assertions.assertTrue(thrown.getMessage().contains("unreadable json"));
    Assertions.assertTrue(thrown.getMessage().contains(METADATA_LOCATION));
  }

  /**
   * A missing intermediate schema is rejected rather than passed through: the commit would
   * otherwise skip it and persist an incomplete schema history.
   */
  @Test
  public void prepare_missingIntermediateSchema_failsClosed() {
    TableDto incoming =
        incoming(SCHEMA_WITH_DEFAULT, optIn())
            .toBuilder()
            .newIntermediateSchemas(Arrays.asList(SCHEMA_WITH_DEFAULT, ""))
            .build();
    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> protection(FIELD_2).prepare(incoming));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_UNUSABLE"));
    Assertions.assertTrue(thrown.getMessage().contains("missing schema"));
  }

  @Test
  public void snapshotCommitFromUnawareClient_rejectedWhenOptedIn() {
    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                commit(
                    protection(FIELD_2),
                    base(SCHEMA_WITHOUT_DEFAULT, optIn()),
                    incoming(SCHEMA_WITH_DEFAULT, optIn()),
                    true,
                    false));
    Assertions.assertTrue(thrown.getMessage().startsWith("COLUMN_DEFAULT_UNAWARE_COMMIT"));
    Assertions.assertTrue(thrown.getMessage().contains("country (field-id 2)"));
    Assertions.assertTrue(thrown.getMessage().contains("Spark 3.1"));
  }

  /** The client decides Type 2, not the {@code initial-default} it sent. */
  @Test
  public void snapshotCommitFromDefaultAwareClient_allowed() {
    Assertions.assertDoesNotThrow(
        () ->
            commit(
                protection(FIELD_2),
                base(SCHEMA_WITHOUT_DEFAULT, optIn()),
                incoming(SCHEMA_WITHOUT_DEFAULT, optIn()),
                true,
                true));
  }

  @Test
  public void snapshotCommitWithoutInitialDefault_allowedWhenNotOptedIn() {
    Map<String, String> optedOut = Collections.singletonMap(ENABLED_PROP, "false");

    Assertions.assertDoesNotThrow(
        () ->
            commit(
                protection(FIELD_2),
                base(SCHEMA_WITHOUT_DEFAULT, optedOut),
                incoming(SCHEMA_WITHOUT_DEFAULT, optedOut),
                true,
                false));
  }

  @Test
  public void schemaOnlyCommitOptingOut_doesNotType1() {
    Assertions.assertDoesNotThrow(
        () ->
            commit(
                protection(FIELD_2),
                base(SCHEMA_WITHOUT_DEFAULT, optIn()),
                incoming(SCHEMA_WITHOUT_DEFAULT, Collections.singletonMap(ENABLED_PROP, "false")),
                false,
                false));
  }

  @Test
  public void snapshotCommitOptingOut_stillType2() {
    Assertions.assertThrows(
        ColumnDefaultException.class,
        () ->
            commit(
                protection(FIELD_2),
                base(SCHEMA_WITHOUT_DEFAULT, optIn()),
                incoming(SCHEMA_WITHOUT_DEFAULT, Collections.singletonMap(ENABLED_PROP, "false")),
                true,
                false));
  }

  @Test
  public void droppingLiveDefault_rejected() {
    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                commit(
                    protection(FROM_PROPERTY),
                    base(SCHEMA_WITHOUT_DEFAULT, stampsField2()),
                    incoming(SCHEMA_WITHOUT_DEFAULT, optIn()),
                    false,
                    true));
    Assertions.assertTrue(thrown.getMessage().contains("COLUMN_DEFAULT_REMOVED"));
    Assertions.assertTrue(thrown.getMessage().contains("country (field-id 2)"));
    Assertions.assertTrue(thrown.getMessage().contains("cannot be removed or changed"));
  }

  @Test
  public void droppingColumnThatHadDefault_allowed() {
    Assertions.assertDoesNotThrow(
        () ->
            commit(
                protection(FROM_PROPERTY),
                base(SCHEMA_WITHOUT_DEFAULT, stampsField2()),
                incoming(SCHEMA_WITHOUT_COUNTRY, optIn()),
                false,
                true));
  }

  @Test
  public void schemaOnlyCommit_doesNotType2() {
    Assertions.assertDoesNotThrow(
        () ->
            commit(
                protection(FIELD_2),
                base(SCHEMA_WITHOUT_DEFAULT, optIn()),
                incoming(SCHEMA_WITHOUT_DEFAULT, optIn()),
                false,
                false));
  }

  /** A buggy source fails the commit closed, as itself rather than as anyone's unusable default. */
  @Test
  public void sourceBug_failsClosedAsItself() {
    IllegalStateException bug = new IllegalStateException("encoder exploded");
    ReadBridgeStripProtection protection =
        protection(
            tableDto -> {
              throw bug;
            });

    Assertions.assertSame(
        bug,
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                commit(
                    protection,
                    base(SCHEMA_WITHOUT_DEFAULT, optIn()),
                    incoming(SCHEMA_WITHOUT_DEFAULT, optIn()),
                    true,
                    true)));
  }

  /**
   * A default the stored table declares but cannot apply is the server's fault, not the commit's.
   */
  @Test
  public void storedUnusableDefault_failsAsServerError() {
    ColumnDefaultException reported = unusableDefault();
    Map<String, String> stored = new HashMap<>(optIn());
    stored.put("unusable", "true");

    TableConfigUnavailableException thrown =
        Assertions.assertThrows(
            TableConfigUnavailableException.class,
            () ->
                commit(
                    protection(rejectingUnusable(reported)),
                    base(SCHEMA_WITHOUT_DEFAULT, stored),
                    incoming(SCHEMA_WITHOUT_DEFAULT, optIn()),
                    true,
                    true));
    Assertions.assertSame(reported, thrown.getCause());
  }

  /** A default the commit declares but cannot apply rejects it with the source's own exception. */
  @Test
  public void declaredUnusableDefault_rejectsTheCommit() {
    ColumnDefaultException reported = unusableDefault();
    Map<String, String> declared = new HashMap<>(optIn());
    declared.put("unusable", "true");

    Assertions.assertSame(
        reported,
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                commit(
                    protection(rejectingUnusable(reported)),
                    base(SCHEMA_WITHOUT_DEFAULT, optIn()),
                    incoming(SCHEMA_WITHOUT_DEFAULT, declared),
                    true,
                    true)));
  }

  /** A create has no stored defaults to protect, but its declared defaults must still apply. */
  @Test
  public void createDeclaringUnusableDefault_rejected() {
    ColumnDefaultException reported = unusableDefault();
    Map<String, String> declared = new HashMap<>(optIn());
    declared.put("unusable", "true");

    Assertions.assertSame(
        reported,
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                commit(
                    protection(rejectingUnusable(reported)),
                    null,
                    incoming(SCHEMA_WITHOUT_DEFAULT, declared),
                    true,
                    true)));
  }

  /**
   * Prepares {@code incoming} and checks the commit it makes on {@code base}, from a client that
   * applies column defaults or not.
   */
  private static void commit(
      ReadBridgeStripProtection protection,
      TableMetadata base,
      TableDto incoming,
      boolean snapshotCommit,
      boolean defaultAwareClient)
      throws ColumnDefaultException {
    TableDto prepared = protection.prepare(incoming);
    protection.enforce(
        new TableCommit(
            TABLE,
            Optional.ofNullable(base),
            SchemaParser.fromJson(prepared.getSchema()),
            ImmutableMap.copyOf(prepared.getTableProperties()),
            snapshotCommit),
        defaultAwareClient);
  }

  private static TableMetadata base(String schemaJson, Map<String, String> properties) {
    Schema schema = SchemaParser.fromJson(schemaJson);
    return TableMetadata.buildFromEmpty()
        .setLocation("file:/data/openhouse/db/tbl-uuid")
        .setCurrentSchema(schema, schema.highestFieldId())
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .setProperties(properties)
        .build();
  }

  private static TableDto incoming(String schema, Map<String, String> properties) {
    return TableDto.builder()
        .databaseId("db")
        .tableId("tbl")
        .tableLocation(METADATA_LOCATION)
        .schema(schema)
        .tableProperties(properties)
        .build();
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

  /** Rejects tables marked {@code unusable}; every other table has a usable default on field 2. */
  private static ColumnDefaultsSource rejectingUnusable(ColumnDefaultException reported) {
    return tableDto -> {
      if (tableDto.getTableProperties().containsKey("unusable")) {
        throw reported;
      }
      return Collections.singletonMap(2, TextNode.valueOf("US"));
    };
  }

  private static ColumnDefaultException unusableDefault() {
    return new ColumnDefaultException("column country: default is not a string");
  }

  private static Map<String, String> optIn() {
    return Collections.singletonMap(ENABLED_PROP, "true");
  }

  private static Map<String, String> stampsField2() {
    Map<String, String> properties = new HashMap<>(optIn());
    properties.put("default-field", "2");
    return properties;
  }
}
