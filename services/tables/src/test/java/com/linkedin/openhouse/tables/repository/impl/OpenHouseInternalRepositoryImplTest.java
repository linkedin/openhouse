package com.linkedin.openhouse.tables.repository.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OpenHouseInternalRepositoryImplTest {

  @Mock private PoliciesSpecMapper policiesMapper;
  @Mock private MeterRegistry meterRegistry;
  @Mock private ClusterProperties clusterProperties;
  @Mock private PreservedKeyChecker preservedKeyChecker;
  @Mock private OpenHouseInternalCatalog catalog;

  @InjectMocks private OpenHouseInternalRepositoryImpl openHouseInternalRepository;

  private static final String DB_ID = "db";
  private static final String TABLE_ID = "table";
  private static final String SCHEMA_JSON =
      "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"string\"}]}";

  @BeforeEach
  void setUp() {
    when(meterRegistry.counter(anyString())).thenReturn(mock(Counter.class));
    when(preservedKeyChecker.allowKeyInCreation(anyString(), any())).thenReturn(true);
    when(policiesMapper.toPoliciesJsonString(any())).thenReturn("{}");
    when(clusterProperties.getClusterIcebergWriteFormatDefault()).thenReturn("parquet");
  }

  @Test
  void testComputePropsForTableCreation_DefaultMetadataVersions() {
    int clusterDefaultMaxMetadataVersions = 10;
    when(clusterProperties.getClusterIcebergWriteMetadataPreviousVersionsMax())
        .thenReturn(clusterDefaultMaxMetadataVersions);
    when(clusterProperties.isClusterIcebergWriteMetadataDeleteAfterCommitEnabled())
        .thenReturn(true);
    when(clusterProperties.getClusterIcebergFormatVersion()).thenReturn(2);

    TableDto tableDto = createTableDto(new HashMap<>());
    Map<String, String> actualProps =
        openHouseInternalRepository.computePropsForTableCreation(tableDto);

    Assertions.assertEquals(
        String.valueOf(clusterDefaultMaxMetadataVersions),
        actualProps.get(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX));
  }

  @Test
  void testComputePropsForTableCreation_UserProvidedMetadataVersions() {
    int clusterDefaultMaxMetadataVersions = 10;
    String userProvidedMaxMetadataVersions = "5";

    // Although cluster property is mocked, it shouldn't be used for the key
    when(clusterProperties.isClusterIcebergWriteMetadataDeleteAfterCommitEnabled())
        .thenReturn(true);
    when(clusterProperties.getClusterIcebergFormatVersion()).thenReturn(2);

    Map<String, String> userProps = new HashMap<>();
    userProps.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, userProvidedMaxMetadataVersions);
    TableDto tableDto = createTableDto(userProps);

    Map<String, String> actualProps =
        openHouseInternalRepository.computePropsForTableCreation(tableDto);

    Assertions.assertEquals(
        userProvidedMaxMetadataVersions,
        actualProps.get(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX));
  }

  @Test
  void testComputePropsForTableCreation_ClusterOrcCompressionCodec() {
    when(clusterProperties.getClusterIcebergWriteOrcCompressionCodec()).thenReturn("zstd");

    TableDto tableDto = createTableDto(new HashMap<>());
    Map<String, String> actualProps =
        openHouseInternalRepository.computePropsForTableCreation(tableDto);

    Assertions.assertEquals("zstd", actualProps.get(TableProperties.ORC_COMPRESSION));
  }

  @Test
  void testComputePropsForTableCreation_UserProvidedOrcCompressionCodecWins() {
    // Cluster default is set but the user-provided value should take precedence.
    when(clusterProperties.getClusterIcebergWriteOrcCompressionCodec()).thenReturn("zstd");

    Map<String, String> userProps = new HashMap<>();
    userProps.put(TableProperties.ORC_COMPRESSION, "snappy");
    TableDto tableDto = createTableDto(userProps);

    Map<String, String> actualProps =
        openHouseInternalRepository.computePropsForTableCreation(tableDto);

    Assertions.assertEquals("snappy", actualProps.get(TableProperties.ORC_COMPRESSION));
  }

  @Test
  void testComputePropsForTableCreation_NoOrcCompressionCodecWhenClusterUnset() {
    // When the cluster does not configure a codec, the property should not be set.
    when(clusterProperties.getClusterIcebergWriteOrcCompressionCodec()).thenReturn(null);

    TableDto tableDto = createTableDto(new HashMap<>());
    Map<String, String> actualProps =
        openHouseInternalRepository.computePropsForTableCreation(tableDto);

    Assertions.assertFalse(actualProps.containsKey(TableProperties.ORC_COMPRESSION));
  }

  @Test
  void testComputePropsForTableCreation_tableLocation() {
    TableDto tableDto = createTableDto(new HashMap<>());
    tableDto = tableDto.toBuilder().tableLocation("file:///data/openhouse/db/table").build();

    Map<String, String> actualProps =
        openHouseInternalRepository.computePropsForTableCreation(tableDto);

    Assertions.assertEquals(
        "/data/openhouse/db/table",
        actualProps.get(HouseTableSerdeUtils.getCanonicalFieldName("tableLocation")));
  }

  @Test
  void findTableRefByIdReturnsPartialTableDto() {
    HouseTable row =
        HouseTable.builder()
            .databaseId(DB_ID)
            .tableId(TABLE_ID)
            .tableUUID("uuid-1")
            .tableLocation("/base/db/table-uuid-1/00001-x.metadata.json")
            .build();
    when(catalog.findHouseTable(TableIdentifier.of(DB_ID, TABLE_ID))).thenReturn(Optional.of(row));

    Optional<TableDto> result =
        openHouseInternalRepository.findTableRefById(
            TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build());

    Assertions.assertTrue(result.isPresent());
    TableDto dto = result.get();
    Assertions.assertEquals(DB_ID, dto.getDatabaseId());
    Assertions.assertEquals(TABLE_ID, dto.getTableId());
    Assertions.assertEquals("uuid-1", dto.getTableUUID());
    Assertions.assertEquals("/base/db/table-uuid-1/00001-x.metadata.json", dto.getTableLocation());
    // Fields not populated by the table-ref lookup should be null/default.
    Assertions.assertNull(dto.getSchema());
    Assertions.assertNull(dto.getTableCreator());
  }

  @Test
  void findTableRefByIdReturnsEmptyWhenHouseTableMissing() {
    when(catalog.findHouseTable(any(TableIdentifier.class))).thenReturn(Optional.empty());

    Optional<TableDto> result =
        openHouseInternalRepository.findTableRefById(
            TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build());

    Assertions.assertFalse(result.isPresent());
  }

  @ParameterizedTest
  @MethodSource("droppedPartitionColumns")
  void testValidateReplaceTable_DroppedPartitionSourceRejected(
      Schema existingSchema, PartitionSpec spec, Schema replacementSchema, String columnName) {
    TableIdentifier identifier = TableIdentifier.of(DB_ID, TABLE_ID);
    Table table = mock(Table.class);
    when(catalog.loadTable(identifier)).thenReturn(table);
    when(table.properties())
        .thenReturn(Collections.singletonMap(CatalogConstants.RTAS_ENABLED_TABLE_PROP, "true"));
    when(table.schema()).thenReturn(existingSchema);
    when(table.specs()).thenReturn(Collections.singletonMap(spec.specId(), spec));

    UnsupportedClientOperationException exception =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.validateReplaceTable(identifier, replacementSchema));

    Assertions.assertEquals(
        "RTAS does not support dropping a partition column "
            + columnName
            + ". Only removing partitioning is allowed",
        exception.getMessage());
  }

  private static Stream<Arguments> droppedPartitionColumns() {
    Schema flatSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    Schema droppedId = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));
    Schema renamedId =
        new Schema(
            Types.NestedField.required(1, "renamedId", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    Schema nestedSchema =
        new Schema(
            Types.NestedField.required(
                1,
                "header",
                Types.StructType.of(Types.NestedField.required(2, "id", Types.LongType.get()))),
            Types.NestedField.required(3, "id", Types.LongType.get()),
            Types.NestedField.optional(4, "data", Types.StringType.get()));
    return Stream.of(
        Arguments.of(
            flatSchema,
            PartitionSpec.builderFor(flatSchema).identity("id").build(),
            droppedId,
            "id"),
        Arguments.of(
            flatSchema,
            PartitionSpec.builderFor(flatSchema).bucket("id", 4).build(),
            droppedId,
            "id"),
        Arguments.of(
            flatSchema,
            PartitionSpec.builderFor(flatSchema).identity("id").build(),
            renamedId,
            "id"),
        Arguments.of(
            nestedSchema,
            PartitionSpec.builderFor(nestedSchema).bucket("header.id", 4).build(),
            flatSchema,
            "header.id"));
  }

  @ParameterizedTest
  @CsvSource({
    "false, false, false, REPLACE TABLE AS SELECT is not enabled",
    "true, true, false, while WAP",
    "true, false, true, while replication"
  })
  void testValidateReplaceTable_PartitionColumnDropGatePreservesOtherRestrictions(
      boolean replaceEnabled, boolean wapEnabled, boolean replicationEnabled, String message) {
    TableIdentifier identifier = TableIdentifier.of(DB_ID, TABLE_ID);
    Table table = mock(Table.class);
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogConstants.RTAS_ENABLED_TABLE_PROP, Boolean.toString(replaceEnabled));
    properties.put(CatalogConstants.WAP_ENABLED_TABLE_PROP, Boolean.toString(wapEnabled));
    properties.put(CatalogConstants.RTAS_PARTITION_COLUMN_DROP_ENABLED_TABLE_PROP, "true");
    when(catalog.loadTable(identifier)).thenReturn(table);
    when(table.properties()).thenReturn(properties);
    if (replicationEnabled) {
      when(policiesMapper.toPoliciesObject(null))
          .thenReturn(
              Policies.builder()
                  .replication(
                      Replication.builder()
                          .config(
                              Collections.singletonList(
                                  ReplicationConfig.builder()
                                      .destination("WAR")
                                      .interval("12H")
                                      .build()))
                          .build())
                  .build());
    }

    UnsupportedClientOperationException exception =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () ->
                openHouseInternalRepository.validateReplaceTable(
                    identifier,
                    new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()))));

    Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
  }

  private TableDto createTableDto(Map<String, String> properties) {
    return TableDto.builder()
        .databaseId(DB_ID)
        .tableId(TABLE_ID)
        .schema(SCHEMA_JSON)
        .tableProperties(properties)
        .tableVersion("v1")
        .tableType(TableType.PRIMARY_TABLE)
        .build();
  }
}
