package com.linkedin.openhouse.tables.repository.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
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

  @Test
  void findTableRefByIdThrowsWhenCatalogIsNotOpenHouseInternalCatalog() {
    // Build a fresh impl with a non-OpenHouseInternal Catalog wired in.
    OpenHouseInternalRepositoryImpl impl = new OpenHouseInternalRepositoryImpl();
    impl.catalog = mock(Catalog.class);

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            impl.findTableRefById(
                TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build()));
  }

  // ---------------------------------------------------------------------------------------------
  // Typed table load vs. shared-name occupancy
  // ---------------------------------------------------------------------------------------------

  private static HouseTable pointer(String entityType) {
    return HouseTable.builder()
        .databaseId(DB_ID)
        .tableId(TABLE_ID)
        .tableUUID("uuid-1")
        .tableLocation("/base/db/table-uuid-1/00001-x.metadata.json")
        .entityType(entityType)
        .build();
  }

  /**
   * {@code findTableRefById} answers "can this key be operated on as a table?" — it backs drop. A
   * VIEW or unknown pointer must read as absent so a view can never be dropped through the table
   * API.
   */
  @ParameterizedTest
  @ValueSource(strings = {"VIEW", "view", "ViEw", "UNKNOWN"})
  void findTableRefByIdReturnsEmptyForNonTable(String entityType) {
    when(catalog.findHouseTable(TableIdentifier.of(DB_ID, TABLE_ID)))
        .thenReturn(Optional.of(pointer(entityType)));

    Assertions.assertFalse(
        openHouseInternalRepository
            .findTableRefById(
                TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build())
            .isPresent(),
        "entityType=" + entityType + " must not resolve to a table ref");
  }

  /** The complement: null and every spelling of TABLE keep their existing partial DTO mapping. */
  @ParameterizedTest
  @CsvSource(
      nullValues = "NULL",
      value = {"NULL", "TABLE", "table", "TaBlE"})
  void findTableRefByIdAcceptsNullAndCaseInsensitiveTable(String entityType) {
    when(catalog.findHouseTable(TableIdentifier.of(DB_ID, TABLE_ID)))
        .thenReturn(Optional.of(pointer(entityType)));

    Optional<TableDto> result =
        openHouseInternalRepository.findTableRefById(
            TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build());

    Assertions.assertTrue(result.isPresent(), "entityType=" + entityType + " must be a table");
    TableDto dto = result.get();
    Assertions.assertEquals(DB_ID, dto.getDatabaseId());
    Assertions.assertEquals(TABLE_ID, dto.getTableId());
    Assertions.assertEquals("uuid-1", dto.getTableUUID());
    Assertions.assertEquals("/base/db/table-uuid-1/00001-x.metadata.json", dto.getTableLocation());
    Assertions.assertNull(dto.getSchema());
    Assertions.assertNull(dto.getTableCreator());
  }

  /**
   * Occupancy is deliberately NOT the same question as typed load. This method answers "is this
   * shared key taken, and by what?" so CREATE and rename-destination can reject an occupied name
   * accurately instead of seeing a view-hidden key as free. It must therefore see EVERY raw pointer
   * — including unknown types, which stay present so callers fail closed — and must never parse
   * metadata (never call loadTable).
   */
  @ParameterizedTest
  @CsvSource(
      nullValues = "NULL",
      value = {
        "NULL,    TABLE",
        "TABLE,   TABLE",
        "table,   TABLE",
        "TaBlE,   TABLE",
        "VIEW,    VIEW",
        "view,    VIEW",
        "ViEw,    VIEW",
        "UNKNOWN, UNKNOWN"
      })
  void findOccupyingEntityTypeSeesEveryRawPointerWithoutLoadingMetadata(
      String storedEntityType, String expectedCanonical) {
    when(catalog.findHouseTable(TableIdentifier.of(DB_ID, TABLE_ID)))
        .thenReturn(Optional.of(pointer(storedEntityType)));

    Optional<String> occupancy =
        openHouseInternalRepository.findOccupyingEntityTypeById(
            TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build());

    Assertions.assertTrue(
        occupancy.isPresent(),
        "A stored pointer with entityType=" + storedEntityType + " occupies the name");
    Assertions.assertEquals(expectedCanonical, occupancy.get());

    verify(catalog).findHouseTable(TableIdentifier.of(DB_ID, TABLE_ID));
    verify(catalog, never()).loadTable(any(TableIdentifier.class));
  }

  /** Only a genuinely absent pointer means the name is free. */
  @Test
  void findOccupyingEntityTypeReturnsEmptyOnlyWhenNoPointerExists() {
    when(catalog.findHouseTable(any(TableIdentifier.class))).thenReturn(Optional.empty());

    Assertions.assertFalse(
        openHouseInternalRepository
            .findOccupyingEntityTypeById(
                TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build())
            .isPresent());

    verify(catalog, never()).loadTable(any(TableIdentifier.class));
  }

  /**
   * HTS 4xx must PROPAGATE out of the occupancy lookup. Swallowing a repository error into "the
   * name is free" would let a CREATE proceed over an existing view during an HTS incident, which is
   * exactly the hole this occupancy check exists to close.
   *
   * <p>Stubbed with {@code doThrow(...).when(...)} rather than {@code when(...).thenThrow(...)}:
   * the latter evaluates its argument, which invokes the mock and would blow up the test itself.
   */
  @Test
  void findOccupyingEntityTypeDoesNotSwallowClientErrors() {
    Mockito.doThrow(
            new HouseTableCallerException("HTS returned 400", new RuntimeException("bad request")))
        .when(catalog)
        .findHouseTable(any(TableIdentifier.class));

    Assertions.assertThrows(
        HouseTableCallerException.class,
        () ->
            openHouseInternalRepository.findOccupyingEntityTypeById(
                TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build()));
  }

  /**
   * HTS 5xx must PROPAGATE for the same reason: an outage must never read as an unoccupied name.
   * This is the branch that a broad {@code catch (Exception e) { return Optional.empty(); }} would
   * silently convert into "free", reopening the CREATE-over-VIEW hole.
   */
  @Test
  void findOccupyingEntityTypeDoesNotSwallowServerErrors() {
    Mockito.doThrow(
            new HouseTableRepositoryStateUnknownException(
                "HTS returned 503", new RuntimeException("unavailable")))
        .when(catalog)
        .findHouseTable(any(TableIdentifier.class));

    Assertions.assertThrows(
        HouseTableRepositoryStateUnknownException.class,
        () ->
            openHouseInternalRepository.findOccupyingEntityTypeById(
                TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build()));
  }

  /** Occupancy follows the same unsupported-catalog contract as {@code findTableRefById}. */
  @Test
  void findOccupyingEntityTypeThrowsWhenCatalogIsNotOpenHouseInternalCatalog() {
    OpenHouseInternalRepositoryImpl impl = new OpenHouseInternalRepositoryImpl();
    impl.catalog = mock(Catalog.class);

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            impl.findOccupyingEntityTypeById(
                TableDtoPrimaryKey.builder().databaseId(DB_ID).tableId(TABLE_ID).build()));
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
