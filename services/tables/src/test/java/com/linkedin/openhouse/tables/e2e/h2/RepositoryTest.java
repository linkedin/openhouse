package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static org.apache.iceberg.types.Types.NestedField.*;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import com.linkedin.openhouse.tables.repository.SchemaValidator;
import com.linkedin.openhouse.tables.repository.impl.InternalRepositoryUtils;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class RepositoryTest {

  @Autowired HouseTableRepository houseTablesRepository;

  @SpyBean @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired StorageManager storageManager;

  @Autowired Catalog catalog;

  @Autowired SchemaValidator validator;

  @SpyBean @Autowired PreservedKeyChecker preservedKeyChecker;

  @Test
  void extractReservedProps() {
    // create an input map with some key-value pairs
    Map<String, String> inputMap = new HashMap<>();
    inputMap.put("openhouse.key1", "value1");
    inputMap.put("otherKey1", "value2");
    inputMap.put("openhouse.key2", "value3");
    TableDto mockTableDto = Mockito.mock(TableDto.class);

    // call the method to extract the reserved props
    // Casting towards a specific implementation here.
    Map<String, String> result =
        InternalRepositoryUtils.extractPreservedProps(inputMap, mockTableDto, preservedKeyChecker);

    // assert that the result map has the expected size and contents
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals("value1", result.get("openhouse.key1"));
    Assertions.assertEquals("value3", result.get("openhouse.key2"));

    // Test empty map make sure no exception
    inputMap.clear();
    result =
        InternalRepositoryUtils.extractPreservedProps(inputMap, mockTableDto, preservedKeyChecker);
    Assertions.assertEquals(0, result.size());
  }

  @Test
  public void testOpenHouseRepository() {
    TableDto creationDTO = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();

    openHouseInternalRepository.save(creationDTO);

    TableDtoPrimaryKey key = getPrimaryKey(creationDTO);

    TableDto table =
        openHouseInternalRepository.findById(key).orElseThrow(NoSuchElementException::new);
    verifyTable(table);

    HouseTable houseTable =
        houseTablesRepository
            .findById(getHouseTablePrimaryKey(key))
            .orElseThrow(NoSuchElementException::new);
    verifyTable(houseTable);

    Assertions.assertTrue(openHouseInternalRepository.existsById(key));

    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  @Test
  public void testConcurrentRepoOps() {
    TableDto creationDTO = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    TableDtoPrimaryKey key = getPrimaryKey(creationDTO);
    openHouseInternalRepository.save(creationDTO);

    // Simulating a scenario of table-already-existed exception and verified exception it throws.
    TableDto existedDto = creationDTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    OpenHouseInternalRepository spyRepo = Mockito.spy(openHouseInternalRepository);
    Mockito.doReturn(false).when(spyRepo).existsById(key);

    Assertions.assertThrows(
        org.apache.iceberg.exceptions.AlreadyExistsException.class, () -> spyRepo.save(existedDto));

    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  /**
   * Testing edge cases for tblprops. For normal behavior see {@link
   * TablesControllerTest#testUpdateProperties()}
   */
  @Test
  public void testTblPropsCornerCases() {
    Map<String, String> userProps = new HashMap<>();
    userProps.put("tableId", "foo"); /* make sure such key shouldn't confuse table service*/
    userProps.put(
        TableProperties.DEFAULT_FILE_FORMAT, "avro"); /* make sure such key will be overwritten */
    userProps.put(TableProperties.FORMAT_VERSION, "1"); /* make sure such key will be overwritten */
    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(userProps)
            .build();

    TableDto returnedDto = openHouseInternalRepository.save(createDto);
    Assertions.assertNotNull(returnedDto.getTableProperties());
    Assertions.assertFalse(returnedDto.getTableProperties().isEmpty());
    Assertions.assertEquals(returnedDto.getTableProperties().get("tableId"), "foo");
    Assertions.assertEquals(
        returnedDto.getTableProperties().get("openhouse.tableId"), TABLE_DTO.getTableId());
    Assertions.assertEquals(
        returnedDto.getTableProperties().get(TableProperties.DEFAULT_FILE_FORMAT).toLowerCase(),
        "orc");
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));
    Assertions.assertEquals(((BaseTable) table).operations().current().formatVersion(), 2);
    Assertions.assertNull(returnedDto.getTableProperties().get(TableProperties.FORMAT_VERSION));
    TableDtoPrimaryKey key = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  @Test
  public void testOpenHouseInvalidClusteringEvolution() {

    TableDto tableDto =
        openHouseInternalRepository.save(
            TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build());

    TableDto changeClusteringColDto =
        tableDto
            .toBuilder()
            .clustering(Arrays.asList(ClusteringColumn.builder().columnName("name").build()))
            .tableVersion(tableDto.getTableLocation())
            .build();

    UnsupportedClientOperationException unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(changeClusteringColDto));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDto removeClusteringDto =
        tableDto.toBuilder().tableVersion(tableDto.getTableLocation()).clustering(null).build();

    unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(removeClusteringDto));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDto addClusteringDto =
        tableDto
            .toBuilder()
            .tableVersion(tableDto.getTableLocation())
            .clustering(
                Streams.concat(
                        Arrays.asList(ClusteringColumn.builder().columnName("count").build())
                            .stream(),
                        TABLE_DTO.getClustering().stream())
                    .collect(Collectors.toList()))
            .build();
    unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(addClusteringDto));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDtoPrimaryKey key = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  @Test
  public void testOpenHouseInvalidTimePartitioningEvolution() {
    TableDto createTableDto = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    TableDto tableDto = openHouseInternalRepository.save(createTableDto);

    TableDto removeTimePartitionCol =
        tableDto
            .toBuilder()
            .timePartitioning(null)
            .tableVersion(tableDto.getTableLocation())
            .build();

    UnsupportedClientOperationException unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(removeTimePartitionCol));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDto differentGranularityTimePartitionCol =
        tableDto
            .toBuilder()
            .timePartitioning(
                TimePartitionSpec.builder()
                    .columnName("timestampCol")
                    .granularity(TimePartitionSpec.Granularity.DAY)
                    .build())
            .tableVersion(tableDto.getTableLocation())
            .build();

    unsupportedClientOperationException =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(differentGranularityTimePartitionCol));
    Assertions.assertEquals(
        "Evolution of table partitioning and clustering columns are not supported, recreate the table with new partition spec.",
        unsupportedClientOperationException.getMessage());

    TableDtoPrimaryKey key = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(key);
    Assertions.assertFalse(openHouseInternalRepository.existsById(key));
  }

  @Test
  public void testExistsByIdThatDoesNotExist() {

    Assertions.assertFalse(
        openHouseInternalRepository.existsById(
            TableDtoPrimaryKey.builder().databaseId("not_found").tableId("not_found").build()));

    NullPointerException nullPointerException =
        Assertions.assertThrows(
            NullPointerException.class,
            () -> openHouseInternalRepository.existsById(TableDtoPrimaryKey.builder().build()));
    Assertions.assertEquals(
        "Cannot create a namespace with a null level", nullPointerException.getMessage());
  }

  @Test
  public void testFindAllIds() {
    openHouseInternalRepository.save(
        TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build());
    openHouseInternalRepository.save(
        TABLE_DTO_DIFF_DB.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build());
    Assertions.assertEquals(2, openHouseInternalRepository.findAllIds().size());

    TableDtoPrimaryKey key = getPrimaryKey(TABLE_DTO);
    TableDtoPrimaryKey keyDiffDb = getPrimaryKey(TABLE_DTO_DIFF_DB);
    openHouseInternalRepository.deleteById(key);
    openHouseInternalRepository.deleteById(keyDiffDb);
  }

  @Test
  public void testCreateTableWithReservedProps() {
    /* The behavior is provided openhouse. properties are ignored */
    final String tblName = "offensiveMap";

    Map<String, String> offensiveMap = new HashMap<>();
    offensiveMap.put("openhouse.tableId", "random");
    offensiveMap.put("openhouse.tableVersion", "random");
    offensiveMap.put("openhouse.tableLocation", "random");
    offensiveMap.put("openhouse.keepReadOnlyProp", "true");
    TableDto offensiveDto =
        TABLE_DTO
            .toBuilder()
            .tableId("offensiveMap")
            .tableProperties(offensiveMap)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();
    PreservedKeyChecker spyPreservedKeyChecker = Mockito.spy(preservedKeyChecker);
    // Test extending the preservedKeyChecker enables allowlisting of properties during table
    // creation
    Mockito.doReturn(true)
        .when(spyPreservedKeyChecker)
        .allowKeyInCreation(Mockito.eq("openhouse.keepReadOnlyProp"), Mockito.any());
    // Demonstrated the offensive setting doesn't matter.
    TableDto createdDTO = openHouseInternalRepository.save(offensiveDto);
    Assertions.assertEquals(createdDTO.getTableId(), tblName);
    Map<String, String> createdTableProps = createdDTO.getTableProperties();
    // Should not be overridden by the user provided value given that these should be filtered on
    // creation
    Assertions.assertNotEquals(createdTableProps.get("openhouse.tableVersions"), "random");
    Assertions.assertNotEquals(createdTableProps.get("openhouse.tableLocation"), "random");
    Assertions.assertEquals(createdTableProps.get("openhouse.keepReadOnlyProp"), "true");

    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder().tableId(tblName).databaseId(TABLE_DTO.getDatabaseId()).build();

    Map<String, String> updatedTableProps = new HashMap<>();
    updatedTableProps.putAll(createdTableProps);
    updatedTableProps.put("openhouse.keepReadOnlyProp", "false");
    TableDto updatedOffensiveDto =
        TABLE_DTO
            .toBuilder()
            .tableId("offensiveMap")
            .tableProperties(updatedTableProps)
            .tableVersion(createdDTO.getTableLocation())
            .build();
    // Should fail due to updating preserved keys
    UnsupportedClientOperationException e =
        Assertions.assertThrows(
            UnsupportedClientOperationException.class,
            () -> openHouseInternalRepository.save(updatedOffensiveDto));
    Assertions.assertTrue(
        e.getMessage()
            .startsWith(
                "Bad tblproperties provided: Can't add, alter or drop table properties due to the restriction: "
                    + "[table properties starting with `openhouse.` cannot be modified], diff in existing "
                    + "& provided table properties: {"));

    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  public void testOpenHouseRepositoryForStagedTable() {
    TableDto returnedTableDto =
        openHouseInternalRepository.save(TableModelConstants.STAGED_TABLE_DTO);
    Assertions.assertEquals(
        TableModelConstants.STAGED_TABLE_DTO.getTableId(), returnedTableDto.getTableId());
    Assertions.assertEquals(
        TableModelConstants.STAGED_TABLE_DTO.getDatabaseId(), returnedTableDto.getDatabaseId());
    Assertions.assertEquals(
        TableModelConstants.STAGED_TABLE_DTO.getClusterId(), returnedTableDto.getClusterId());
    Assertions.assertNotNull(returnedTableDto.getTableLocation());
    Assertions.assertFalse(returnedTableDto.isStageCreate());

    Optional<TableDto> stagedTableDto =
        openHouseInternalRepository.findById(getPrimaryKey(TableModelConstants.STAGED_TABLE_DTO));
    Assertions.assertFalse(stagedTableDto.isPresent());
  }

  @Test
  public void testMetadataUpdateForDeleted() {
    /* create the base table */
    TableDto creationDTO = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    TableDto returnDTO = openHouseInternalRepository.save(creationDTO);

    /* prepare tableUpdate */
    Map<String, String> baseProps = new HashMap<>(returnDTO.getTableProperties());
    baseProps.put("action", "update");
    TableDto updateDto =
        returnDTO
            .toBuilder()
            .tableVersion(returnDTO.getTableLocation())
            .tableProperties(baseProps)
            .build();

    /* Using catalog to do update first. */
    TableIdentifier tableIdentifier =
        TableIdentifier.of(creationDTO.getDatabaseId(), creationDTO.getTableId());
    catalog.dropTable(tableIdentifier);

    Assertions.assertThrows(
        RequestValidationFailureException.class, () -> openHouseInternalRepository.save(updateDto));
    Assertions.assertThrows(NoSuchTableException.class, () -> catalog.loadTable(tableIdentifier));
  }

  @Test
  public void testMetadataConcurrentUpdate() {
    /* create the base table */
    TableDto creationDTO =
        TABLE_DTO
            .toBuilder()
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableType(TableType.PRIMARY_TABLE)
            .build();
    TableDto returnDTO = openHouseInternalRepository.save(creationDTO);

    /* prepare tableUpdate: Two base upon one version but different tblproperties updates are baked in. */
    Map<String, String> baseProps = new HashMap<>(returnDTO.getTableProperties());
    baseProps.put("action", "update");
    TableDto updateDtoSuccess =
        returnDTO
            .toBuilder()
            .tableVersion(returnDTO.getTableLocation())
            .tableProperties(baseProps)
            .build();

    Map<String, String> basePropsFail = new HashMap<>(returnDTO.getTableProperties());
    basePropsFail.put("action", "fail");
    TableDto updateDtoFail =
        returnDTO
            .toBuilder()
            .tableVersion(returnDTO.getTableLocation())
            .tableProperties(basePropsFail)
            .build();

    Assertions.assertDoesNotThrow(() -> openHouseInternalRepository.save(updateDtoSuccess));
    /* Throwing {@link CommitFailedException} in repository level, caught in service level and converted to {@link EntityConcurrentModificationException} */
    Assertions.assertThrows(
        CommitFailedException.class, () -> openHouseInternalRepository.save(updateDtoFail));
    Assertions.assertDoesNotThrow(
        () ->
            openHouseInternalRepository.deleteById(
                TableDtoPrimaryKey.builder()
                    .tableId(updateDtoSuccess.getTableId())
                    .databaseId(updateDtoSuccess.getDatabaseId())
                    .build()));
  }

  @Test
  public void testCreateTableWithTableTypeProperty() {
    for (TableType tableType : TableType.values()) {
      final String tblName = String.format("%s_%s", "tableWithType", tableType);
      TableDto tableTypeDTO =
          TABLE_DTO
              .toBuilder()
              .tableId(tblName)
              .tableType(tableType)
              .tableVersion(INITIAL_TABLE_VERSION)
              .build();

      TableDto createdDTO = openHouseInternalRepository.save(tableTypeDTO);
      Assertions.assertEquals(createdDTO.getTableId(), tblName);
      Assertions.assertEquals(createdDTO.getTableType(), tableType);

      TableDtoPrimaryKey primaryKey =
          TableDtoPrimaryKey.builder()
              .tableId(tblName)
              .databaseId(TABLE_DTO.getDatabaseId())
              .build();
      openHouseInternalRepository.deleteById(primaryKey);
      Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
    }
  }

  @Test
  void testSchemaEvolutionBasic() {
    Schema oldSchema =
        new Schema(
            required(1, "name", Types.StringType.get()),
            required(2, "count", Types.LongType.get()));
    Schema newSchema = new Schema(required(1, "name", Types.StringType.get()));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .schema(SchemaParser.toJson(oldSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    openHouseInternalRepository.save(createDto);
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));
    UpdateSchema update = table.newTransaction().updateSchema().unionByNameWith(newSchema);

    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, createDto.getTableUri()));
    TableDtoPrimaryKey primaryKey = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testSchemaEvolutionWithMismatchedFieldId() {
    Schema oldSchema =
        new Schema(
            required(1, "name", Types.StringType.get()),
            required(2, "count", Types.LongType.get()));

    // negative case: Id being different
    Schema newSchema =
        new Schema(
            required(2, "name", Types.StringType.get()),
            required(1, "count", Types.LongType.get()));

    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, "random_uri"));
  }

  @Test
  void testSchemaEvolutionStruct() {
    Types.StructType leafStructType1 =
        Types.StructType.of(
            optional(1, "leafLongCol", Types.LongType.get()),
            optional(2, "leafDoubleCol", Types.DoubleType.get()));
    Types.StructType nestedStructType1 =
        Types.StructType.of(required(3, "leafStructCol", leafStructType1));
    Schema oldSchema = new Schema(required(4, "nestedStructCol", nestedStructType1));
    Types.StructType leafStructType2 =
        Types.StructType.of(optional(1, "leafLongCol", Types.LongType.get()));
    Types.StructType nestedStructType2 =
        Types.StructType.of(required(3, "leafStructCol", leafStructType2));
    Schema newSchema = new Schema(required(4, "nestedStructCol", nestedStructType2));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .schema(SchemaParser.toJson(oldSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    openHouseInternalRepository.save(createDto);
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));
    UpdateSchema update = table.newTransaction().updateSchema().unionByNameWith(newSchema);

    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, createDto.getTableUri()));
    TableDtoPrimaryKey primaryKey = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testSchemaEvolutionList() {
    Types.StructType leafStructType1 =
        Types.StructType.of(
            optional(1, "leafLongCol", Types.LongType.get()),
            optional(2, "leafDoubleCol", Types.DoubleType.get()));
    Types.ListType nestedListType1 = Types.ListType.ofRequired(3, leafStructType1);
    Schema oldSchema = new Schema(required(4, "nestedListCol", nestedListType1));

    Types.StructType leafStructType2 =
        Types.StructType.of(optional(1, "leafLongCol", Types.LongType.get()));
    Types.ListType nestedListType2 = Types.ListType.ofRequired(3, leafStructType2);
    Schema newSchema = new Schema(required(4, "nestedListCol", nestedListType2));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .schema(SchemaParser.toJson(oldSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    openHouseInternalRepository.save(createDto);
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));
    UpdateSchema update = table.newTransaction().updateSchema().unionByNameWith(newSchema);

    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, createDto.getTableUri()));
    TableDtoPrimaryKey primaryKey = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testSchemaEvolutionMap() {
    Types.StructType leafStructType1 =
        Types.StructType.of(
            optional(1, "leafLongCol", Types.LongType.get()),
            optional(2, "leafDoubleCol", Types.DoubleType.get()));
    Types.MapType nestedMapType1 =
        Types.MapType.ofRequired(3, 4, Types.StringType.get(), leafStructType1);

    Types.StructType nestedStructType1 =
        Types.StructType.of(optional(5, "nestMapCol", nestedMapType1));
    Types.MapType nested2MapType1 =
        Types.MapType.ofRequired(6, 7, Types.StringType.get(), nestedStructType1);

    Schema oldSchema = new Schema(required(8, "nested2MapCol", nested2MapType1));

    Types.StructType leafStructType2 =
        Types.StructType.of(optional(1, "leafLongCol", Types.LongType.get()));
    Types.MapType nestedMapType2 =
        Types.MapType.ofRequired(3, 4, Types.StringType.get(), leafStructType2);

    Types.StructType nested2StructType2 =
        Types.StructType.of(optional(5, "nestMapCol", nestedMapType2));
    Types.MapType nested2MapType2 =
        Types.MapType.ofRequired(6, 7, Types.StringType.get(), nested2StructType2);

    Schema newSchema = new Schema(required(8, "nested2MapCol", nested2MapType2));

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .schema(SchemaParser.toJson(oldSchema, false))
            .timePartitioning(null)
            .clustering(null)
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    openHouseInternalRepository.save(createDto);
    Table table =
        catalog.loadTable(TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId()));

    UpdateSchema update = table.newTransaction().updateSchema().unionByNameWith(newSchema);
    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> validator.validateWriteSchema(oldSchema, newSchema, createDto.getTableUri()));
    TableDtoPrimaryKey primaryKey = getPrimaryKey(TABLE_DTO);
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  @Test
  void testReplicationFlowForPutSnapshots() {
    /* The openhouse. properties are ignored and are not validate if request is coming from replication flow */
    final String tblName = "replicaTable";
    final String replicaClusterId = "srcCluster";
    final String primaryClusterId = "destCluster";
    // existing table is of type replica_table
    Map<String, String> existingTblMap = new HashMap<>();
    existingTblMap.put("openhouse.tableType", TableType.REPLICA_TABLE.toString());
    existingTblMap.put("openhouse.clusterId", replicaClusterId);
    existingTblMap.put("openhouse.tableUri", "replicaClusterURI");
    existingTblMap.put("policies", SHARED_TABLE_POLICIES.toString());

    TableDto tableDto =
        TABLE_DTO
            .toBuilder()
            .tableId(tblName)
            .clusterId(replicaClusterId)
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableType(TableType.REPLICA_TABLE)
            .tableProperties(existingTblMap)
            .policies(SHARED_TABLE_POLICIES)
            .build();

    TableDto savedTblDto = openHouseInternalRepository.save(tableDto);
    Assertions.assertNull(savedTblDto.getPolicies().getReplication());

    Map<String, String> destTblMap = new HashMap<>();
    destTblMap.put("openhouse.tableType", TableType.PRIMARY_TABLE.toString());
    destTblMap.put("openhouse.clusterId", primaryClusterId);
    destTblMap.put("openhouse.tableUri", "primaryClusterURI");
    destTblMap.put("policies", TABLE_POLICIES.toString());

    TableDto newRequestTblDto =
        savedTblDto
            .toBuilder()
            .tableId(tblName)
            .clusterId(primaryClusterId)
            .tableType(TableType.PRIMARY_TABLE)
            .tableVersion(savedTblDto.getTableLocation())
            .tableProperties(destTblMap)
            .policies(TABLE_POLICIES)
            .build();
    // Demonstrated that the replica table updates are not blocked with table properties from
    // primary table
    TableDto newTblDTO = openHouseInternalRepository.save(newRequestTblDto);
    Assertions.assertEquals(newTblDTO.getTableId(), tblName);
    Assertions.assertEquals(newTblDTO.getTableType(), TableType.REPLICA_TABLE);
    Assertions.assertEquals(newTblDTO.getClusterId(), replicaClusterId);
    // assert that replication configs are copied as part of policies copy
    Assertions.assertEquals(
        newTblDTO.getPolicies().getReplication().getConfig().get(0).getDestination(), "CLUSTER1");

    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder().tableId(tblName).databaseId(TABLE_DTO.getDatabaseId()).build();
    openHouseInternalRepository.deleteById(primaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(primaryKey));
  }

  private TableDtoPrimaryKey getPrimaryKey(TableDto tableDto) {
    return TableDtoPrimaryKey.builder()
        .databaseId(tableDto.getDatabaseId())
        .tableId(tableDto.getTableId())
        .build();
  }

  private HouseTablePrimaryKey getHouseTablePrimaryKey(TableDtoPrimaryKey primaryKey) {
    return HouseTablePrimaryKey.builder()
        .databaseId(primaryKey.getDatabaseId())
        .tableId(primaryKey.getTableId())
        .build();
  }

  private void verifyTable(TableDto table) {
    Assertions.assertEquals(TABLE_DTO.getTableId(), table.getTableId());
    Assertions.assertEquals(TABLE_DTO.getDatabaseId(), table.getDatabaseId());
    Assertions.assertEquals(TABLE_DTO.getClusterId(), table.getClusterId());
    Assertions.assertEquals(TABLE_DTO.getTableUri(), table.getTableUri());
    Path path =
        Paths.get(
            "file:",
            storageManager.getDefaultStorage().getClient().getRootPrefix(),
            table.getDatabaseId(),
            table.getTableId() + "-" + table.getTableUUID());
    Assertions.assertEquals(TABLE_DTO.getTimePartitioning(), table.getTimePartitioning());
    Assertions.assertEquals(TABLE_DTO.getClustering(), table.getClustering());
    Assertions.assertTrue(table.getTableLocation().startsWith(path.toString()));
  }

  private void verifyTable(HouseTable table) {
    Assertions.assertEquals(TABLE_DTO.getTableId(), table.getTableId());
    Assertions.assertEquals(TABLE_DTO.getDatabaseId(), table.getDatabaseId());
    Assertions.assertEquals(TABLE_DTO.getClusterId(), table.getClusterId());
    Assertions.assertEquals(TABLE_DTO.getTableUri(), table.getTableUri());
    Path path =
        Paths.get(
            storageManager.getDefaultStorage().getClient().getRootPrefix(),
            table.getDatabaseId(),
            table.getTableId() + "-" + table.getTableUUID());
    Assertions.assertTrue(table.getTableLocation().startsWith(path.toString()));
  }
}
