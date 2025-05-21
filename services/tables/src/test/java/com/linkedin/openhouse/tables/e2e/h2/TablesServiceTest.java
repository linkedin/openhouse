package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.common.test.schema.ResourceIoHelper;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.services.TablesService;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.util.Pair;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = SpringH2Application.class)
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class TablesServiceTest {

  @Autowired TablesService tablesService;

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired StorageManager storageManager;

  @MockBean AuthorizationHandler authorizationHandler;

  @Autowired AuthorizationUtils authorizationUtils;

  @BeforeEach
  public void setup() {
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), (DatabaseDto) Mockito.any(), Mockito.any()))
        .thenReturn(true);
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), (TableDto) Mockito.any(), Mockito.any()))
        .thenReturn(true);
  }

  private void compareTables(TableDto expected, TableDto actual, TableDto previousVersion) {
    Assertions.assertEquals(expected.getClusterId(), actual.getClusterId());
    Assertions.assertEquals(expected.getDatabaseId(), actual.getDatabaseId());
    Assertions.assertEquals(expected.getTableId(), actual.getTableId());
    Assertions.assertEquals(expected.getTableUri(), actual.getTableUri());
    Path expectedPath =
        Paths.get(
            "file:",
            storageManager.getDefaultStorage().getClient().getRootPrefix(),
            actual.getDatabaseId(),
            actual.getTableId() + "-" + actual.getTableUUID());
    Assertions.assertTrue(actual.getTableLocation().startsWith(expectedPath.toString()));
    if (previousVersion != null) {
      Assertions.assertEquals(
          stripPathScheme(previousVersion.getTableLocation()), actual.getTableVersion());
    } else {
      Assertions.assertEquals(INITIAL_TABLE_VERSION, actual.getTableVersion());
    }
  }

  /**
   * Getting rid of "file:" part if needed for ease of comparison of tableLocation / tableVersion
   */
  static String stripPathScheme(String path) {
    return path.startsWith("file:") ? path.split("file:")[1] : path;
  }

  private TableDto verifyPutTableRequest(
      TableDto tableDto, TableDto previousTableDto, boolean isCreate) {
    Pair<TableDto, Boolean> putResult;
    putResult =
        tablesService.putTable(buildCreateUpdateTableRequestBody(tableDto), TEST_USER, isCreate);
    compareTables(tableDto, putResult.getFirst(), previousTableDto);
    Assertions.assertEquals(
        isCreate,
        putResult.getSecond(),
        "Table exists flag should" + (isCreate ? " not " : " ") + "be set");
    return putResult.getFirst();
  }

  private void verifyPutIdenticalTableRequest(TableDto tableDto, TableDto previousTableDto) {
    Pair<TableDto, Boolean> putResult;
    putResult = tablesService.putTable(buildCreateUpdateTableRequestBody(tableDto), null, false);
    Assertions.assertEquals(tableDto, previousTableDto);
    // If putting identical TableDto object, the updates in HTS doesn't happen and overwrite flag
    // should be false
    Assertions.assertFalse(putResult.getSecond());
  }

  private void verifyGetTableRequest(TableDto tableDto) {
    compareTables(
        tableDto,
        tablesService.getTable(tableDto.getDatabaseId(), tableDto.getTableId(), TEST_USER),
        null);
  }

  @Test
  public void testTableService() {
    // Create Table
    TableDto putResultCreate = verifyPutTableRequest(TABLE_DTO, null, true);
    TableDto putResultCreateSameDB = verifyPutTableRequest(TABLE_DTO_SAME_DB, null, true);
    TableDto putResultCreateDiffDB = verifyPutTableRequest(TABLE_DTO_DIFF_DB, null, true);

    // Read Table
    verifyGetTableRequest(TABLE_DTO);
    verifyGetTableRequest(TABLE_DTO_SAME_DB);
    verifyGetTableRequest(TABLE_DTO_DIFF_DB);

    // Attempt to update table with the exactly same object - Should have no impact on the Table
    // version
    verifyPutIdenticalTableRequest(putResultCreate, putResultCreate);
    verifyPutIdenticalTableRequest(putResultCreateSameDB, putResultCreateSameDB);
    verifyPutIdenticalTableRequest(putResultCreateSameDB, putResultCreateSameDB);

    // Update Table with valid schema change (adding a new required field)
    TableDto updatedPutResultCreate =
        verifyPutTableRequest(evolveDummySchema(putResultCreate), putResultCreate, false);
    Assertions.assertEquals(
        updatedPutResultCreate.getTableVersion(),
        stripPathScheme(putResultCreate.getTableLocation()));
    TableDto updatedPutResultCreateSameDB =
        verifyPutTableRequest(
            evolveDummySchema(putResultCreateSameDB), putResultCreateSameDB, false);
    Assertions.assertEquals(
        updatedPutResultCreateSameDB.getTableVersion(),
        stripPathScheme(putResultCreateSameDB.getTableLocation()));
    TableDto updatedPutResultCreateDiffDB =
        verifyPutTableRequest(
            evolveDummySchema(putResultCreateDiffDB), putResultCreateDiffDB, false);
    Assertions.assertEquals(
        updatedPutResultCreateDiffDB.getTableVersion(),
        stripPathScheme(putResultCreateDiffDB.getTableLocation()));

    // Delete Table
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
    tablesService.deleteTable(
        TABLE_DTO_SAME_DB.getDatabaseId(), TABLE_DTO_SAME_DB.getTableId(), TEST_USER);
    tablesService.deleteTable(
        TABLE_DTO_DIFF_DB.getDatabaseId(), TABLE_DTO_DIFF_DB.getTableId(), TEST_USER);

    // Read After Delete
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }

  @Test
  public void testInvalidSchemaEvolution() throws IOException {
    // Setup: Create the base tableDTO.
    TableDto putResultCreate = verifyPutTableRequest(TABLE_DTO, null, true);

    // case I: Invalid schema evolution by illegal type promotion
    String invalidTypePromo =
        ResourceIoHelper.getSchemaJsonFromResource("invalid_type_promote.json");
    Assertions.assertThrows(
        InvalidSchemaEvolutionException.class,
        () ->
            verifyPutTableRequest(
                decorateSchemaEvolution(putResultCreate, invalidTypePromo),
                putResultCreate,
                false));

    // Clean up
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }

  @Test
  public void testSimpleFieldUpdate() throws IOException {
    String baseSchema = ResourceIoHelper.getSchemaJsonFromResource("field_update/base.json");
    TableDto baseResult =
        verifyPutTableRequest(decorateSchemaEvolution(TABLE_DTO, baseSchema), null, true);
    String baseInt2Long =
        ResourceIoHelper.getSchemaJsonFromResource("field_update/base_int2long.json");
    TableDto updatedResult =
        verifyPutTableRequest(decorateSchemaEvolution(baseResult, baseInt2Long), baseResult, false);

    // Verify version
    Assertions.assertEquals(
        stripPathScheme(baseResult.getTableLocation()), updatedResult.getTableVersion());

    // Verify schema updated
    // Again schema's namespace might be not matching so only compare fields.
    Assertions.assertTrue(
        getSchemaFromSchemaJson(updatedResult.getSchema())
            .sameSchema(getSchemaFromSchemaJson(baseInt2Long)));

    // Clean up
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }

  @Test
  public void testTableGetFailIfDoesntExist() {
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> tablesService.getTable("DB_NOT_FOUND", "TBL_NOT_FOUND", TEST_USER));
  }

  @Test
  public void testTableCreateFailsIfAlreadyExist() {
    verifyPutTableRequest(TABLE_DTO, null, true);
    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            tablesService.putTable(buildCreateUpdateTableRequestBody(TABLE_DTO), TEST_USER, true));
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testTablePutDoesNotFailIfAlreadyExist() {
    verifyPutTableRequest(TABLE_DTO, null, true);
    Assertions.assertDoesNotThrow(
        () -> tablesService.putTable(CREATE_TABLE_REQUEST_BODY, TEST_USER, false));
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testTableDeleteThatDoesNotExist() {
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }

  @Test
  public void testTableDeleteAlreadyDeleted() {
    verifyPutTableRequest(TABLE_DTO, null, true);
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            tablesService.deleteTable(
                TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }

  @Test
  public void testTimePartitioning() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "stringId", Types.StringType.get()),
            Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone()),
            Types.NestedField.required(
                3,
                "complexType2",
                Types.StructType.of(
                    Types.NestedField.optional(
                        4, "nestedTimestampCol", Types.TimestampType.withoutZone()))));
    String schemaJson = getSchemaJsonFromSchema(schema);
    TableDto putRequest = decorateSchemaEvolution(TABLE_DTO, schemaJson);

    // Test top level column
    verifyPutTableRequest(
        decorateTimePartitionSpec(
            putRequest,
            TimePartitionSpec.builder()
                .columnName("timestampCol")
                .granularity(TimePartitionSpec.Granularity.DAY)
                .build()),
        null,
        true);
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);

    // Test nested column
    verifyPutTableRequest(
        decorateTimePartitionSpec(
            putRequest,
            TimePartitionSpec.builder()
                .columnName("complexType2.nestedTimestampCol")
                .granularity(TimePartitionSpec.Granularity.HOUR)
                .build()),
        null,
        true);
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testTimePartitioningEvolution() {
    Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "stringId", Types.StringType.get()),
            Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone()));
    TableDto putRequest = decorateSchemaEvolution(TABLE_DTO, getSchemaJsonFromSchema(schema));

    // create table succeeds
    TableDto putTableDto =
        verifyPutTableRequest(
            decorateTimePartitionSpec(
                putRequest,
                TimePartitionSpec.builder()
                    .columnName("timestampCol")
                    .granularity(TimePartitionSpec.Granularity.DAY)
                    .build()),
            null,
            true);

    // schema evolution throws error with renaming partition column
    org.apache.iceberg.Schema evolvedSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "stringId", Types.StringType.get()),
            Types.NestedField.required(2, "timestampCol2", Types.TimestampType.withoutZone()));
    Assertions.assertThrows(
        UnsupportedClientOperationException.class,
        () ->
            tablesService.putTable(
                buildCreateUpdateTableRequestBody(
                    decorateTimePartitionSpec(
                        decorateSchemaEvolution(
                            putTableDto, getSchemaJsonFromSchema(evolvedSchema)),
                        TimePartitionSpec.builder()
                            .columnName("timestampCol2")
                            .granularity(TimePartitionSpec.Granularity.DAY)
                            .build())),
                null,
                false));

    // schema evolves successfully without renaming partition column
    org.apache.iceberg.Schema evolvedSchema2 =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "stringId", Types.StringType.get()),
            Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(
                3, "newCol", Types.StringType.get())); /* newly added column has to be optional */
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.putTable(
                buildCreateUpdateTableRequestBody(
                    decorateTimePartitionSpec(
                        decorateSchemaEvolution(
                            putTableDto, getSchemaJsonFromSchema(evolvedSchema2)),
                        TimePartitionSpec.builder()
                            .columnName("timestampCol")
                            .granularity(TimePartitionSpec.Granularity.DAY)
                            .build())),
                null,
                false));

    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testUpdateAclPoliciesOnTable() {
    verifyPutTableRequest(SHARED_TABLE_DTO, null, true);
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.updateAclPolicies(
                SHARED_TABLE_DTO.getDatabaseId(),
                SHARED_TABLE_DTO.getTableId(),
                UpdateAclPoliciesRequestBody.builder()
                    .role("AclEditor")
                    .principal("DUMMY_USER")
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .build(),
                TEST_USER));

    Assertions.assertDoesNotThrow(
        () ->
            tablesService.updateAclPolicies(
                SHARED_TABLE_DTO.getDatabaseId(),
                SHARED_TABLE_DTO.getTableId(),
                UpdateAclPoliciesRequestBody.builder()
                    .role("AclEditor")
                    .principal("DUMMY_USER")
                    .operation(UpdateAclPoliciesRequestBody.Operation.REVOKE)
                    .build(),
                TEST_USER));

    tablesService.deleteTable(
        SHARED_TABLE_DTO.getDatabaseId(), SHARED_TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testUpdateAclPoliciesOnTableThatDoesNotExist() {
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            tablesService.updateAclPolicies(
                SHARED_TABLE_DTO.getDatabaseId(),
                SHARED_TABLE_DTO.getTableId(),
                UpdateAclPoliciesRequestBody.builder()
                    .role("AclEditor")
                    .principal("DUMMY_USER")
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .build(),
                TEST_USER));
  }

  @Test
  public void testGetAclPoliciesOnTable() {
    verifyPutTableRequest(TABLE_DTO, null, true);
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.getAclPolicies(
                TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));

    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testGetAclPoliciesOnTableThatDoesNotExist() {
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            tablesService.getAclPolicies(
                TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }

  @Test
  public void testGetAclPoliciesForPrincipalOnTable() {
    verifyPutTableRequest(TABLE_DTO, null, true);
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.getAclPolicies(
                TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER, TEST_USER_PRINCIPAL));

    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testGetAclPoliciesForPrincipalOnTableThatDoesNotExist() {
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            tablesService.getAclPolicies(
                TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER, TEST_USER_PRINCIPAL));
  }

  @Test
  public void testUpdateAclPoliciesOnUnSharedTable() {
    verifyPutTableRequest(TABLE_DTO, null, true);

    Assertions.assertThrows(
        UnsupportedClientOperationException.class,
        () ->
            tablesService.updateAclPolicies(
                TABLE_DTO.getDatabaseId(),
                TABLE_DTO.getTableId(),
                UpdateAclPoliciesRequestBody.builder()
                    .role("AclEditor")
                    .principal("DUMMY_USER")
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .build(),
                TEST_USER));

    Assertions.assertDoesNotThrow(
        () ->
            tablesService.updateAclPolicies(
                TABLE_DTO.getDatabaseId(),
                TABLE_DTO.getTableId(),
                UpdateAclPoliciesRequestBody.builder()
                    .role("AclEditor")
                    .principal("DUMMY_USER")
                    .operation(UpdateAclPoliciesRequestBody.Operation.REVOKE)
                    .build(),
                TEST_USER));
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testTableTypePropertyOnTable() {
    // test if default tableType is used if tableType is not defined
    TableDto tableDtoCopy = TABLE_DTO.toBuilder().tableType(null).build();
    Assertions.assertNull(tableDtoCopy.getTableType());
    TableDto putResultCreate = verifyPutTableRequest(tableDtoCopy, null, true);
    Assertions.assertEquals(putResultCreate.getTableType(), TableType.PRIMARY_TABLE);
    // Read Table
    verifyGetTableRequest(TABLE_DTO.toBuilder().tableType(TableType.PRIMARY_TABLE).build());
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);

    for (TableType tableType : TableType.values()) {
      // Create Table
      putResultCreate =
          verifyPutTableRequest(TABLE_DTO.toBuilder().tableType(tableType).build(), null, true);
      Assertions.assertEquals(putResultCreate.getTableType(), tableType);
      // Read Table
      verifyGetTableRequest(TABLE_DTO.toBuilder().tableType(tableType).build());
      tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
    }
  }

  @Test
  public void testInvalidUpdateTableTypeForExistingTable() {
    // Create Table
    TableDto putResultCreate = verifyPutTableRequest(TABLE_DTO, null, true);
    Assertions.assertEquals(putResultCreate.getTableType(), TableType.PRIMARY_TABLE);
    // update table after setting the tableType to REPLICA_TABLE and expect exception
    Assertions.assertThrows(
        UnsupportedClientOperationException.class,
        () ->
            verifyPutTableRequest(
                putResultCreate
                    .toBuilder()
                    .tableType(TableType.REPLICA_TABLE)
                    .stageCreate(false)
                    .build(),
                null,
                false));
    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testReplicaTableCreationWithUUIDFromProperties() {
    UUID expectedUUID = UUID.randomUUID();
    TableDto tableDtoCopy =
        TABLE_DTO
            .toBuilder()
            .tableProperties(
                ImmutableMap.of(
                    CatalogConstants.OPENHOUSE_UUID_KEY,
                    expectedUUID.toString(),
                    "openhouse.tableId",
                    "t",
                    "openhouse.databaseId",
                    "db",
                    "openhouse.tableLocation",
                    String.format("/tmp/db/t-%s/metadata.json", expectedUUID)))
            .tableType(TableType.REPLICA_TABLE)
            .build();
    Assertions.assertEquals(tableDtoCopy.getTableType(), TableType.REPLICA_TABLE);
    TableDto putResultCreate = verifyPutTableRequest(tableDtoCopy, null, true);
    Assertions.assertEquals(putResultCreate.getTableType(), TableType.REPLICA_TABLE);
    Assertions.assertEquals(putResultCreate.getTableUUID(), expectedUUID.toString());
    // Read Table
    Assertions.assertEquals(
        expectedUUID.toString(),
        tablesService
            .getTable(tableDtoCopy.getDatabaseId(), tableDtoCopy.getTableId(), TEST_USER)
            .getTableUUID());
    tablesService.deleteTable(tableDtoCopy.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  /** Test that if tableType is REPLICA_TABLE only system admin can update the table. required. */
  @Test
  public void testReplicaTableUpdateAsNonSystemAdmin() {
    UUID expectedUUID = UUID.randomUUID();
    TableDto tableDtoCopy =
        TABLE_DTO
            .toBuilder()
            .tableProperties(
                ImmutableMap.of(
                    CatalogConstants.OPENHOUSE_UUID_KEY,
                    expectedUUID.toString(),
                    "openhouse.tableId",
                    TABLE_DTO.getTableId(),
                    "openhouse.databaseId",
                    TABLE_DTO.getDatabaseId(),
                    "openhouse.tableLocation",
                    String.format(
                        "/tmp/%s/%s-%s/metadata.json",
                        TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), expectedUUID)))
            .tableType(TableType.REPLICA_TABLE)
            .build();
    Assertions.assertEquals(tableDtoCopy.getTableType(), TableType.REPLICA_TABLE);
    TableDto putResultCreate = verifyPutTableRequest(tableDtoCopy, null, true);
    Assertions.assertEquals(putResultCreate.getTableType(), TableType.REPLICA_TABLE);
    Assertions.assertEquals(putResultCreate.getTableUUID(), expectedUUID.toString());
    // Read Table
    Assertions.assertEquals(
        expectedUUID.toString(),
        tablesService
            .getTable(tableDtoCopy.getDatabaseId(), tableDtoCopy.getTableId(), TEST_USER)
            .getTableUUID());

    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), Mockito.any(TableDto.class), Mockito.eq(Privileges.SYSTEM_ADMIN)))
        .thenReturn(false);
    Assertions.assertThrows(
        AccessDeniedException.class,
        () ->
            tablesService.deleteTable(
                tableDtoCopy.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), Mockito.any(TableDto.class), Mockito.eq(Privileges.SYSTEM_ADMIN)))
        .thenReturn(true);
    tablesService.deleteTable(tableDtoCopy.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
  }

  @Test
  public void testPrimaryTableUpdateAsNonSystemAdmin() {
    TableDto tableDtoCopy = TABLE_DTO.toBuilder().build();
    Assertions.assertEquals(tableDtoCopy.getTableType(), TableType.PRIMARY_TABLE);
    TableDto putResultCreate = verifyPutTableRequest(tableDtoCopy, null, true);
    Assertions.assertEquals(putResultCreate.getTableType(), TableType.PRIMARY_TABLE);

    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), Mockito.any(TableDto.class), Mockito.eq(Privileges.SYSTEM_ADMIN)))
        .thenReturn(false);
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.deleteTable(
                tableDtoCopy.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }

  /** assert lock is created as policy object on createLock call */
  @Test
  public void testCreateLockOnTable() {
    TableDto tableDtoCopy = TABLE_DTO.toBuilder().build();
    verifyPutTableRequest(tableDtoCopy, null, true);
    tablesService.createLock(
        tableDtoCopy.getDatabaseId(),
        tableDtoCopy.getTableId(),
        CreateUpdateLockRequestBody.builder().locked(true).expirationInDays(4).build(),
        TEST_USER);
    TableDto result =
        tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
    Assertions.assertTrue(result.getPolicies().getLockState().isLocked());
    Assertions.assertEquals(result.getPolicies().getLockState().getExpirationInDays(), 4);
    // update lock state to false, assert that lock state does not change since create lock should
    // only set it to true
    tablesService.createLock(
        tableDtoCopy.getDatabaseId(),
        tableDtoCopy.getTableId(),
        CreateUpdateLockRequestBody.builder().locked(false).build(),
        TEST_USER);
    TableDto result1 =
        tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
    Assertions.assertTrue(result1.getPolicies().getLockState().isLocked());
    tablesService.deleteLock(tableDtoCopy.getDatabaseId(), tableDtoCopy.getTableId(), TEST_USER);
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.deleteTable(
                tableDtoCopy.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }

  /** assert lock is created as policy object on createLock call */
  @Test
  public void testDeleteLockOnTable() {
    TableDto tableDtoCopy = TABLE_DTO.toBuilder().build();
    verifyPutTableRequest(tableDtoCopy, null, true);
    tablesService.createLock(
        tableDtoCopy.getDatabaseId(),
        tableDtoCopy.getTableId(),
        CreateUpdateLockRequestBody.builder().locked(true).expirationInDays(4).build(),
        TEST_USER);
    TableDto result =
        tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
    Assertions.assertTrue(result.getPolicies().getLockState().isLocked());
    Assertions.assertEquals(result.getPolicies().getLockState().getExpirationInDays(), 4);
    // update lock state to false, assert that lock state does not change since create lock should
    // only set it to true
    tablesService.deleteLock(tableDtoCopy.getDatabaseId(), tableDtoCopy.getTableId(), TEST_USER);
    TableDto result1 =
        tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
    Assertions.assertNull(result1.getPolicies().getLockState());

    Assertions.assertDoesNotThrow(
        () ->
            tablesService.deleteTable(
                tableDtoCopy.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }

  @Test
  public void testFailedOpsOnLockTable() {
    TableDto tableDtoCopy = TABLE_DTO.toBuilder().build();
    verifyPutTableRequest(tableDtoCopy, null, true);
    tablesService.createLock(
        tableDtoCopy.getDatabaseId(),
        tableDtoCopy.getTableId(),
        CreateUpdateLockRequestBody.builder().locked(true).expirationInDays(4).build(),
        TEST_USER);
    TableDto result =
        tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
    Assertions.assertTrue(result.getPolicies().getLockState().isLocked());
    // assert delete on locked table throws UnsupportedOperationException
    UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody =
        UpdateAclPoliciesRequestBody.builder()
            .role("AclEditor")
            .principal("DUMMY_USER")
            .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
            .build();
    Assertions.assertThrows(
        UnsupportedClientOperationException.class,
        () ->
            tablesService.updateAclPolicies(
                tableDtoCopy.getDatabaseId(),
                TABLE_DTO.getTableId(),
                updateAclPoliciesRequestBody,
                TEST_USER));
    tablesService.deleteLock(tableDtoCopy.getDatabaseId(), tableDtoCopy.getTableId(), TEST_USER);
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.deleteTable(
                tableDtoCopy.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));
  }
}
