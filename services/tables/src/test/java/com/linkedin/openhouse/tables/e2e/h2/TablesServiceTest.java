package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;
import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.*;
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
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTableDto;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
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

  /**
   * Regression test for the corrupted-metadata drop path: even when metadata.json cannot be parsed
   * (loadTable would throw), deleteTable must still succeed because it goes through the HTS-only
   * findTableRefById lookup and avoids loadTable entirely.
   */
  @Test
  public void testTableDeleteSucceedsWhenMetadataJsonIsCorrupted() throws IOException {
    TableDto created = verifyPutTableRequest(TABLE_DTO, null, true);

    // tableLocation on TableDto is the metadata.json path (file:/<base>/<filename>.metadata.json).
    Path metadataPath = Paths.get(URI.create(created.getTableLocation()));
    Assertions.assertTrue(
        Files.exists(metadataPath),
        "metadata.json should exist on disk after create: " + metadataPath);

    // Corrupt the file so TableMetadataParser.read fails.
    Files.write(metadataPath, "{\"not\":\"valid iceberg metadata\"}".getBytes());

    // Sanity check: reading the table now fails because loadTable parses metadata.json.
    Assertions.assertThrows(
        Exception.class,
        () -> tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));

    // Drop should still succeed despite the corruption.
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.deleteTable(
                TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));

    // Verify HTS row is gone — a second delete should now hit the not-found path.
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

  /** Test replica table permissions: update requires SYSTEM_ADMIN, delete uses DELETE_TABLE. */
  @Test
  public void testReplicaTableUpdateAndDeletePermissions() {
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

    // Deny SYSTEM_ADMIN — update on replica should fail
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), Mockito.any(TableDto.class), Mockito.eq(Privileges.SYSTEM_ADMIN)))
        .thenReturn(false);
    Assertions.assertThrows(
        AccessDeniedException.class,
        () -> verifyPutTableRequest(tableDtoCopy, putResultCreate, false));

    // Deny DELETE_TABLE — delete on replica should fail
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), Mockito.any(TableDto.class), Mockito.eq(Privileges.DELETE_TABLE)))
        .thenReturn(false);
    Assertions.assertThrows(
        AccessDeniedException.class,
        () ->
            tablesService.deleteTable(
                tableDtoCopy.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));

    // Allow DELETE_TABLE — delete on replica should succeed (SYSTEM_ADMIN still denied)
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), Mockito.any(TableDto.class), Mockito.eq(Privileges.DELETE_TABLE)))
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

  @Test
  public void testSearchTablesWithFieldsRequiresGetTableMetadata() {
    TableDto tableDtoCopy = TABLE_DTO.toBuilder().build();
    verifyPutTableRequest(tableDtoCopy, null, true);

    // No fields requested — identifier-only search must succeed regardless of GET_TABLE_METADATA.
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(),
                Mockito.any(DatabaseDto.class),
                Mockito.eq(Privileges.GET_TABLE_METADATA)))
        .thenReturn(false);
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.searchTables(
                tableDtoCopy.getDatabaseId(), 0, 10, null, Collections.emptyList(), TEST_USER));
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.searchTables(tableDtoCopy.getDatabaseId(), 0, 10, null, null, TEST_USER));

    // Fields requested but GET_TABLE_METADATA denied — must throw.
    Assertions.assertThrows(
        AccessDeniedException.class,
        () ->
            tablesService.searchTables(
                tableDtoCopy.getDatabaseId(),
                0,
                10,
                null,
                Arrays.asList("tableLocation"),
                TEST_USER));

    // Allow GET_TABLE_METADATA — field-projection search now succeeds.
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(),
                Mockito.any(DatabaseDto.class),
                Mockito.eq(Privileges.GET_TABLE_METADATA)))
        .thenReturn(true);
    Assertions.assertDoesNotThrow(
        () ->
            tablesService.searchTables(
                tableDtoCopy.getDatabaseId(),
                0,
                10,
                null,
                Arrays.asList("tableLocation"),
                TEST_USER));

    tablesService.deleteTable(tableDtoCopy.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER);
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

  @Test
  public void testRenameTable() {
    TableDto putResultCreate = verifyPutTableRequest(TABLE_DTO, null, true);
    // Create a table in the same db for conflicts
    TableDto conflictingTable = verifyPutTableRequest(TABLE_DTO_SAME_DB, null, true);

    verifyGetTableRequest(TABLE_DTO);
    verifyGetTableRequest(TABLE_DTO_SAME_DB);

    tablesService.renameTable(
        TABLE_DTO.getDatabaseId(),
        TABLE_DTO.getTableId(),
        TABLE_DTO.getDatabaseId(),
        "renamedTable",
        TEST_USER);

    TableDto renamedTable =
        tablesService.getTable(TABLE_DTO.getDatabaseId(), "renamedTable", TEST_USER);
    Assertions.assertEquals(renamedTable.getTableId(), "renamedTable");
    Assertions.assertEquals(renamedTable.getDatabaseId(), TABLE_DTO.getDatabaseId());
    Assertions.assertEquals(renamedTable.getTableType(), TABLE_DTO.getTableType());
    Assertions.assertEquals(renamedTable.getCreationTime(), putResultCreate.getCreationTime());

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> tablesService.getTable(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId(), TEST_USER));

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            tablesService.renameTable(
                TABLE_DTO.getDatabaseId(),
                "renamedTable",
                TABLE_DTO_SAME_DB.getDatabaseId(),
                TABLE_DTO_SAME_DB.getTableId(),
                TEST_USER));

    tablesService.renameTable(
        TABLE_DTO.getDatabaseId(),
        "renamedTable",
        TABLE_DTO.getDatabaseId(),
        "secondRenamedTable",
        TEST_USER);

    TableDto secondRenamedTable =
        tablesService.getTable(TABLE_DTO.getDatabaseId(), "secondRenamedTable", TEST_USER);
    Assertions.assertEquals(secondRenamedTable.getTableId(), "secondRenamedTable");
    Assertions.assertEquals(
        secondRenamedTable.getCreationTime(), putResultCreate.getCreationTime());
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> tablesService.getTable(TABLE_DTO.getDatabaseId(), "renamedTable", TEST_USER));

    tablesService.deleteTable(TABLE_DTO.getDatabaseId(), "secondRenamedTable", TEST_USER);
    tablesService.deleteTable(
        TABLE_DTO_SAME_DB.getDatabaseId(), TABLE_DTO_SAME_DB.getTableId(), TEST_USER);
  }

  @Test
  public void testSearchSoftDeletedTables() {
    String databaseId = TABLE_DTO.getDatabaseId() + "_searchSoftDeleted_tableServiceTest";
    HouseTable softDeletedTable =
        HouseTable.builder()
            .tableId(TABLE_DTO.getTableId())
            .databaseId(databaseId)
            .tableLocation(TABLE_DTO.getTableLocation())
            .tableVersion(TABLE_DTO.getTableVersion())
            .tableCreator(TABLE_DTO.getTableCreator())
            .lastModifiedTime(TABLE_DTO.getLastModifiedTime())
            .creationTime(TABLE_DTO.getCreationTime())
            .deletedAtMs(System.currentTimeMillis())
            .purgeAfterMs(System.currentTimeMillis())
            .build();

    HouseTable softDeletedTable2 =
        HouseTable.builder()
            .databaseId(databaseId + "_2")
            .deletedAtMs(System.currentTimeMillis())
            .build();

    HouseTablesH2Repository.softDeletedTables.put(
        SoftDeletedTablePrimaryKey.builder()
            .databaseId(databaseId)
            .tableId(TABLE_DTO.getTableId())
            .deletedAtMs(softDeletedTable.getDeletedAtMs())
            .build(),
        softDeletedTable);

    HouseTablesH2Repository.softDeletedTables.put(
        SoftDeletedTablePrimaryKey.builder()
            .databaseId(databaseId + "_2")
            .tableId(TABLE_DTO.getTableId() + "_2")
            .deletedAtMs(softDeletedTable2.getDeletedAtMs())
            .build(),
        softDeletedTable2);

    Page<SoftDeletedTableDto> result =
        tablesService.searchSoftDeletedTables(databaseId, null, 0, 10, null);

    // Verify
    Assertions.assertNotNull(result);
    Assertions.assertEquals(1, result.getContent().size());
    Assertions.assertEquals(TABLE_DTO.getTableId(), result.getContent().get(0).getTableId());
    Assertions.assertEquals(databaseId, result.getContent().get(0).getDatabaseId());
  }

  @Test
  public void testPurgeSoftDeletedTables() {
    String purgeDbId = TABLE_DTO.getDatabaseId() + "_purge";
    HouseTable softDeletedTable =
        HouseTable.builder()
            .tableId(TABLE_DTO.getTableId())
            .databaseId(purgeDbId)
            .tableLocation(TABLE_DTO.getTableLocation())
            .tableVersion(TABLE_DTO.getTableVersion())
            .tableCreator(TABLE_DTO.getTableCreator())
            .lastModifiedTime(TABLE_DTO.getLastModifiedTime())
            .creationTime(TABLE_DTO.getCreationTime())
            .deletedAtMs(System.currentTimeMillis())
            .purgeAfterMs(System.currentTimeMillis())
            .build();

    HouseTablesH2Repository.softDeletedTables.put(
        SoftDeletedTablePrimaryKey.builder()
            .databaseId(purgeDbId)
            .tableId(TABLE_DTO.getTableId())
            .deletedAtMs(softDeletedTable.getDeletedAtMs())
            .build(),
        softDeletedTable);

    Page<SoftDeletedTableDto> result =
        tablesService.searchSoftDeletedTables(purgeDbId, null, 0, 10, null);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(1, result.getContent().size());
    Assertions.assertEquals(TABLE_DTO.getTableId(), result.getContent().get(0).getTableId());
    Assertions.assertEquals(purgeDbId, result.getContent().get(0).getDatabaseId());

    long purgeAfterMs = System.currentTimeMillis() + 1000;
    // Purge soft deleted table
    tablesService.purgeSoftDeletedTables(
        purgeDbId, TABLE_DTO.getTableId(), purgeAfterMs, TEST_USER);
    result = tablesService.searchSoftDeletedTables(purgeDbId, null, 0, 10, null);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(0, result.getContent().size());
  }

  @Test
  public void testRestoreTable() {
    String restoreDbId = TABLE_DTO.getDatabaseId() + "_restore";
    long deletedAtMs = System.currentTimeMillis();

    HouseTable softDeletedTable =
        HouseTable.builder()
            .tableId(TABLE_DTO.getTableId())
            .databaseId(restoreDbId)
            .tableLocation(TABLE_DTO.getTableLocation())
            .tableVersion(TABLE_DTO.getTableVersion())
            .tableCreator(TABLE_DTO.getTableCreator())
            .lastModifiedTime(TABLE_DTO.getLastModifiedTime())
            .creationTime(TABLE_DTO.getCreationTime())
            .deletedAtMs(deletedAtMs)
            .purgeAfterMs(System.currentTimeMillis() + 86400000) // 1 day from now
            .build();

    HouseTablesH2Repository.softDeletedTables.put(
        SoftDeletedTablePrimaryKey.builder()
            .databaseId(restoreDbId)
            .tableId(TABLE_DTO.getTableId())
            .deletedAtMs(deletedAtMs)
            .build(),
        softDeletedTable);

    Page<SoftDeletedTableDto> result =
        tablesService.searchSoftDeletedTables(restoreDbId, null, 0, 10, null);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(1, result.getContent().size());
    Assertions.assertEquals(TABLE_DTO.getTableId(), result.getContent().get(0).getTableId());
    Assertions.assertEquals(restoreDbId, result.getContent().get(0).getDatabaseId());

    // Restore the table
    tablesService.restoreTable(restoreDbId, TABLE_DTO.getTableId(), deletedAtMs, TEST_USER);

    // Validate the table is restored by checking it's no longer in soft deleted tables
    result = tablesService.searchSoftDeletedTables(restoreDbId, null, 0, 10, null);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(0, result.getContent().size());
  }

  @Test
  public void testRestoreTableNotFound() {
    String nonExistentDbId = TABLE_DTO.getDatabaseId() + "_nonexistent";
    long deletedAtMs = System.currentTimeMillis();

    // Try to restore a table that doesn't exist in soft deleted tables
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            tablesService.restoreTable(
                nonExistentDbId, "nonexistent_table", deletedAtMs, TEST_USER));
  }

  // ---------------------------------------------------------------------------------------------
  // Shared-key occupancy and wrong-type guards on the table service
  // ---------------------------------------------------------------------------------------------

  /**
   * Raw pointer rows must be seeded through the pointer repository directly, because a VIEW row is
   * invisible to the table API and therefore cannot be created — or cleaned up — through it. Every
   * seeded key is removed in {@link #deleteSeededPointers()}.
   */
  @Autowired HouseTableRepository houseTablesRepository;

  private final List<HouseTablePrimaryKey> seededPointerKeys = new ArrayList<>();

  private final List<Path> seededDirectories = new ArrayList<>();

  @AfterEach
  public void deleteSeededPointers() throws IOException {
    for (HouseTablePrimaryKey key : seededPointerKeys) {
      try {
        houseTablesRepository.deleteById(key);
      } catch (Exception e) {
        // Best effort: cleanup must not mask the real assertion failure.
      }
    }
    seededPointerKeys.clear();
    for (Path directory : seededDirectories) {
      try (Stream<Path> paths = Files.walk(directory)) {
        paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      } catch (Exception e) {
        // Best effort.
      }
    }
    seededDirectories.clear();
  }

  private static final String OCCUPANCY_DB = "entity_type_occupancy_db";

  /**
   * Seeds a raw pointer whose {@code tableLocation} points at a real on-disk metadata.json under
   * the storage root, so a purge attempt would be observable as a missing file.
   */
  private HouseTablePrimaryKey seedRawPointer(String databaseId, String tableId, String entityType)
      throws IOException {
    Path tableDirectory =
        Paths.get(
            storageManager.getDefaultStorage().getClient().getRootPrefix(),
            databaseId,
            tableId + "-" + UUID.randomUUID());
    Files.createDirectories(tableDirectory);
    Path metadataFile = tableDirectory.resolve("00001-seeded.metadata.json");
    Files.write(metadataFile, "{\"not\":\"parsed by these tests\"}".getBytes());
    seededDirectories.add(tableDirectory);

    houseTablesRepository.save(
        HouseTable.builder()
            .databaseId(databaseId)
            .tableId(tableId)
            .clusterId(TABLE_DTO.getClusterId())
            .tableUri(String.format("%s.%s.%s", TABLE_DTO.getClusterId(), databaseId, tableId))
            .tableUUID(UUID.randomUUID().toString())
            .tableLocation(metadataFile.toString())
            .tableVersion(INITIAL_TABLE_VERSION)
            .storageType(storageManager.getDefaultStorage().getType().getValue())
            .entityType(entityType)
            .build());

    HouseTablePrimaryKey key =
        HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(tableId).build();
    seededPointerKeys.add(key);
    return key;
  }

  private HouseTable reloadPointer(HouseTablePrimaryKey key) {
    return houseTablesRepository
        .findById(key)
        .orElseThrow(
            () ->
                new AssertionError(
                    "Raw pointer " + key.getDatabaseId() + "." + key.getTableId() + " is gone"));
  }

  /** Snapshot of every metadata.json under a database's storage root. */
  private Set<String> metadataFilesUnder(String databaseId) throws IOException {
    Path databaseRoot =
        Paths.get(storageManager.getDefaultStorage().getClient().getRootPrefix(), databaseId);
    if (!Files.exists(databaseRoot)) {
      return Collections.emptySet();
    }
    try (Stream<Path> paths = Files.walk(databaseRoot)) {
      return paths
          .filter(p -> p.toString().endsWith(".metadata.json"))
          .map(Path::toString)
          .collect(Collectors.toSet());
    }
  }

  /**
   * CREATE TABLE at a view-occupied name must be rejected with an accurate typed 409 BEFORE any
   * authorization decision and BEFORE any metadata file is written.
   *
   * <p>This is the test that fails against a naive design that only guards the table {@code
   * doRefresh}: with that design the typed load reports "no table", so the create proceeds through
   * authorization, allocates a location, and writes a candidate metadata.json — leaving an orphaned
   * file and surfacing a misleading concurrency 409 from the HTS publish boundary. The load-bearing
   * assertions here are therefore the unchanged metadata-file set and the never-authorized
   * verification, not the exception type.
   */
  @Test
  public void testCreateTableRejectsViewOccupancyBeforeAuthorizationOrMetadata()
      throws IOException {
    HouseTablePrimaryKey viewKey = seedRawPointer(OCCUPANCY_DB, "occupied_by_view", "VIEW");
    HouseTable before = reloadPointer(viewKey);
    Set<String> metadataFilesBefore = metadataFilesUnder(OCCUPANCY_DB);

    // Nothing authorizes during raw-repository seeding, so a plain never() verification is enough.
    Mockito.verify(authorizationHandler, Mockito.never())
        .checkAccessDecision(Mockito.any(), (DatabaseDto) Mockito.any(), Mockito.any());

    TableDto createDto =
        TABLE_DTO
            .toBuilder()
            .databaseId(OCCUPANCY_DB)
            .tableId("occupied_by_view")
            .tableUri(TABLE_DTO.getClusterId() + "." + OCCUPANCY_DB + ".occupied_by_view")
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();

    AlreadyExistsException thrown =
        Assertions.assertThrows(
            AlreadyExistsException.class,
            () ->
                tablesService.putTable(
                    buildCreateUpdateTableRequestBody(createDto), TEST_USER, true));
    Assertions.assertEquals(
        "Table name " + OCCUPANCY_DB + ".occupied_by_view is occupied by a view",
        thrown.getMessage());

    HouseTable after = reloadPointer(viewKey);
    Assertions.assertEquals("VIEW", after.getEntityType());
    Assertions.assertEquals(before.getEntityType(), after.getEntityType());
    Assertions.assertEquals(before.getTableLocation(), after.getTableLocation());
    Assertions.assertEquals(before.getTableUUID(), after.getTableUUID());

    Assertions.assertEquals(
        metadataFilesBefore,
        metadataFilesUnder(OCCUPANCY_DB),
        "A rejected create must not write a candidate metadata.json");

    Mockito.verify(authorizationHandler, Mockito.never())
        .checkAccessDecision(Mockito.any(), (DatabaseDto) Mockito.any(), Mockito.any());
    Mockito.verify(authorizationHandler, Mockito.never())
        .checkAccessDecision(Mockito.any(), (TableDto) Mockito.any(), Mockito.any());
  }

  /** A view (any spelling) or unknown type can never be dropped through the table API. */
  @ParameterizedTest
  @ValueSource(strings = {"VIEW", "view", "ViEw", "UNKNOWN"})
  public void testDeleteTableRejectsNonTableAndPreservesPointer(String entityType)
      throws IOException {
    HouseTablePrimaryKey key = seedRawPointer(OCCUPANCY_DB, "no_drop_target", entityType);
    HouseTable before = reloadPointer(key);
    Path metadataFile = Paths.get(before.getTableLocation());
    Assertions.assertTrue(Files.exists(metadataFile));

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> tablesService.deleteTable(OCCUPANCY_DB, "no_drop_target", TEST_USER));

    HouseTable after = reloadPointer(key);
    Assertions.assertEquals(entityType, after.getEntityType());
    Assertions.assertEquals(before.getTableLocation(), after.getTableLocation());
    Assertions.assertTrue(
        Files.exists(metadataFile), "A rejected drop must not purge the object's storage prefix");
  }

  /** A wrong-type rename SOURCE reads as "no such table" and nothing is created or moved. */
  @ParameterizedTest
  @ValueSource(strings = {"VIEW", "view", "ViEw", "UNKNOWN"})
  public void testRenameTableRejectsNonTableSourceAndPreservesPointer(String entityType)
      throws IOException {
    HouseTablePrimaryKey sourceKey = seedRawPointer(OCCUPANCY_DB, "no_rename_source", entityType);
    HouseTable before = reloadPointer(sourceKey);

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            tablesService.renameTable(
                OCCUPANCY_DB, "no_rename_source", OCCUPANCY_DB, "renamed_target", TEST_USER));

    HouseTable after = reloadPointer(sourceKey);
    Assertions.assertEquals(entityType, after.getEntityType());
    Assertions.assertEquals(before.getTableLocation(), after.getTableLocation());
    Assertions.assertFalse(
        houseTablesRepository
            .findById(
                HouseTablePrimaryKey.builder()
                    .databaseId(OCCUPANCY_DB)
                    .tableId("renamed_target")
                    .build())
            .isPresent(),
        "A rejected rename must not create the destination pointer");
  }

  /**
   * Renaming a real table onto a view-occupied destination must fail with an accurate typed 409
   * BEFORE authorization, before any pointer mutation, and before any metadata is written.
   *
   * <p>The exception TYPE alone proves nothing here: the shared primary key would eventually raise
   * the same {@link AlreadyExistsException} from the storage layer. What kills that accidental
   * fallback is (a) the byte-identical destination pointer, (b) the unchanged source {@code
   * *.metadata.json} file set — the fallback only triggers after a candidate file is written — and
   * (c) the verification that no authorization decision was ever taken.
   */
  @Test
  public void testRenameTableRejectsViewDestinationBeforeAuthorizationOrMetadata()
      throws IOException {
    TableDto sourceDto =
        TABLE_DTO
            .toBuilder()
            .databaseId(OCCUPANCY_DB)
            .tableId("rename_source")
            .tableUri(TABLE_DTO.getClusterId() + "." + OCCUPANCY_DB + ".rename_source")
            .tableVersion(INITIAL_TABLE_VERSION)
            .build();
    TableDto created = verifyPutTableRequest(sourceDto, null, true);
    HouseTablePrimaryKey sourceKey =
        HouseTablePrimaryKey.builder().databaseId(OCCUPANCY_DB).tableId("rename_source").build();
    Path sourceDirectory = Paths.get(URI.create(created.getTableLocation())).getParent();
    // Register the real source table for teardown immediately after creation, so it cannot survive
    // the class if any assertion below fails. @AfterEach removes both the pointer and the files.
    seededPointerKeys.add(sourceKey);
    seededDirectories.add(sourceDirectory);

    HouseTablePrimaryKey destinationKey = seedRawPointer(OCCUPANCY_DB, "rename_dest_view", "VIEW");

    HouseTable sourceBefore = reloadPointer(sourceKey);
    HouseTable destinationBefore = reloadPointer(destinationKey);
    Set<String> sourceMetadataBefore = metadataFilesIn(sourceDirectory);

    // Source setup legitimately authorizes; clear the recorded invocations (but keep the stubs) so
    // the never() verification below is about the rename only.
    Mockito.clearInvocations(authorizationHandler);

    AlreadyExistsException thrown =
        Assertions.assertThrows(
            AlreadyExistsException.class,
            () ->
                tablesService.renameTable(
                    OCCUPANCY_DB, "rename_source", OCCUPANCY_DB, "rename_dest_view", TEST_USER));
    Assertions.assertEquals(
        "Table name " + OCCUPANCY_DB + ".rename_dest_view is occupied by a view",
        thrown.getMessage());

    HouseTable destinationAfter = reloadPointer(destinationKey);
    Assertions.assertEquals("VIEW", destinationAfter.getEntityType());
    Assertions.assertEquals(
        destinationBefore.getTableLocation(), destinationAfter.getTableLocation());
    Assertions.assertEquals(destinationBefore.getTableUUID(), destinationAfter.getTableUUID());

    HouseTable sourceAfter = reloadPointer(sourceKey);
    Assertions.assertEquals(sourceBefore.getTableLocation(), sourceAfter.getTableLocation());
    Assertions.assertEquals(sourceBefore.getEntityType(), sourceAfter.getEntityType());

    Assertions.assertEquals(
        sourceMetadataBefore,
        metadataFilesIn(sourceDirectory),
        "A rejected rename must not write a new source metadata.json");

    Mockito.verify(authorizationHandler, Mockito.never())
        .checkAccessDecision(Mockito.any(), (DatabaseDto) Mockito.any(), Mockito.any());
    Mockito.verify(authorizationHandler, Mockito.never())
        .checkAccessDecision(Mockito.any(), (TableDto) Mockito.any(), Mockito.any());
    // No explicit deleteTable here: sourceKey/sourceDirectory are registered for @AfterEach
    // teardown above, so cleanup happens even if an assertion between here and there fails.
  }

  /**
   * Service-layer complement to the HTTP 404 tests: reading a view (any spelling) or an unknown
   * discriminator through the table API is indistinguishable from "no such table", and the read
   * itself must not disturb the pointer.
   */
  @ParameterizedTest
  @ValueSource(strings = {"VIEW", "view", "ViEw", "UNKNOWN"})
  public void testGetTableRejectsNonTableAndPreservesPointer(String entityType) throws IOException {
    HouseTablePrimaryKey key = seedRawPointer(OCCUPANCY_DB, "read_as_table", entityType);
    HouseTable before = reloadPointer(key);

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> tablesService.getTable(OCCUPANCY_DB, "read_as_table", TEST_USER));

    HouseTable after = reloadPointer(key);
    Assertions.assertEquals(entityType, after.getEntityType());
    Assertions.assertEquals(before.getTableLocation(), after.getTableLocation());
    Assertions.assertTrue(
        Files.exists(Paths.get(before.getTableLocation())),
        "A rejected read must not touch the object's files");
  }

  private Set<String> metadataFilesIn(Path directory) throws IOException {
    if (directory == null || !Files.exists(directory)) {
      return Collections.emptySet();
    }
    try (Stream<Path> paths = Files.walk(directory)) {
      return paths
          .filter(p -> p.toString().endsWith(".metadata.json"))
          .map(Path::toString)
          .collect(Collectors.toSet());
    }
  }
}
