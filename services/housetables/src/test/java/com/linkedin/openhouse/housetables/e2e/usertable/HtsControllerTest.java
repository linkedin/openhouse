package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static com.linkedin.openhouse.housetables.model.TestHtsApiConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.request.CreateUpdateEntityRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.dto.mapper.SoftDeletedUserTablesMapper;
import com.linkedin.openhouse.housetables.e2e.fixture.UserTableRawSeeder;
import com.linkedin.openhouse.housetables.e2e.fixture.UserTableStoreCleaner;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.TestHtsApiConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.persistence.PersistenceException;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.util.MultiValueMap;
import org.springframework.util.MultiValueMapAdapter;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@AutoConfigureMockMvc
public class HtsControllerTest {

  @Autowired HtsRepository<UserTableRow, UserTableRowPrimaryKey> htsRepository;

  @Autowired UserTableStoreCleaner userTableStoreCleaner;

  @Autowired UserTableRawSeeder userTableRawSeeder;

  @Autowired SoftDeletedUserTableHtsJdbcRepository softDeletedHtsJdbcRepository;

  @Autowired MockMvc mvc;

  @Autowired SoftDeletedUserTablesMapper softDeletedTableMapper;

  @Autowired DataSource dataSource;

  /** Lets a dependency failure be injected where a real one would be raised. */
  @SpyBean UserTableHtsJdbcRepository htsJdbcRepository;

  @BeforeEach
  public void setup() {
    // TODO: Use rest API to create the table and test the find/delete user table again.
    // For now manually create the user table upfront.
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    userTableRawSeeder.seedLegacyRow(testUserTableRow);
  }

  @AfterEach
  public void tearDown() {
    Mockito.reset(htsJdbcRepository);
    // The JPA cleanup loads every row, so a planted non-canonical spelling must go first;
    // otherwise converter hydration during teardown poisons the rest of the class.
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE entity_type NOT IN ('TABLE', 'VIEW')");
    userTableStoreCleaner.clear();
    softDeletedHtsJdbcRepository.deleteAll();
  }

  @Test
  public void testFindAllFromDbWithTableId() throws Exception {
    // TODO: Use rest API to create the table
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_1.get_userTableRow());

    Map<String, List<String>> paramsInternal = new HashMap<>();
    paramsInternal.put("databaseId", Collections.singletonList(TEST_DB_ID));
    paramsInternal.put("tableId", Collections.singletonList("test_table0"));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(paramsInternal);
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/query")
                .params(params)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    GetAllEntityResponseBody.builder()
                        .results(
                            Arrays.asList(TEST_USER_TABLE).stream()
                                .map(
                                    userTable ->
                                        userTable
                                            .toBuilder()
                                            .tableVersion(userTable.getMetadataLocation())
                                            .build())
                                .collect(Collectors.toList()))
                        .build()
                        .toJson()));
  }

  @Test
  public void testFindAllFromDbWithTablePattern() throws Exception {
    // TODO: Use rest API to create the table
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_1.get_userTableRow());

    Map<String, List<String>> paramsInternal = new HashMap<>();
    paramsInternal.put("databaseId", Collections.singletonList(TEST_DB_ID));
    paramsInternal.put("tableId", Collections.singletonList("test_table%"));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(paramsInternal);
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/query")
                .params(params)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    GetAllEntityResponseBody.builder()
                        .results(
                            Arrays.asList(
                                    TEST_USER_TABLE,
                                    TEST_TUPLE_1_0.get_userTable(),
                                    TEST_TUPLE_2_0.get_userTable())
                                .stream()
                                .map(
                                    userTable ->
                                        userTable
                                            .toBuilder()
                                            .tableVersion(userTable.getMetadataLocation())
                                            .build())
                                .collect(Collectors.toList()))
                        .build()
                        .toJson()));
  }

  @Test
  /** Using LIST endpoint to test a partially filled user table object as request body */
  public void testFindAllFromDb() throws Exception {
    // TODO: Use rest API to create the table
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_1.get_userTableRow());

    // Inserted two tables in db0, combining the one in the setup method there should be 3
    Map<String, List<String>> paramsInternal = new HashMap<>();
    paramsInternal.put("databaseId", Collections.singletonList(TEST_DB_ID));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(paramsInternal);
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/query")
                .params(params)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    GetAllEntityResponseBody.builder()
                        .results(
                            Arrays.asList(
                                    TEST_USER_TABLE,
                                    TEST_TUPLE_1_0.get_userTable(),
                                    TEST_TUPLE_2_0.get_userTable())
                                .stream()
                                .map(
                                    userTable ->
                                        userTable
                                            .toBuilder()
                                            .tableVersion(userTable.getMetadataLocation())
                                            .build())
                                .collect(Collectors.toList()))
                        .build()
                        .toJson()));
  }

  /** Using LIST endpoint to test an empty user table object request body */
  @Test
  public void testFindAllDatabases() throws Exception {
    // TODO: Use rest API to create the table
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_1.get_userTableRow());

    mvc.perform(MockMvcRequestBuilders.get("/hts/tables/query").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    GetAllEntityResponseBody.builder()
                        .results(
                            Arrays.asList(
                                    UserTable.builder().databaseId("test_db0").build(),
                                    UserTable.builder().databaseId("test_db1").build())
                                .stream()
                                .map(
                                    userTable ->
                                        userTable
                                            .toBuilder()
                                            .tableVersion(userTable.getMetadataLocation())
                                            .build())
                                .collect(Collectors.toList()))
                        .build()
                        .toJson()));
  }

  @Test
  public void testFindUserTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("tableId", TEST_TABLE_ID)
                .param("databaseId", TEST_DB_ID)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            jsonPath(
                "$.entity.tableId",
                is(
                    equalTo(
                        TestHtsApiConstants.TEST_GET_USER_TABLE_RESPONSE_BODY
                            .getEntity()
                            .getTableId()))))
        .andExpect(
            jsonPath(
                "$.entity.databaseId",
                is(
                    equalTo(
                        TestHtsApiConstants.TEST_GET_USER_TABLE_RESPONSE_BODY
                            .getEntity()
                            .getDatabaseId()))))
        .andExpect(
            jsonPath(
                "$.entity.metadataLocation",
                is(
                    equalTo(
                        TestHtsApiConstants.TEST_GET_USER_TABLE_RESPONSE_BODY
                            .getEntity()
                            .getMetadataLocation()))))
        .andExpect(
            jsonPath(
                "$.entity.storageType",
                is(
                    equalTo(
                        TestHtsApiConstants.TEST_GET_USER_TABLE_RESPONSE_BODY
                            .getEntity()
                            .getStorageType()))));
  }

  @Test
  public void testUserTableNotFound() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("tableId", NON_EXISTED_TABLE)
                .param("databaseId", NON_EXISTED_DB)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))))
        .andExpect(jsonPath("$.error", is(equalTo("Not Found"))))
        .andExpect(
            jsonPath(
                "$.message",
                is(
                    equalTo(
                        NOT_FOUND_ERROR_MSG_TEMPLATE
                            .replace("$db", NON_EXISTED_DB)
                            .replace("$tbl", NON_EXISTED_TABLE)))));
  }

  @Test
  public void testDeleteUserTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/tables")
                .param("databaseId", TEST_DB_ID)
                .param("tableId", TEST_TABLE_ID))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));
  }

  @Test
  public void testDeleteNonExistedUserTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/tables")
                .param("databaseId", NON_EXISTED_DB)
                .param("tableId", NON_EXISTED_TABLE))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))))
        .andExpect(jsonPath("$.error", is(equalTo("Not Found"))))
        .andExpect(jsonPath("$.cause", notNullValue()))
        .andExpect(
            jsonPath(
                "$.message",
                is(
                    equalTo(
                        NOT_FOUND_ERROR_MSG_TEMPLATE
                            .replace("$db", NON_EXISTED_DB)
                            .replace("$tbl", NON_EXISTED_TABLE)))));
  }

  @Test
  public void testPutUserTable() throws Exception {
    // Ensure the target table to be created, testTuple2_0, not existed yet.
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("tableId", TEST_TUPLE_2_0.getTableId())
                .param("databaseId", TEST_TUPLE_2_0.getDatabaseId())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());

    // Create the table and return correct status code
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(TEST_TUPLE_2_0.get_userTable())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.tableId", is(equalTo(TEST_TUPLE_2_0.getTableId()))))
        .andExpect(jsonPath("$.entity.databaseId", is(equalTo(TEST_TUPLE_2_0.getDatabaseId()))))
        .andExpect(jsonPath("$.entity.metadataLocation", is(equalTo(TEST_TUPLE_2_0.getTableLoc()))))
        .andExpect(jsonPath("$.entity.storageType", is(equalTo(TEST_TUPLE_1_0.getStorageType()))));

    // Update the same table and returning the updated object.
    String atVersion = TEST_TUPLE_2_0.get_userTable().getMetadataLocation();
    String modifiedMetaLoc = TEST_TUPLE_2_0.get_userTable().getMetadataLocation() + "change";

    UserTable modified2_0 =
        UserTable.builder()
            .tableId(TEST_TUPLE_2_0.get_userTable().getTableId())
            .databaseId(TEST_TUPLE_2_0.get_userTable().getDatabaseId())
            .tableVersion(atVersion)
            .metadataLocation(modifiedMetaLoc)
            .build();
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(modified2_0)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.tableId", is(equalTo(TEST_TUPLE_2_0.getTableId()))))
        .andExpect(jsonPath("$.entity.databaseId", is(equalTo(TEST_TUPLE_2_0.getDatabaseId()))))
        .andExpect(jsonPath("$.entity.metadataLocation", is(modifiedMetaLoc)))
        .andExpect(jsonPath("$.entity.storageType", is(equalTo(TEST_TUPLE_1_0.getStorageType()))));
  }

  @Test
  public void testConflictAtTargetVersion() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(TEST_TUPLE_2_0.get_userTable())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON));

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(
                            TEST_TUPLE_2_0
                                .get_userTable()
                                .toBuilder()
                                .tableVersion("file:/older/version")
                                .metadataLocation("file:/next/version")
                                .build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isConflict())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON));
  }

  @Test
  public void testInvalidParamsUserTable() throws Exception {
    UserTable modified2_0 =
        UserTable.builder()
            .tableId(null)
            .databaseId(TEST_TUPLE_2_0.get_userTable().getDatabaseId())
            .build();
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(modified2_0)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(jsonPath("$.message", containsString("tableId cannot be empty")))
        .andExpect(jsonPath("$.message", containsString("metadataLocation cannot be empty")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))));

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", TEST_DB_ID)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testPutUserTableWithNullStorageType() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("tableId", TEST_TUPLE_1_0.getTableId())
                .param("databaseId", TEST_TUPLE_1_0.getDatabaseId())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());

    // Create the table and return correct status code
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(
                            TEST_TUPLE_1_0.get_userTable().toBuilder().storageType(null).build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.tableId", is(equalTo(TEST_TUPLE_1_0.getTableId()))))
        .andExpect(jsonPath("$.entity.databaseId", is(equalTo(TEST_TUPLE_1_0.getDatabaseId()))))
        .andExpect(jsonPath("$.entity.metadataLocation", is(equalTo(TEST_TUPLE_1_0.getTableLoc()))))
        .andExpect(jsonPath("$.entity.storageType", is(equalTo(TEST_DEFAULT_STORAGE_TYPE))));
  }

  @Test
  public void testRenameUserTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", TEST_DB_ID)
                .param("fromTableId", TEST_TABLE_ID)
                .param("toDatabaseId", TEST_DB_ID)
                .param("toTableId", TEST_TABLE_ID + "_renamed")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));
  }

  @Test
  public void testRenameUserTableFails() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", TEST_DB_ID)
                .param("fromTableId", NON_EXISTED_TABLE)
                .param("toDatabaseId", TEST_DB_ID)
                .param("toTableId", TEST_TABLE_ID + "_renamed")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isNotFound());

    // Currently we don't support renaming a table across databases.
    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", TEST_DB_ID)
                .param("fromTableId", TEST_TABLE_ID)
                .param("toDatabaseId", TEST_DB_ID + "_renamed")
                .param("toTableId", TEST_TABLE_ID + "_renamed")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isBadRequest());

    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", TEST_DB_ID)
                .param("fromTableId", TEST_TABLE_ID)
                .param("toDatabaseId", TEST_DB_ID)
                .param("toTableId", TEST_TABLE_ID)
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isBadRequest());

    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_0.get_userTableRow());
    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", TEST_DB_ID)
                .param("fromTableId", TEST_TABLE_ID)
                .param("toDatabaseId", TEST_TUPLE_2_0.getDatabaseId())
                .param("toTableId", TEST_TUPLE_2_0.getTableId())
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isConflict());
  }

  @Test
  public void testQuerySoftDeletedTables() throws Exception {
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_0.get_userTableRow());
    // Soft delete the table
    mvc.perform(
            MockMvcRequestBuilders.delete("/v1/hts/tables")
                .param("databaseId", TEST_TUPLE_1_0.getDatabaseId())
                .param("tableId", TEST_TUPLE_1_0.getTableId())
                .param("isSoftDelete", "true"))
        .andExpect(status().isNoContent());

    Map<String, List<String>> paramsInternal = new HashMap<>();
    paramsInternal.put("databaseId", Collections.singletonList(TEST_DB_ID));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(paramsInternal);

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/querySoftDeleted")
                .params(params)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.pageResults.content", hasSize(1)))
        .andExpect(jsonPath("$.pageResults.content[0].databaseId", is(TEST_DB_ID)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is(TEST_TUPLE_1_0.getTableId())))
        .andExpect(jsonPath("$.pageResults.content[0].deletedAtMs", notNullValue()))
        .andExpect(jsonPath("$.pageResults.content[0].purgeAfterMs", notNullValue()));
  }

  @Test
  public void testQuerySoftDeletedTablesByTableId() throws Exception {
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_0.get_userTableRow());

    mvc.perform(
            MockMvcRequestBuilders.delete("/v1/hts/tables")
                .param("databaseId", TEST_TUPLE_1_0.getDatabaseId())
                .param("tableId", TEST_TUPLE_1_0.getTableId())
                .param("isSoftDelete", "true"))
        .andExpect(status().isNoContent());

    mvc.perform(
            MockMvcRequestBuilders.delete("/v1/hts/tables")
                .param("databaseId", TEST_TUPLE_2_0.getDatabaseId())
                .param("tableId", TEST_TUPLE_2_0.getTableId())
                .param("isSoftDelete", "true"))
        .andExpect(status().isNoContent());

    Map<String, List<String>> paramsInternal = new HashMap<>();
    paramsInternal.put("databaseId", Collections.singletonList(TEST_TUPLE_1_0.getDatabaseId()));
    paramsInternal.put("tableId", Collections.singletonList(TEST_TUPLE_1_0.getTableId()));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(paramsInternal);

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/querySoftDeleted")
                .params(params)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.content", hasSize(1)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is(TEST_TUPLE_1_0.getTableId())));
  }

  @Test
  public void testQuerySoftDeletedTablesByPurgeAfterMs() throws Exception {
    String testTableId = "testQuerySoftDeletedTable";
    userTableRawSeeder.seedLegacyRow(
        TEST_TUPLE_1_0.get_userTableRow().toBuilder().tableId(testTableId).build());

    // First, soft delete a table
    mvc.perform(
            MockMvcRequestBuilders.delete("/v1/hts/tables")
                .param("databaseId", TEST_DB_ID)
                .param("tableId", testTableId)
                .param("isSoftDelete", "true"))
        .andExpect(status().isNoContent());

    // Query without purgeAfterMs (should return the soft deleted table)
    Map<String, List<String>> paramsInternal = new HashMap<>();
    paramsInternal.put("databaseId", Collections.singletonList(TEST_DB_ID));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(paramsInternal);

    // Query with future purgeAfterMs (should return the soft deleted table)
    long futureTimestamp = Instant.now().plus(10, ChronoUnit.DAYS).toEpochMilli();
    paramsInternal = new HashMap<>();
    paramsInternal.put("databaseId", Collections.singletonList(TEST_DB_ID));
    paramsInternal.put("purgeAfterMs", Collections.singletonList(String.valueOf(futureTimestamp)));
    params = new MultiValueMapAdapter(paramsInternal);

    // Should return the soft deleted table due to default purgeAfterMs being 7 days in the future
    mvc.perform(MockMvcRequestBuilders.get("/hts/tables/querySoftDeleted").params(params))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.content", hasSize(1)))
        .andExpect(jsonPath("$.pageResults.content[0].databaseId", is(TEST_DB_ID)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is(testTableId)));

    // Query with past purgeAfterMs (should return soft deleted table)
    long pastTimestamp = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
    paramsInternal = new HashMap<>();
    paramsInternal.put("databaseId", Collections.singletonList(TEST_DB_ID));
    paramsInternal.put("purgeAfterMs", Collections.singletonList(String.valueOf(pastTimestamp)));
    params = new MultiValueMapAdapter(paramsInternal);

    mvc.perform(MockMvcRequestBuilders.get("/hts/tables/querySoftDeleted").params(params))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.content", hasSize(0)));
  }

  @Test
  public void testRestoreSoftDeletedTable() throws Exception {
    // Insert a soft deleted entry directly into repository
    long deletedAtMs = System.currentTimeMillis();
    long purgeAfterMs = deletedAtMs + 604800000L; // 7 days from deletion
    String tableId = "testRestoreTable";
    SoftDeletedUserTableRow softDeletedEntry =
        SoftDeletedUserTableRow.builder()
            .tableId(tableId)
            .databaseId(TEST_DB_ID)
            .deletedAtMs(deletedAtMs)
            .version(1L)
            .metadataLocation("test-location")
            .storageType("HDFS")
            .creationTime(System.currentTimeMillis())
            .purgeAfterMs(purgeAfterMs)
            .build();
    softDeletedHtsJdbcRepository.save(softDeletedEntry);

    // Restore the soft deleted table
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables/restore")
                .param("databaseId", TEST_DB_ID)
                .param("tableId", tableId)
                .param("deletedAtMs", String.valueOf(deletedAtMs)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.databaseId", is(TEST_DB_ID)))
        .andExpect(jsonPath("$.entity.tableId", is(tableId)))
        .andExpect(jsonPath("$.entity.deletedAtMs").doesNotExist())
        .andExpect(jsonPath("$.entity.purgeAfterMs").doesNotExist());

    // Verify table is now active again
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", TEST_DB_ID)
                .param("tableId", tableId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.databaseId", is(TEST_DB_ID)))
        .andExpect(jsonPath("$.entity.tableId", is(tableId)));

    // Verify it's no longer in soft deleted tables
    Map<String, List<String>> queryParams = new HashMap<>();
    queryParams.put("databaseId", Collections.singletonList(TEST_DB_ID));
    queryParams.put("tableId", Collections.singletonList(tableId));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(queryParams);
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/querySoftDeleted")
                .params(params)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.content", hasSize(0)));
  }

  @Test
  public void testRestoreNonExistentSoftDeletedTable() throws Exception {
    // Try to recover a non-existent soft deleted table
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables/restore")
                .param("databaseId", "non_existent_db")
                .param("tableId", "non_existent_table")
                .param("deletedAtMs", "1234567890"))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testPurgeSoftDeletedTable() throws Exception {
    // Insert a soft deleted entry directly into repository
    long deletedAtMs = System.currentTimeMillis();
    long purgeAfterMs = deletedAtMs + 604800000L; // 7 days from deletion
    SoftDeletedUserTableRow softDeletedEntry =
        SoftDeletedUserTableRow.builder()
            .tableId(TEST_TABLE_ID)
            .databaseId(TEST_DB_ID)
            .deletedAtMs(deletedAtMs)
            .version(1L)
            .metadataLocation("test-location")
            .storageType("HDFS")
            .creationTime(System.currentTimeMillis())
            .purgeAfterMs(purgeAfterMs)
            .build();
    softDeletedHtsJdbcRepository.save(softDeletedEntry);

    // Get the soft deleted table to obtain deletedAtMs
    Map<String, List<String>> queryParams = new HashMap<>();
    queryParams.put("databaseId", Collections.singletonList(TEST_DB_ID));
    queryParams.put("tableId", Collections.singletonList(TEST_TABLE_ID));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(queryParams);

    // Purge the soft deleted table
    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/tables/purge")
                .param("databaseId", TEST_DB_ID)
                .param("tableId", TEST_TABLE_ID)
                .param("purgeAfterMs", String.valueOf(purgeAfterMs + 1)))
        .andExpect(status().isNoContent());

    // Verify it's no longer in soft deleted tables
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/querySoftDeleted")
                .params(params)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.content", hasSize(0)));
  }

  @Test
  public void testPurgeAllSoftDeletedTables() throws Exception {
    // Insert a soft deleted entry directly into repository
    long deletedAtMs = System.currentTimeMillis();
    long purgeAfterMs = deletedAtMs + 604800000L; // 7 days from deletion
    SoftDeletedUserTableRow softDeletedEntry =
        SoftDeletedUserTableRow.builder()
            .tableId(TEST_TABLE_ID)
            .databaseId(TEST_DB_ID)
            .deletedAtMs(deletedAtMs)
            .version(1L)
            .metadataLocation("test-location")
            .storageType("HDFS")
            .creationTime(System.currentTimeMillis())
            .purgeAfterMs(purgeAfterMs)
            .build();
    softDeletedHtsJdbcRepository.save(softDeletedEntry);
    softDeletedHtsJdbcRepository.save(
        softDeletedEntry.toBuilder().deletedAtMs(deletedAtMs + 1).purgeAfterMs(0L).build());
    softDeletedHtsJdbcRepository.save(
        softDeletedEntry
            .toBuilder()
            .deletedAtMs(deletedAtMs + 2)
            .purgeAfterMs(purgeAfterMs + 1)
            .build());

    // Get the soft deleted table to obtain deletedAtMs
    Map<String, List<String>> queryParams = new HashMap<>();
    queryParams.put("databaseId", Collections.singletonList(TEST_DB_ID));
    queryParams.put("tableId", Collections.singletonList(TEST_TABLE_ID));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(queryParams);

    // Purge the soft deleted table
    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/tables/purge")
                .param("databaseId", TEST_DB_ID)
                .param("tableId", TEST_TABLE_ID))
        .andExpect(status().isNoContent());

    // Verify it's no longer in soft deleted tables
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/querySoftDeleted")
                .params(params)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.content", hasSize(0)));
  }

  // ---------------------------------------------------------------------------------------------
  // entityType discriminator over HTTP
  // ---------------------------------------------------------------------------------------------

  /**
   * Canonical interleaved fixture, seeded in its own database so it does not disturb the {@code
   * test_db0} counts asserted by the tests above.
   */
  private static final String ENTITY_TYPE_DB = "entity_type_db";

  /**
   * The HTTP contract the tables service actually consumes: a view at a table's key is a 404, the
   * same response an absent row produces, so no client-side check is needed to hide it.
   */
  @Test
  public void testGetUserTableReturnsNotFoundForNonTableRow() throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "point_read", EntityType.VIEW);

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "point_read")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))));

    assertThat(
            htsRepository
                .findById(
                    UserTableRowPrimaryKey.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("point_read")
                        .build())
                .isPresent())
        .isTrue();
  }

  /** Regression: a legacy row with a SQL NULL discriminator still reads as a table over HTTP. */
  @Test
  public void testGetUserTableReturnsLegacyRowAsTable() throws Exception {
    seedLegacyRow(ENTITY_TYPE_DB, "point_read");

    expectTablePointReadAnswersTable();
  }

  /** Regression: an explicitly typed table row reads as the same thing. */
  @Test
  public void testGetUserTableReturnsTypedTableRow() throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "point_read", EntityType.TABLE);

    expectTablePointReadAnswersTable();
  }

  private void expectTablePointReadAnswersTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "point_read")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.tableId", is(equalTo("point_read"))))
        .andExpect(jsonPath("$.entity.entityType", is("TABLE")));
  }

  private UserTableRow entityTypeRow(String databaseId, String tableId, EntityType entityType) {
    return UserTableRow.builder()
        .databaseId(databaseId)
        .tableId(tableId)
        .version(null)
        .metadataLocation(String.format("/openhouse/%s/%s/v0_metadata.json", databaseId, tableId))
        .storageType(TEST_DEFAULT_STORAGE_TYPE)
        .creationTime(TEST_CREATION_TIME)
        .entityType(entityType)
        .build();
  }

  /** The enum-typed entity cannot express a non-canonical spelling; only the column can. */
  private void insertRawEntityType(String databaseId, String tableId, String entityType) {
    new JdbcTemplate(dataSource)
        .update(
            "INSERT INTO user_table_row "
                + "(database_id, table_id, version, metadata_location, storage_type, creation_time, entity_type) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)",
            databaseId,
            tableId,
            0L,
            String.format("/openhouse/%s/%s/v0_metadata.json", databaseId, tableId),
            TEST_DEFAULT_STORAGE_TYPE,
            TEST_CREATION_TIME,
            entityType);
  }

  /** Nullable column: an absent value is a real outcome, so callers state which they expect. */
  private Optional<String> readRawEntityType(String databaseId, String tableId) {
    return Optional.ofNullable(
        new JdbcTemplate(dataSource)
            .queryForObject(
                "SELECT entity_type FROM user_table_row WHERE database_id = ? AND table_id = ?",
                String.class,
                databaseId,
                tableId));
  }

  /**
   * Plants a pre-discriminator row, whose column holds SQL NULL. Separate from {@link
   * #seedTypedRow} rather than one helper with a nullable argument: these are two fixtures, not two
   * modes.
   */
  private void seedLegacyRow(String databaseId, String tableId) {
    insertRawEntityType(databaseId, tableId, null);
  }

  /** Plants a typed row through JPA, so the enum boundary is still the thing under test. */
  private void seedTypedRow(String databaseId, String tableId, EntityType entityType) {
    htsRepository.save(entityTypeRow(databaseId, tableId, entityType));
  }

  private void seedCanonicalRows(String prefix) {
    seedLegacyRow(ENTITY_TYPE_DB, prefix + "t00_legacy");
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t01_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t02_explicit", EntityType.TABLE);
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t03_view", EntityType.VIEW);
    seedLegacyRow(ENTITY_TYPE_DB, prefix + "t04_legacy");
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t05_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t06_explicit", EntityType.TABLE);
  }

  private static MultiValueMap<String, String> queryParams(String... keyValues) {
    Map<String, List<String>> paramsInternal = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      paramsInternal.put(keyValues[i], Collections.singletonList(keyValues[i + 1]));
    }
    return new MultiValueMapAdapter(paramsInternal);
  }

  /** Both v0 table query families exclude views and keep legacy NULL rows. */
  @Test
  public void testTableQueriesExcludeViewsAndKeepLegacyRows() throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", hasSize(4)))
        .andExpect(
            jsonPath(
                "$.results[*].tableId",
                containsInAnyOrder("t00_legacy", "t02_explicit", "t04_legacy", "t06_explicit")))
        .andExpect(jsonPath("$.results[*].tableId", not(hasItem("t01_view"))))
        .andExpect(jsonPath("$.results[*].tableId", not(hasItem("t03_view"))))
        .andExpect(jsonPath("$.results[*].tableId", not(hasItem("t05_view"))));

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "tableId", "t0%"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", hasSize(4)))
        .andExpect(
            jsonPath(
                "$.results[*].tableId",
                containsInAnyOrder("t00_legacy", "t02_explicit", "t04_legacy", "t06_explicit")));
  }

  /**
   * Anti-post-filter assertion over HTTP for the v1 paged query families: an implementation that
   * filters the returned page would report totalElements=7/totalPages=4 and a 1-row first page.
   */
  @Test
  public void testPaginatedTableQueriesFilterBeforePaging() throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/tables/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .param("page", "0")
                .param("size", "2")
                .param("sortBy", "tableId")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.totalElements", is(4)))
        .andExpect(jsonPath("$.pageResults.totalPages", is(2)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(2)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is("t00_legacy")))
        .andExpect(jsonPath("$.pageResults.content[1].tableId", is("t02_explicit")));

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/tables/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .param("page", "1")
                .param("size", "2")
                .param("sortBy", "tableId")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.totalElements", is(4)))
        .andExpect(jsonPath("$.pageResults.totalPages", is(2)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(2)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is("t04_legacy")))
        .andExpect(jsonPath("$.pageResults.content[1].tableId", is("t06_explicit")));

    // Same assertions on the pattern form.
    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/tables/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "tableId", "t0%"))
                .param("page", "0")
                .param("size", "2")
                .param("sortBy", "tableId")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.totalElements", is(4)))
        .andExpect(jsonPath("$.pageResults.totalPages", is(2)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(2)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is("t00_legacy")))
        .andExpect(jsonPath("$.pageResults.content[1].tableId", is("t02_explicit")));
  }

  /** The route owns the type, so a writer that omits it still stores the canonical constant. */
  @Test
  public void testEntityTypePutAndGetRoundTrip() throws Exception {
    UserTable viewEntity =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("put_view")
            .tableVersion(INITIAL_TABLE_VERSION)
            .metadataLocation("/openhouse/entity_type_db/put_view/v0_metadata.json")
            .entityType("VIEW")
            .build();

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(viewEntity)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.entity.entityType", is("VIEW")));

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "put_view")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());

    assertThat(
            htsRepository
                .findById(
                    UserTableRowPrimaryKey.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("put_view")
                        .build())
                .get()
                .getEntityType())
        .isEqualTo(EntityType.VIEW);

    // A legacy PUT that omits the field is stamped by the route it reached.
    UserTable legacyEntity =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("put_legacy")
            .tableVersion(INITIAL_TABLE_VERSION)
            .metadataLocation("/openhouse/entity_type_db/put_legacy/v0_metadata.json")
            .build();

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(legacyEntity)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.entity.entityType", is("TABLE")));

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "put_legacy")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.entityType", is("TABLE")));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_legacy")).hasValue("TABLE");
  }

  /**
   * The column vocabulary stays exactly TABLE/VIEW/NULL. The lowercase spelling goes to the view
   * route, because the route owns the type.
   */
  @Test
  public void testEntityTypePutNormalizesSpellingToCanonicalConstant() throws Exception {
    UserTable lowercaseView =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("put_lower_view")
            .tableVersion(INITIAL_TABLE_VERSION)
            .metadataLocation("/openhouse/entity_type_db/put_lower_view/v0_metadata.json")
            .entityType("view")
            .build();

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(lowercaseView)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.entity.entityType", is("VIEW")));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_lower_view")).hasValue("VIEW");

    // The same spelling on the table route now contradicts the route and is refused.
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(
                            lowercaseView.toBuilder().tableId("put_lower_view_on_table").build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  /**
   * An unrecognized discriminator is a bad request, not a server error: validation rejects it
   * before the enum boundary is reached, and the enum boundary would reject it as a request failure
   * too.
   */
  @Test
  public void testEntityTypePutWithUnknownValueIsBadRequest() throws Exception {
    UserTable garbage =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("put_garbage")
            .tableVersion(INITIAL_TABLE_VERSION)
            .metadataLocation("/openhouse/entity_type_db/put_garbage/v0_metadata.json")
            .entityType("UNKNOWN")
            .build();

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(garbage)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.BAD_REQUEST.name()))));

    assertThat(
            htsRepository
                .findById(
                    UserTableRowPrimaryKey.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("put_garbage")
                        .build())
                .isPresent())
        .isFalse();
  }

  /**
   * {@code /hts/tables/query} is table-scoped by path, so {@code entityType} is not a supported
   * query parameter. It is mapped onto the request object but never reaches a predicate, so a
   * client that sends one is silently answered with tables.
   */
  @Test
  public void testEntityTypeQueryParameterIsIgnored() throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "entityType", "VIEW"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", hasSize(4)))
        .andExpect(
            jsonPath(
                "$.results[*].tableId",
                containsInAnyOrder("t00_legacy", "t02_explicit", "t04_legacy", "t06_explicit")));
  }

  /**
   * Publish-boundary defense in depth. Issues the exact HTS PUT a table create would emit at a key
   * already occupied by a VIEW pointer: tableVersion=INITIAL_VERSION, no entityType, and a
   * different candidate metadataLocation. The pointer must be rejected with 409 and left
   * byte-identical — same numeric JPA {@code version}, {@code entityType} and {@code
   * metadataLocation}.
   */
  @Test
  public void testCreateTablePointerPublishCannotOverwriteView() throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "occupied_by_view", EntityType.VIEW);

    UserTableRowPrimaryKey key =
        UserTableRowPrimaryKey.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("occupied_by_view")
            .build();
    UserTableRow before = htsRepository.findById(key).get();

    UserTable tableCreatePut =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("occupied_by_view")
            .tableVersion(INITIAL_TABLE_VERSION)
            .metadataLocation(
                "/openhouse/entity_type_db/occupied_by_view-uuid/00001-candidate.metadata.json")
            .build();

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(tableCreatePut)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isConflict());

    UserTableRow after = htsRepository.findById(key).get();
    assertThat(after.getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(after.getEntityType()).isEqualTo(before.getEntityType());
    assertThat(after.getVersion()).isEqualTo(before.getVersion());
    assertThat(after.getMetadataLocation()).isEqualTo(before.getMetadataLocation());
  }

  // ---------------------------------------------------------------------------------------------
  // view routes
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testGetViewReturnsViewsAndHidesTables() throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "view_point", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "table_point", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "legacy_point");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "view_point")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.tableId", is("view_point")))
        .andExpect(jsonPath("$.entity.databaseId", is(ENTITY_TYPE_DB)))
        .andExpect(jsonPath("$.entity.entityType", is("VIEW")));

    // Case-insensitive on the key, exactly like the table read.
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB.toUpperCase())
                .param("tableId", "VIEW_POINT")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.entityType", is("VIEW")));

    // Hidden, not deleted.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "table_point")).hasValue("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "legacy_point")).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"table_point", "legacy_point", "absent_point"})
  public void testGetViewIsNotFoundForEveryNonViewKey(String tableId) throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "table_point", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "legacy_point");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", tableId)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))))
        .andExpect(
            jsonPath(
                "$.message",
                is(
                    equalTo(
                        NO_SUCH_ENTITY_ERROR_MSG_TEMPLATE
                            .replace("$ent", "View")
                            .replace("$id", ENTITY_TYPE_DB + "." + tableId)))));
  }

  @Test
  public void testGetViewWithInvalidKeyIsBadRequest() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "bad??id")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))))
        .andExpect(jsonPath("$.message", containsString("tableId provided: bad??id")))
        .andExpect(jsonPath("$.stacktrace").doesNotExist());

    // A missing required parameter is a binding failure, which is also a 400.
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  /** Unlike the table query, an empty filter returns every view, not database names. */
  @Test
  public void testViewQueriesExcludeTablesAndLegacyRows() throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.results", hasSize(3)))
        .andExpect(
            jsonPath(
                "$.results[*].tableId", containsInAnyOrder("t01_view", "t03_view", "t05_view")))
        .andExpect(jsonPath("$.results[*].tableId", not(hasItem("t00_legacy"))))
        .andExpect(jsonPath("$.results[*].tableId", not(hasItem("t02_explicit"))))
        .andExpect(jsonPath("$.results[*].entityType", everyItem(is("VIEW"))));

    // Pattern form.
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "tableId", "t0%"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", hasSize(3)))
        .andExpect(
            jsonPath(
                "$.results[*].tableId", containsInAnyOrder("t01_view", "t03_view", "t05_view")));

    // Empty filter map: every view, fully identified — not a database-name projection.
    mvc.perform(MockMvcRequestBuilders.get("/hts/views/query").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", hasSize(3)))
        .andExpect(
            jsonPath(
                "$.results[*].tableId", containsInAnyOrder("t01_view", "t03_view", "t05_view")))
        .andExpect(jsonPath("$.results[*].databaseId", everyItem(is(ENTITY_TYPE_DB))));
  }

  /**
   * Regression: the two empty queries mean different things; the view route must not inherit this.
   */
  @Test
  public void testEmptyTableQueryStillProjectsDatabaseNames() throws Exception {
    seedCanonicalRows("");

    mvc.perform(MockMvcRequestBuilders.get("/hts/tables/query").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results[*].databaseId", hasItem(ENTITY_TYPE_DB)))
        .andExpect(jsonPath("$.results[*].tableId", everyItem(nullValue())));
  }

  /** A post-filtering implementation would report totalElements=7 and a 1-row first page. */
  @Test
  public void testPaginatedViewQueriesFilterBeforePaging() throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .param("page", "0")
                .param("size", "2")
                .param("sortBy", "tableId")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.totalElements", is(3)))
        .andExpect(jsonPath("$.pageResults.totalPages", is(2)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(2)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is("t01_view")))
        .andExpect(jsonPath("$.pageResults.content[1].tableId", is("t03_view")));

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .param("page", "1")
                .param("size", "2")
                .param("sortBy", "tableId")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.totalElements", is(3)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(1)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is("t05_view")));

    // Same assertions on the pattern form.
    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "tableId", "t0%"))
                .param("page", "0")
                .param("size", "2")
                .param("sortBy", "tableId")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.totalElements", is(3)))
        .andExpect(jsonPath("$.pageResults.totalPages", is(2)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(2)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is("t01_view")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"TABLE", "table", "VIEW", "UNKNOWN"})
  public void testEntityTypeQueryParameterIsIgnoredOnViewQuery(String entityType) throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "entityType", entityType))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", hasSize(3)))
        .andExpect(
            jsonPath(
                "$.results[*].tableId", containsInAnyOrder("t01_view", "t03_view", "t05_view")));
  }

  /** The paged route drops it at the same boundary; the two routes must not diverge here. */
  @ParameterizedTest
  @ValueSource(strings = {"TABLE", "table", "VIEW", "UNKNOWN"})
  public void testEntityTypeQueryParameterIsIgnoredOnPagedViewQuery(String entityType)
      throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "entityType", entityType))
                .param("page", "0")
                .param("size", "2")
                .param("sortBy", "tableId")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.totalElements", is(3)))
        .andExpect(jsonPath("$.pageResults.content[0].tableId", is("t01_view")))
        .andExpect(jsonPath("$.pageResults.content[*].entityType", everyItem(is("VIEW"))));
  }

  /** As on the unpaged route; only the paging metadata differs. */
  @Test
  public void testPagedViewQueryWithAnEmptyFilterMapReturnsEveryView() throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .param("sortBy", "tableId")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.number", is(0)))
        .andExpect(jsonPath("$.pageResults.size", is(50)))
        .andExpect(jsonPath("$.pageResults.totalElements", is(3)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(3)))
        .andExpect(
            jsonPath(
                "$.pageResults.content[*].tableId",
                containsInAnyOrder("t01_view", "t03_view", "t05_view")))
        // Not a database-name projection: every result is a fully identified view.
        .andExpect(jsonPath("$.pageResults.content[*].entityType", everyItem(is("VIEW"))));
  }

  /**
   * Regression guard: {@code _} is a single-character wildcard, so {@code match_%} also matches
   * {@code matchXview}. Every canonical fixture id has a literal underscore there, so only a
   * differently-spelled row can demonstrate it. Pre-existing behaviour, pinned not endorsed.
   */
  @Test
  public void testViewPatternQueryKeepsUnderscoreAsASqlWildcard() throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "match_t01_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "matchXview", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "nomatchview", EntityType.VIEW);

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "tableId", "match_%"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", hasSize(2)))
        .andExpect(
            jsonPath("$.results[*].tableId", containsInAnyOrder("match_t01_view", "matchXview")));
  }

  @Test
  public void testPutViewCreatesThenUpdates() throws Exception {
    UserTable view =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("put_view_lifecycle")
            .tableVersion(INITIAL_TABLE_VERSION)
            .metadataLocation("/openhouse/entity_type_db/put_view_lifecycle/v0_metadata.json")
            .build();

    // Omitted type on the view route is stamped VIEW.
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(view)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.tableId", is("put_view_lifecycle")))
        .andExpect(jsonPath("$.entity.entityType", is("VIEW")));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_view_lifecycle")).hasValue("VIEW");

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(
                            view.toBuilder()
                                .tableVersion(view.getMetadataLocation())
                                .metadataLocation(
                                    "/openhouse/entity_type_db/put_view_lifecycle/v1_metadata.json")
                                .build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.entityType", is("VIEW")))
        .andExpect(
            jsonPath(
                "$.entity.metadataLocation",
                is("/openhouse/entity_type_db/put_view_lifecycle/v1_metadata.json")));

    // The view is readable through its own point read and absent from the table one.
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "put_view_lifecycle")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "put_view_lifecycle")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  /** The endpoint declares the type; a payload may agree or stay silent, never override. */
  @Test
  public void testPutWithOppositeExplicitTypeIsBadRequest() throws Exception {
    UserTable viewOnTableRoute =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("cross_type_put")
            .tableVersion(INITIAL_TABLE_VERSION)
            .metadataLocation("/openhouse/entity_type_db/cross_type_put/v0_metadata.json")
            .entityType("VIEW")
            .build();

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(viewOnTableRoute)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))))
        .andExpect(
            jsonPath(
                "$.message",
                is(equalTo("entityType provided: VIEW, but this endpoint serves TABLE only"))))
        .andExpect(jsonPath("$.stacktrace").doesNotExist());

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(viewOnTableRoute.toBuilder().entityType("TABLE").build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                is(equalTo("entityType provided: TABLE, but this endpoint serves VIEW only"))));

    // Unknown values are rejected on the view route too, by the same ingress rule.
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(viewOnTableRoute.toBuilder().entityType("UNKNOWN").build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                is(equalTo("entityType provided: UNKNOWN, but this endpoint serves VIEW only"))));

    // A rejected write persists nothing.
    assertThat(
            htsRepository
                .findById(
                    UserTableRowPrimaryKey.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("cross_type_put")
                        .build())
                .isPresent())
        .isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"occupied_by_table", "occupied_by_legacy"})
  public void testPutViewCannotOverwriteTableOrLegacyRow(String tableId) throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "occupied_by_table", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "occupied_by_legacy");

    UserTableRow before =
        htsRepository
            .findById(
                UserTableRowPrimaryKey.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId(tableId)
                    .build())
            .get();

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(
                            UserTable.builder()
                                .databaseId(ENTITY_TYPE_DB)
                                .tableId(tableId)
                                .tableVersion(before.getMetadataLocation())
                                .metadataLocation(
                                    String.format(
                                        "/openhouse/entity_type_db/%s/v1_metadata.json", tableId))
                                .build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.CONFLICT.name()))))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.CONFLICT.getReasonPhrase()))))
        // The conflict names the occupant's type and the key it holds.
        .andExpect(jsonPath("$.message", containsString("TABLE")))
        .andExpect(jsonPath("$.message", containsString(ENTITY_TYPE_DB + "." + tableId)))
        .andExpect(jsonPath("$.cause", notNullValue()))
        .andExpect(jsonPath("$.stacktrace").doesNotExist());

    UserTableRow after =
        htsRepository
            .findById(
                UserTableRowPrimaryKey.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId(tableId)
                    .build())
            .get();
    assertThat(after.getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(after.getVersion()).isEqualTo(before.getVersion());
    assertThat(after.getMetadataLocation()).isEqualTo(before.getMetadataLocation());
  }

  @Test
  public void testDeleteViewRemovesTheViewAndCreatesNoSoftDeletedRow() throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "drop_view", EntityType.VIEW);

    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "drop_view"))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));

    assertThat(
            htsRepository
                .findById(
                    UserTableRowPrimaryKey.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("drop_view")
                        .build())
                .isPresent())
        .isFalse();

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/querySoftDeleted")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "drop_view")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.content", hasSize(0)));

    // A second drop is a plain 404.
    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "drop_view"))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testTypedDeletesCannotCrossTypes() throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "cross_delete_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "cross_delete_table", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "cross_delete_legacy");

    // A view at a table-scoped delete is reported exactly as an absent table is.
    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "cross_delete_view")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.NOT_FOUND.getReasonPhrase()))))
        .andExpect(
            jsonPath(
                "$.message",
                is(
                    equalTo(
                        NOT_FOUND_ERROR_MSG_TEMPLATE
                            .replace("$db", ENTITY_TYPE_DB)
                            .replace("$tbl", "cross_delete_view")))))
        .andExpect(jsonPath("$.cause", notNullValue()))
        .andExpect(jsonPath("$.stacktrace").doesNotExist());

    mvc.perform(
            MockMvcRequestBuilders.delete("/v1/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "cross_delete_view")
                .param("isSoftDelete", "true")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath(
                "$.message",
                is(
                    equalTo(
                        NOT_FOUND_ERROR_MSG_TEMPLATE
                            .replace("$db", ENTITY_TYPE_DB)
                            .replace("$tbl", "cross_delete_view")))));

    // And the mirror: the view route reports a table, or a legacy row, as an absent view.
    for (String tableId : new String[] {"cross_delete_table", "cross_delete_legacy"}) {
      mvc.perform(
              MockMvcRequestBuilders.delete("/hts/views")
                  .param("databaseId", ENTITY_TYPE_DB)
                  .param("tableId", tableId)
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isNotFound())
          .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))))
          .andExpect(
              jsonPath(
                  "$.message",
                  is(
                      equalTo(
                          NO_SUCH_ENTITY_ERROR_MSG_TEMPLATE
                              .replace("$ent", "View")
                              .replace("$id", ENTITY_TYPE_DB + "." + tableId)))));
    }

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "cross_delete_view")).hasValue("VIEW");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "cross_delete_table")).hasValue("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "cross_delete_legacy")).isEmpty();
  }

  @Test
  public void testDeleteViewWithInvalidKeyIsBadRequest() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/views")
                .param("databaseId", "db??")
                .param("tableId", "tb??")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))))
        // Both offending components are named, exactly as the table delete reports them.
        .andExpect(jsonPath("$.message", containsString("databaseId provided: db??")))
        .andExpect(jsonPath("$.message", containsString("tableId provided: tb??")))
        .andExpect(jsonPath("$.cause", notNullValue()))
        // ServiceAuditAspect strips the stacktrace from every client-facing error body; it is
        // retained only on the audit event. The view routes must not be an exception to that.
        .andExpect(jsonPath("$.stacktrace").doesNotExist());
  }

  @Test
  public void testViewQueryRejectsInvalidFilters() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views/query")
                .params(queryParams("databaseId", "db%"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))));

    // A tableId pattern without a database has no scope to apply to.
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views/query")
                .params(queryParams("tableId", "t0%"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());

    // Only databaseId and tableId are supported filters.
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "creationTime", "123"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testPaginatedViewQueryRejectsInvalidPagingAndAppliesDefaults() throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .param("page", "-1")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .param("size", "0")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .params(queryParams("databaseId", "db%"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());

    // No page or size supplied: the first page of fifty.
    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.number", is(0)))
        .andExpect(jsonPath("$.pageResults.size", is(50)))
        .andExpect(jsonPath("$.pageResults.totalElements", is(3)))
        .andExpect(jsonPath("$.pageResults.totalPages", is(1)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(3)));
  }

  // ---------------------------------------------------------------------------------------------
  // neutral entity route
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testNeutralEntityReadReportsCanonicalType() throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "neutral_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "neutral_table", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "neutral_legacy");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "neutral_view")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.tableId", is("neutral_view")))
        .andExpect(jsonPath("$.entity.entityType", is("VIEW")));

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "neutral_table")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.entityType", is("TABLE")));

    // A legacy null is reported as TABLE, because that is what the data means.
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "neutral_legacy")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.entityType", is("TABLE")));
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "neutral_legacy")).isEmpty();
  }

  @Test
  public void testNeutralEntityReadReportsAbsenceAndInvalidKeysDistinctly() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "neutral_absent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath(
                "$.message",
                is(
                    equalTo(
                        NO_SUCH_ENTITY_ERROR_MSG_TEMPLATE
                            .replace("$ent", "Entity")
                            .replace("$id", ENTITY_TYPE_DB + ".neutral_absent")))));

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "bad??id")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  // ---------------------------------------------------------------------------------------------
  // corrupt storage over HTTP
  // ---------------------------------------------------------------------------------------------

  /** A corrupt occupant must never read as free, so the read and the write on it both fail. */
  @Test
  public void testCorruptDiscriminatorIsServerErrorOnNeutralReadAndPut() throws Exception {
    insertRawEntityType(ENTITY_TYPE_DB, "corrupt_row", "UNKNOWN");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "corrupt_row")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError());

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(
                            UserTable.builder()
                                .databaseId(ENTITY_TYPE_DB)
                                .tableId("corrupt_row")
                                .tableVersion(INITIAL_TABLE_VERSION)
                                .metadataLocation(
                                    "/openhouse/entity_type_db/corrupt_row/v1_metadata.json")
                                .build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError());

    // The occupant is retained for operator repair.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "corrupt_row")).hasValue("UNKNOWN");
  }

  /** The diagnostic has to survive the persistence wrapping to reach the operator. */
  @Test
  public void testCorruptDiscriminatorResponseCarriesColumnDiagnostic() throws Exception {
    insertRawEntityType(ENTITY_TYPE_DB, "corrupt_diagnostic", "UNKNOWN");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "corrupt_diagnostic")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.name()))))
        .andExpect(
            jsonPath("$.error", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase()))))
        .andExpect(
            jsonPath(
                "$.message",
                containsString("Column user_table_row.entity_type holds unrecognized value")))
        .andExpect(jsonPath("$.message", containsString("UNKNOWN")))
        .andExpect(jsonPath("$.cause", notNullValue()))
        .andExpect(jsonPath("$.stacktrace").doesNotExist());
  }

  /**
   * Regression: {@code TABLE_ROW_PREDICATE} excludes an unrecognized spelling before hydration, so
   * a corrupt row is simply absent from the table point read under H2. Unchanged from base.
   */
  @Test
  public void testCorruptRowIsExcludedFromTheTablePointReadRatherThanFailing() throws Exception {
    insertRawEntityType(ENTITY_TYPE_DB, "corrupt_table_point", "UNKNOWN");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "corrupt_table_point")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "corrupt_table_point")).hasValue("UNKNOWN");
  }

  /**
   * The table reads now cross the same boundary as the view reads, so corruption reaching one
   * produces the column-and-value diagnostic rather than the ORM {@code toString()}. This is the
   * one intended behavioural change to the table path. Injected, because under a folding collation
   * — not under H2 — the predicate would match a spelling Java rejects.
   */
  @Test
  public void testCorruptDiscriminatorOnTheTablePointReadCarriesColumnDiagnostic()
      throws Exception {
    Mockito.doThrow(corruptWrapper())
        .when(htsJdbcRepository)
        .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(anyString(), anyString());

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "corrupt_table_point")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.name()))))
        .andExpect(
            jsonPath("$.error", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase()))))
        .andExpect(
            jsonPath(
                "$.message",
                containsString("Column user_table_row.entity_type holds unrecognized value")))
        .andExpect(jsonPath("$.message", containsString("UNKNOWN")))
        .andExpect(jsonPath("$.entity").doesNotExist());
  }

  /**
   * Restore's occupancy read is neutral, so it is the one table path on which a genuinely corrupt
   * row hydrates under H2. It must fail loudly rather than report the key as free and clobber it.
   */
  @Test
  public void testCorruptOccupantMakesRestoreFailRatherThanOverwrite() throws Exception {
    insertRawEntityType(ENTITY_TYPE_DB, "corrupt_restore_target", "UNKNOWN");

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables/restore")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "corrupt_restore_target")
                .param("deletedAtMs", "1")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(
            jsonPath(
                "$.message",
                containsString("Column user_table_row.entity_type holds unrecognized value")));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "corrupt_restore_target")).hasValue("UNKNOWN");
  }

  /** The table query family reaches it too, and reports no partial list alongside the failure. */
  @ParameterizedTest
  @ValueSource(strings = {"/hts/tables/query", "/v1/hts/tables/query"})
  public void testCorruptDiscriminatorOnATableQueryCarriesColumnDiagnostic(String route)
      throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "healthy_table", EntityType.TABLE);
    Mockito.doThrow(corruptWrapper())
        .when(htsJdbcRepository)
        .findAllTablesByFilters(anyString(), any(), any(), any(), any(), any());
    Mockito.doThrow(corruptWrapper())
        .when(htsJdbcRepository)
        .findAllTablesByFilters(anyString(), any(), any(), any(), any(), any(), any());

    mvc.perform(
            MockMvcRequestBuilders.get(route)
                .params(queryParams("databaseId", ENTITY_TYPE_DB))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(
            jsonPath(
                "$.message",
                containsString("Column user_table_row.entity_type holds unrecognized value")))
        .andExpect(jsonPath("$.results").doesNotExist())
        .andExpect(jsonPath("$.pageResults").doesNotExist());
  }

  /**
   * The other half of the gate: a non-corrupt failure on a table read is rethrown unchanged, so the
   * body is byte-identical to what the generic advice produced before this change.
   */
  @Test
  public void testNonCorruptFailureOnATableReadKeepsTheGenericBody() throws Exception {
    DataAccessResourceFailureException raw =
        new DataAccessResourceFailureException("datasource down");
    Mockito.doThrow(raw)
        .when(htsJdbcRepository)
        .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(anyString(), anyString());

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "any_table")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        // handleGenericException renders the exception's own toString(), unchanged by the boundary.
        .andExpect(jsonPath("$.message", is(equalTo(raw.toString()))))
        .andExpect(jsonPath("$.cause", is(equalTo("Not Available"))))
        .andExpect(jsonPath("$.stacktrace").doesNotExist());
  }

  @Test
  public void testCorruptDiscriminatorIsNotFoundOnTypedDeleteAndRename() throws Exception {
    insertRawEntityType(ENTITY_TYPE_DB, "corrupt_mutate", "UNKNOWN");

    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "corrupt_mutate"))
        .andExpect(status().isNotFound());

    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "corrupt_mutate"))
        .andExpect(status().isNotFound());

    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", ENTITY_TYPE_DB)
                .param("fromTableId", "corrupt_mutate")
                .param("toDatabaseId", ENTITY_TYPE_DB)
                .param("toTableId", "corrupt_mutate_renamed")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isNotFound());

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "corrupt_mutate")).hasValue("UNKNOWN");
  }

  // ---------------------------------------------------------------------------------------------
  // dependency failures on the new routes
  // ---------------------------------------------------------------------------------------------

  /**
   * A handler that swallowed the failure into an empty result would answer 200 with no rows, so the
   * absence of any payload is asserted alongside the status. The failure is injected because H2
   * cannot select a corrupt row through the view predicate.
   */
  @ParameterizedTest
  @CsvSource({
    "/hts/views,          point",
    "/hts/views/query,    unpaged",
    "/v1/hts/views/query, paged"
  })
  public void testCorruptRowOnAnyViewRouteIsADiagnostic500WithNoPartialBody(
      String route, String shape) throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "healthy_view", EntityType.VIEW);
    injectViewReadFailure(shape, corruptWrapper());

    mvc.perform(
            MockMvcRequestBuilders.get(route)
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "point".equals(shape) ? "healthy_view" : null)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.name()))))
        .andExpect(
            jsonPath("$.error", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase()))))
        .andExpect(
            jsonPath(
                "$.message",
                containsString("Column user_table_row.entity_type holds unrecognized value")))
        .andExpect(jsonPath("$.message", containsString("UNKNOWN")))
        .andExpect(jsonPath("$.cause", notNullValue()))
        // The scoped advice is audited and stripped exactly like the shared one.
        .andExpect(jsonPath("$.stacktrace").doesNotExist())
        // Never a partial success: no rows are reported alongside the failure.
        .andExpect(jsonPath("$.results").doesNotExist())
        .andExpect(jsonPath("$.pageResults").doesNotExist())
        .andExpect(jsonPath("$.entity").doesNotExist());
  }

  /** Rendered generically from the preserved original, not as an empty result or as bad data. */
  @ParameterizedTest
  @CsvSource({
    "/hts/views,          point",
    "/hts/views/query,    unpaged",
    "/v1/hts/views/query, paged"
  })
  public void testUnrelatedDependencyFailureOnAnyViewRouteIsAGeneric500(String route, String shape)
      throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "healthy_view", EntityType.VIEW);
    injectViewReadFailure(shape, new DataAccessResourceFailureException("datasource down"));

    mvc.perform(
            MockMvcRequestBuilders.get(route)
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "point".equals(shape) ? "healthy_view" : null)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.name()))))
        .andExpect(jsonPath("$.message", containsString("datasource down")))
        .andExpect(
            jsonPath(
                "$.message",
                not(containsString("Column user_table_row.entity_type holds unrecognized value"))))
        .andExpect(jsonPath("$.stacktrace").doesNotExist())
        .andExpect(jsonPath("$.results").doesNotExist())
        .andExpect(jsonPath("$.pageResults").doesNotExist());
  }

  @Test
  public void testDependencyFailureOnTheNeutralRouteIsAGeneric500RatherThanNotFound()
      throws Exception {
    Mockito.doThrow(new DataAccessResourceFailureException("datasource down"))
        .when(htsJdbcRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(anyString(), anyString());

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "any_key")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.message", containsString("datasource down")))
        .andExpect(jsonPath("$.entity").doesNotExist());
  }

  @Test
  public void testDependencyFailureOnTheViewPutIsAGeneric500() throws Exception {
    DataAccessResourceFailureException raw =
        new DataAccessResourceFailureException("datasource down");
    Mockito.doThrow(raw)
        .when(htsJdbcRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(anyString(), anyString());

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(
                            UserTable.builder()
                                .databaseId(ENTITY_TYPE_DB)
                                .tableId("put_when_down")
                                .tableVersion(INITIAL_TABLE_VERSION)
                                .metadataLocation(
                                    "/openhouse/entity_type_db/put_when_down/v0_metadata.json")
                                .build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.message", is(equalTo(raw.toString()))))
        .andExpect(jsonPath("$.entity").doesNotExist());
  }

  /**
   * Regression guard for the frozen wire contract: translating the failure into a module-owned type
   * must not change one byte of the body, because the advice renders the preserved original.
   */
  @Test
  public void testTranslatedMutationFailureRendersTheSame500BodyAsTheRawFailureDid()
      throws Exception {
    DataAccessResourceFailureException raw =
        new DataAccessResourceFailureException("datasource down");
    Mockito.doThrow(raw).when(htsJdbcRepository).save(any());

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(
                            UserTable.builder()
                                .databaseId(ENTITY_TYPE_DB)
                                .tableId("save_when_down")
                                .tableVersion(INITIAL_TABLE_VERSION)
                                .metadataLocation(
                                    "/openhouse/entity_type_db/save_when_down/v0_metadata.json")
                                .build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.name()))))
        .andExpect(
            jsonPath("$.error", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase()))))
        // Exactly the original failure's toString(), not the module wrapper's message.
        .andExpect(jsonPath("$.message", is(equalTo(raw.toString()))))
        .andExpect(jsonPath("$.message", not(containsString("Mutating the user table store"))))
        .andExpect(jsonPath("$.cause", is(equalTo("Not Available"))))
        .andExpect(jsonPath("$.stacktrace").doesNotExist())
        .andExpect(jsonPath("$.entity").doesNotExist());
  }

  @Test
  public void testDependencyFailureOnTheViewDeleteIsAGeneric500RatherThanNotFound()
      throws Exception {
    DataAccessResourceFailureException raw =
        new DataAccessResourceFailureException("datasource down");
    Mockito.doThrow(raw).when(htsJdbcRepository).deleteViewById(any());

    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "drop_when_down")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.INTERNAL_SERVER_ERROR.name()))))
        .andExpect(jsonPath("$.message", is(equalTo(raw.toString()))))
        .andExpect(jsonPath("$.cause", is(equalTo("Not Available"))));
  }

  /** The wrapper shape Hibernate produces when the attribute converter fails on a row. */
  private static JpaSystemException corruptWrapper() {
    return new JpaSystemException(
        new PersistenceException(
            "Error attempting to apply AttributeConverter",
            new CorruptEntityTypeException(
                "Column user_table_row.entity_type holds unrecognized value ['UNKNOWN']; "
                    + "only TABLE, VIEW (in any case) and NULL are valid",
                new IllegalArgumentException("UNKNOWN"))));
  }

  private void injectViewReadFailure(String shape, RuntimeException failure) {
    switch (shape) {
      case "point":
        Mockito.doThrow(failure)
            .when(htsJdbcRepository)
            .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(anyString(), anyString());
        break;
      case "unpaged":
        Mockito.doThrow(failure)
            .when(htsJdbcRepository)
            .findAllViewsByFilters(anyString(), any(), any(), any(), any(), any());
        break;
      case "paged":
        Mockito.doThrow(failure)
            .when(htsJdbcRepository)
            .findAllViewsByFilters(anyString(), any(), any(), any(), any(), any(), any());
        break;
      default:
        throw new IllegalArgumentException("unknown query shape " + shape);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // table rename is table-scoped
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testRenameTableRefusesToMoveAView() throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "rename_view_src", EntityType.VIEW);

    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", ENTITY_TYPE_DB)
                .param("fromTableId", "rename_view_src")
                .param("toDatabaseId", ENTITY_TYPE_DB)
                .param("toTableId", "rename_view_dst")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isNotFound());

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_view_src")).hasValue("VIEW");
    assertThat(
            htsRepository
                .findById(
                    UserTableRowPrimaryKey.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("rename_view_dst")
                        .build())
                .isPresent())
        .isFalse();
  }

  /**
   * A SQL NULL already hydrates as TABLE, so only the raw column proves the literal was written.
   */
  @Test
  public void testRenameTableStampsCanonicalTableOnLegacyRow() throws Exception {
    seedLegacyRow(ENTITY_TYPE_DB, "rename_legacy_src");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_legacy_src")).isEmpty();

    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", ENTITY_TYPE_DB)
                .param("fromTableId", "rename_legacy_src")
                .param("toDatabaseId", ENTITY_TYPE_DB)
                .param("toTableId", "rename_legacy_dst")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_legacy_dst")).hasValue("TABLE");
  }

  /**
   * Regression guard: a destination held by the other type, or by a corrupt row, must stay
   * "occupied" under {@code TABLE_ROW_PREDICATE} rather than read as free.
   */
  @ParameterizedTest
  @CsvSource({"rename_dst_view, VIEW", "rename_dst_corrupt, UNKNOWN"})
  public void testRenameTableIntoOccupiedDestinationIsConflict(
      String destinationTableId, String storedSpelling) throws Exception {
    seedTypedRow(ENTITY_TYPE_DB, "rename_src_table", EntityType.TABLE);
    insertRawEntityType(ENTITY_TYPE_DB, destinationTableId, storedSpelling);

    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", ENTITY_TYPE_DB)
                .param("fromTableId", "rename_src_table")
                .param("toDatabaseId", ENTITY_TYPE_DB)
                .param("toTableId", destinationTableId)
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isConflict());

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_src_table")).hasValue("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, destinationTableId)).hasValue(storedSpelling);
  }

  // ---------------------------------------------------------------------------------------------
  // ingress
  // ---------------------------------------------------------------------------------------------

  /** Ingress runs ahead of the validator, so it is the first code to see an absent entity. */
  @ParameterizedTest
  @ValueSource(strings = {"/hts/tables", "/hts/views"})
  public void testPutWithNullEntityIsBadRequest(String route) throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put(route)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(null)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(jsonPath("$.message", containsString("entity cannot be empty")));
  }

  /**
   * The create path cannot show this: a legacy occupant makes it an update that must rewrite the
   * column, and an untouched SQL NULL hydrates as TABLE, so only the raw column proves it.
   */
  @Test
  public void testPutTableOverLegacyNullOccupantUpdatesAndMigratesTheColumn() throws Exception {
    seedLegacyRow(ENTITY_TYPE_DB, "put_over_legacy");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_over_legacy")).isEmpty();

    UserTable update =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("put_over_legacy")
            .tableVersion("/openhouse/entity_type_db/put_over_legacy/v0_metadata.json")
            .metadataLocation("/openhouse/entity_type_db/put_over_legacy/v1_metadata.json")
            .build();

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(update)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.entityType", is("TABLE")))
        .andExpect(
            jsonPath(
                "$.entity.metadataLocation",
                is("/openhouse/entity_type_db/put_over_legacy/v1_metadata.json")));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_over_legacy")).hasValue("TABLE");
  }
}
