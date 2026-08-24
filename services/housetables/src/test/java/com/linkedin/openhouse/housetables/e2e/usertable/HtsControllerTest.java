package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static com.linkedin.openhouse.housetables.model.TestHtsApiConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.request.CreateUpdateEntityRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.dto.mapper.SoftDeletedUserTablesMapper;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.TestHtsApiConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
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

  @Autowired SoftDeletedUserTableHtsJdbcRepository softDeletedHtsJdbcRepository;

  @Autowired MockMvc mvc;

  @Autowired SoftDeletedUserTablesMapper softDeletedTableMapper;

  @Autowired DataSource dataSource;

  @BeforeEach
  public void setup() {
    // TODO: Use rest API to create the table and test the find/delete user table again.
    // For now manually create the user table upfront.
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    htsRepository.save(testUserTableRow);
  }

  @AfterEach
  public void tearDown() {
    // The JPA cleanup loads every row, so a planted non-canonical spelling must go first;
    // otherwise converter hydration during teardown poisons the rest of the class.
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE entity_type NOT IN ('TABLE', 'VIEW')");
    htsRepository.deleteAll();
    softDeletedHtsJdbcRepository.deleteAll();
  }

  @Test
  public void testFindAllFromDbWithTableId() throws Exception {
    // TODO: Use rest API to create the table
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());

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
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());

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
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());

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
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());

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

    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
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
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
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
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());

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
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow().toBuilder().tableId(testTableId).build());

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
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "point_read", EntityType.VIEW));

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

  @ParameterizedTest
  @NullSource
  @EnumSource(value = EntityType.class, names = "TABLE")
  public void testGetUserTableReturnsNullAndTableRows(EntityType entityType) throws Exception {
    seedRow(ENTITY_TYPE_DB, "point_read", entityType);

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

  /**
   * The strict converter refuses to write a null discriminator, so a legacy row — and any
   * non-canonical spelling — can only be planted through the column itself.
   */
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

  private String readRawEntityType(String databaseId, String tableId) {
    return new JdbcTemplate(dataSource)
        .queryForObject(
            "SELECT entity_type FROM user_table_row WHERE database_id = ? AND table_id = ?",
            String.class,
            databaseId,
            tableId);
  }

  private void seedRow(String databaseId, String tableId, EntityType entityType) {
    if (entityType == null) {
      insertRawEntityType(databaseId, tableId, null);
    } else {
      htsRepository.save(entityTypeRow(databaseId, tableId, entityType));
    }
  }

  private void seedCanonicalRows(String prefix) {
    seedRow(ENTITY_TYPE_DB, prefix + "t00_legacy", null);
    seedRow(ENTITY_TYPE_DB, prefix + "t01_view", EntityType.VIEW);
    seedRow(ENTITY_TYPE_DB, prefix + "t02_explicit", EntityType.TABLE);
    seedRow(ENTITY_TYPE_DB, prefix + "t03_view", EntityType.VIEW);
    seedRow(ENTITY_TYPE_DB, prefix + "t04_legacy", null);
    seedRow(ENTITY_TYPE_DB, prefix + "t05_view", EntityType.VIEW);
    seedRow(ENTITY_TYPE_DB, prefix + "t06_explicit", EntityType.TABLE);
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

  /**
   * A table PUT that omits the field is stamped at ingress, so the PUT response and a later GET
   * agree; a view created on its own route stays unreadable through the table read.
   */
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

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "put_view")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.entityType", is("VIEW")));

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

    // A legacy PUT that omits the field is stamped by the endpoint it arrived on.
    UserTable legacyEntity =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("put_legacy")
            .tableVersion(INITIAL_TABLE_VERSION)
            .metadataLocation("/openhouse/entity_type_db/put_legacy/v0_metadata.json")
            .build();

    // Ingress normalization runs before the handler, so the response already carries the type.
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

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_legacy")).isEqualTo("TABLE");

    assertThat(
            htsRepository
                .findById(
                    UserTableRowPrimaryKey.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("put_legacy")
                        .build())
                .get()
                .getEntityType())
        .isEqualTo(EntityType.TABLE);
  }

  /**
   * Any spelling is accepted but the canonical constant is stored, keeping the column vocabulary
   * exactly TABLE/VIEW/NULL. Each spelling must arrive on the route serving its type.
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

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_lower_view")).isEqualTo("VIEW");
    assertThat(
            htsRepository
                .findById(
                    UserTableRowPrimaryKey.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("put_lower_view")
                        .build())
                .get()
                .getEntityType())
        .isEqualTo(EntityType.VIEW);

    UserTable lowercaseTable =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("put_lower_table")
            .tableVersion(INITIAL_TABLE_VERSION)
            .metadataLocation("/openhouse/entity_type_db/put_lower_table/v0_metadata.json")
            .entityType("TaBlE")
            .build();

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(lowercaseTable)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.entity.entityType", is("TABLE")));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_lower_table")).isEqualTo("TABLE");
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
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "occupied_by_view", EntityType.VIEW));

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

  /** {@code GET /hts/views} is the mirror of the table point read, and is equally exclusive. */
  @Test
  public void testGetViewReturnsViewsAndHidesTables() throws Exception {
    seedRow(ENTITY_TYPE_DB, "view_point", EntityType.VIEW);
    seedRow(ENTITY_TYPE_DB, "table_point", EntityType.TABLE);
    seedRow(ENTITY_TYPE_DB, "legacy_point", null);

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

    for (String tableId : new String[] {"table_point", "legacy_point", "absent_point"}) {
      mvc.perform(
              MockMvcRequestBuilders.get("/hts/views")
                  .param("databaseId", ENTITY_TYPE_DB)
                  .param("tableId", tableId)
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isNotFound())
          .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))));
    }

    // Hidden, not deleted.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "table_point")).isEqualTo("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "legacy_point")).isNull();
  }

  /** An invalid key is a bad request before any lookup happens. */
  @Test
  public void testGetViewWithInvalidKeyIsBadRequest() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "bad??id")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());

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
            MockMvcRequestBuilders.get("/hts/views")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "tableId", "t01_view"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

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
                "$.results[*].tableId", containsInAnyOrder("t01_view", "t03_view", "t05_view")));
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

  /** The view query is view-scoped by path, so an {@code entityType} parameter changes nothing. */
  @Test
  public void testEntityTypeQueryParameterIsIgnoredOnViewQuery() throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/views/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "entityType", "TABLE"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", hasSize(3)))
        .andExpect(
            jsonPath(
                "$.results[*].tableId", containsInAnyOrder("t01_view", "t03_view", "t05_view")));
  }

  /** A view PUT creates then updates, and answers with the canonical type both times. */
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

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_view_lifecycle")).isEqualTo("VIEW");

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
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.BAD_REQUEST.name()))));

    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(viewOnTableRoute.toBuilder().entityType("TABLE").build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());

    // Unknown values are rejected on the view route too.
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(viewOnTableRoute.toBuilder().entityType("UNKNOWN").build())
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());

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

  /** A view PUT at a key held by a table (or a legacy null) is a conflict, not an overwrite. */
  @Test
  public void testPutViewCannotOverwriteTableOrLegacyRow() throws Exception {
    seedRow(ENTITY_TYPE_DB, "occupied_by_table", EntityType.TABLE);
    seedRow(ENTITY_TYPE_DB, "occupied_by_legacy", null);

    for (String tableId : new String[] {"occupied_by_table", "occupied_by_legacy"}) {
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
          .andExpect(status().isConflict());

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

    // The rejected write leaves the legacy occupant unmigrated.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "occupied_by_legacy")).isNull();
  }

  /** {@code DELETE /hts/views} removes exactly one view and creates no soft-deleted row. */
  @Test
  public void testDeleteViewRemovesTheViewAndCreatesNoSoftDeletedRow() throws Exception {
    seedRow(ENTITY_TYPE_DB, "drop_view", EntityType.VIEW);

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

  /** Wrong-type deletes are 404 in both directions and leave the occupant in place. */
  @Test
  public void testTypedDeletesCannotCrossTypes() throws Exception {
    seedRow(ENTITY_TYPE_DB, "cross_delete_view", EntityType.VIEW);
    seedRow(ENTITY_TYPE_DB, "cross_delete_table", EntityType.TABLE);
    seedRow(ENTITY_TYPE_DB, "cross_delete_legacy", null);

    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "cross_delete_view"))
        .andExpect(status().isNotFound());

    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "cross_delete_table"))
        .andExpect(status().isNotFound());

    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/views")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "cross_delete_legacy"))
        .andExpect(status().isNotFound());

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "cross_delete_view")).isEqualTo("VIEW");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "cross_delete_table")).isEqualTo("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "cross_delete_legacy")).isNull();
  }

  /** An invalid key on the view delete is a bad request, not a 404. */
  @Test
  public void testDeleteViewWithInvalidKeyIsBadRequest() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.delete("/hts/views")
                .param("databaseId", "db??")
                .param("tableId", "tb??"))
        .andExpect(status().isBadRequest());
  }

  // ---------------------------------------------------------------------------------------------
  // neutral entity route
  // ---------------------------------------------------------------------------------------------

  /** The occupancy read answers for either type and always names a canonical, non-null one. */
  @Test
  public void testNeutralEntityReadReportsCanonicalType() throws Exception {
    seedRow(ENTITY_TYPE_DB, "neutral_view", EntityType.VIEW);
    seedRow(ENTITY_TYPE_DB, "neutral_table", EntityType.TABLE);
    seedRow(ENTITY_TYPE_DB, "neutral_legacy", null);

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
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "neutral_legacy")).isNull();

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "neutral_absent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "bad??id")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  /**
   * Only the status is asserted: the dedicated exception and the persistence-wrapped translation
   * take different advice branches, and both are legitimately 500.
   */
  @Test
  public void testCorruptDiscriminatorIsServerErrorOnNeutralReadAndPut() throws Exception {
    insertRawEntityType(ENTITY_TYPE_DB, "corrupt_row", "UNKNOWN");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/entities")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "corrupt_row")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is5xxServerError());

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
        .andExpect(status().is5xxServerError());

    // The occupant is retained for operator repair.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "corrupt_row")).isEqualTo("UNKNOWN");
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
        .andExpect(
            jsonPath(
                "$.message",
                containsString("Column user_table_row.entity_type holds unrecognized value")))
        .andExpect(jsonPath("$.message", containsString("['UNKNOWN']")));
  }

  /** A typed delete or table rename at a corrupt key is 404, and the row stays put. */
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

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "corrupt_mutate")).isEqualTo("UNKNOWN");
  }

  // ---------------------------------------------------------------------------------------------
  // table rename is table-scoped
  // ---------------------------------------------------------------------------------------------

  /** The rename route is table-scoped: a view at the source key is a 404, and is not moved. */
  @Test
  public void testRenameTableRefusesToMoveAView() throws Exception {
    seedRow(ENTITY_TYPE_DB, "rename_view_src", EntityType.VIEW);

    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", ENTITY_TYPE_DB)
                .param("fromTableId", "rename_view_src")
                .param("toDatabaseId", ENTITY_TYPE_DB)
                .param("toTableId", "rename_view_dst")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isNotFound());

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_view_src")).isEqualTo("VIEW");
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

  /** The source column is SQL NULL, so only the raw column proves the type was written. */
  @Test
  public void testRenameTableStampsCanonicalTableOnLegacyRow() throws Exception {
    insertRawEntityType(ENTITY_TYPE_DB, "rename_legacy_src", null);

    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", ENTITY_TYPE_DB)
                .param("fromTableId", "rename_legacy_src")
                .param("toDatabaseId", ENTITY_TYPE_DB)
                .param("toTableId", "rename_legacy_dst")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_legacy_dst")).isEqualTo("TABLE");
  }

  /**
   * A destination held by a view is still a conflict; both rows are left alone.
   *
   * <p>Regression guard: a destination occupied by the other type must stay "occupied" (409) and
   * never read as "free" under {@code TABLE_ROW_PREDICATE}.
   */
  @Test
  public void testRenameTableIntoViewOccupiedDestinationIsConflict() throws Exception {
    seedRow(ENTITY_TYPE_DB, "rename_src_table", EntityType.TABLE);
    seedRow(ENTITY_TYPE_DB, "rename_dst_view", EntityType.VIEW);

    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", ENTITY_TYPE_DB)
                .param("fromTableId", "rename_src_table")
                .param("toDatabaseId", ENTITY_TYPE_DB)
                .param("toTableId", "rename_dst_view")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isConflict());

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_src_table")).isEqualTo("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_dst_view")).isEqualTo("VIEW");
  }

  /**
   * Ingress stamping runs ahead of the validator, so it is the first code to see an absent entity,
   * and answering 400 rather than dereferencing it is its job.
   */
  @Test
  public void testPutUserTableWithNullEntityIsBadRequest() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(null)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))));
  }

  /** The same on the view route, which stamps through the same ingress helper. */
  @Test
  public void testPutUserViewWithNullEntityIsBadRequest() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/views")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<UserTable>builder()
                        .entity(null)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))));
  }

  /**
   * The create path cannot show this: a legacy occupant exists, so it is an update (200, not 201)
   * that must rewrite the column. An untouched SQL NULL hydrates as TABLE, so only the raw column
   * proves it.
   */
  @Test
  public void testPutTableOverLegacyNullOccupantUpdatesAndMigratesTheColumn() throws Exception {
    insertRawEntityType(ENTITY_TYPE_DB, "put_over_legacy", null);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_over_legacy")).isNull();

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

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_over_legacy")).isEqualTo("TABLE");
  }

  /**
   * The primary key enforces occupancy without reading the row, so the answer is the same 409 a
   * healthy occupant produces rather than the 500 a hydration attempt would cause.
   *
   * <p>Regression guard: a corrupt-typed destination must stay "occupied" (409) and never read as
   * "free" under {@code TABLE_ROW_PREDICATE}. Do not delete it as redundant.
   */
  @Test
  public void testRenameTableIntoCorruptOccupiedDestinationIsConflict() throws Exception {
    seedRow(ENTITY_TYPE_DB, "rename_src_for_corrupt", EntityType.TABLE);
    insertRawEntityType(ENTITY_TYPE_DB, "rename_dst_corrupt", "UNKNOWN");

    mvc.perform(
            MockMvcRequestBuilders.patch("/hts/tables/rename")
                .param("fromDatabaseId", ENTITY_TYPE_DB)
                .param("fromTableId", "rename_src_for_corrupt")
                .param("toDatabaseId", ENTITY_TYPE_DB)
                .param("toTableId", "rename_dst_corrupt")
                .param("metadataLocation", "mockMetadataLocation"))
        .andExpect(status().isConflict());

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_src_for_corrupt")).isEqualTo("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_dst_corrupt")).isEqualTo("UNKNOWN");
  }

  /** The view query is validated exactly like its table sibling; an invalid filter is a 400. */
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

  /** Paging on the view query is validated, and its documented defaults are page 0 and size 50. */
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
}
