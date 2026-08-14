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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
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

  private UserTableRow entityTypeRow(String databaseId, String tableId, String entityType) {
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

  private void seedCanonicalRows(String prefix) {
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t00_legacy", null));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t01_view", "VIEW"));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t02_explicit", "TABLE"));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t03_view", "VIEW"));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t04_legacy", null));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t05_view", "VIEW"));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t06_explicit", "TABLE"));
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

  /** The discriminator survives the HTTP PUT/GET boundary, and legacy writers stay null. */
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
            MockMvcRequestBuilders.put("/hts/tables")
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
        .isEqualTo("VIEW");

    // A legacy PUT that omits the field must stay null end-to-end.
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
        .andExpect(jsonPath("$.entity.entityType").doesNotExist());

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", ENTITY_TYPE_DB)
                .param("tableId", "put_legacy")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entity.entityType").doesNotExist());

    assertThat(
            htsRepository
                .findById(
                    UserTableRowPrimaryKey.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("put_legacy")
                        .build())
                .get()
                .getEntityType())
        .isNull();
  }

  /**
   * Pins validator + service routing over HTTP, not merely repository behavior: the request carries
   * only databaseId and entityType=VIEW. It fails if the validator rejects the parameter or if the
   * routing predicate still classifies this as a plain table listing.
   */
  @Test
  public void testEntityTypeOnlyViewQueryRoutesToGeneralSearch() throws Exception {
    seedCanonicalRows("");

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "entityType", "VIEW"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", hasSize(3)))
        .andExpect(
            jsonPath(
                "$.results[*].tableId", containsInAnyOrder("t01_view", "t03_view", "t05_view")));

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/hts/tables/query")
                .params(queryParams("databaseId", ENTITY_TYPE_DB, "entityType", "VIEW"))
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
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "occupied_by_view", "VIEW"));

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
    assertThat(after.getEntityType()).isEqualTo("VIEW");
    assertThat(after.getEntityType()).isEqualTo(before.getEntityType());
    assertThat(after.getVersion()).isEqualTo(before.getVersion());
    assertThat(after.getMetadataLocation()).isEqualTo(before.getMetadataLocation());
  }
}
