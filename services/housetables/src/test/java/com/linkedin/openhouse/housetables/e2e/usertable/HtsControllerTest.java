package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static com.linkedin.openhouse.housetables.model.TestHtsApiConstants.*;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.request.CreateUpdateEntityRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.TestHtsApiConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
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

  @Autowired MockMvc mvc;

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
}
