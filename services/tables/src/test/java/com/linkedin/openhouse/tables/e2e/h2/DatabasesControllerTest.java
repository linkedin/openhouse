package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX;
import static com.linkedin.openhouse.tables.model.DatabaseModelConstants.GET_DATABASE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.DatabaseModelConstants.GET_DATABASE_RESPONSE_BODY_DIFF_DB;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.mock.RequestConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@WithMockUser(username = "testUser")
public class DatabasesControllerTest {

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired Catalog catalog;

  @Autowired MockMvc mvc;

  @Autowired StorageManager storageManager;

  private void deleteTableAndValidateResponse(GetTableResponseBody getTableResponseBody)
      throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.delete(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/%s",
                    getTableResponseBody.getDatabaseId(),
                    getTableResponseBody.getTableId())))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));
  }

  @Test
  @Tag("cleanUp")
  public void testGetAllDatabases() throws Exception {
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY, mvc, storageManager);
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY_SAME_DB, mvc, storageManager);
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY_DIFF_DB, mvc, storageManager);

    mvc.perform(
            MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    GetAllDatabasesResponseBody.builder()
                        .results(
                            new ArrayList<>(
                                Arrays.asList(
                                    GET_DATABASE_RESPONSE_BODY,
                                    GET_DATABASE_RESPONSE_BODY_DIFF_DB)))
                        .build()
                        .toJson()));
  }

  @Test
  public void testGetAllDatabasesEmptyResult() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    GetAllDatabasesResponseBody.builder()
                        .results(new ArrayList<>())
                        .build()
                        .toJson()));
  }

  @Test
  public void testGetAllDatabasesPaginated() throws Exception {
    List<GetTableResponseBody> tables = new ArrayList<>();
    List<GetDatabaseResponseBody> databases = new ArrayList<>();
    // Create 10 tables in different databases.
    for (int i = 0; i < 10; i++) {
      String databaseId = "d" + i;
      GetTableResponseBody table = buildGetTableResponseBodyWithDbTbl(databaseId, "t1");
      tables.add(table);
      RequestAndValidateHelper.createTableAndValidateResponse(table, mvc, storageManager);
      databases.add(
          GetDatabaseResponseBody.builder()
              .databaseId(databaseId)
              .clusterId("test-cluster")
              .build());
    }
    // Get all databases with page size = 4. Number of databases in each page should be 4,4,2.
    int pageSize = 4;
    for (int i = 0; i < 3; i++) {
      int fromIndex = i * pageSize;
      int toIndex = Math.min(fromIndex + pageSize, databases.size());
      Page<GetDatabaseResponseBody> expectedResults =
          new PageImpl<>(databases.subList(fromIndex, toIndex), PageRequest.of(i, pageSize), 10);
      mvc.perform(
              MockMvcRequestBuilders.get("/v2/databases")
                  .param("page", String.valueOf(i))
                  .param("size", String.valueOf(pageSize))
                  .contentType(MediaType.APPLICATION_JSON)
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isOk())
          .andExpect(content().contentType(MediaType.APPLICATION_JSON))
          .andExpect(
              content()
                  .json(
                      GetAllDatabasesResponseBody.builder()
                          .pageResults(expectedResults)
                          .build()
                          .toJson()));
    }
    for (int i = 0; i < 10; i++) {
      RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, tables.get(i));
    }
  }

  @Test
  public void testGetUpdateAclPoliciesOnDatabasesEmptyResult() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.patch(CURRENT_MAJOR_VERSION_PREFIX + "/databases/db/aclPolicies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(RequestConstants.TEST_UPDATE_ACL_POLICIES_REQUEST_BODY.toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is(204));
  }

  // The same request other than making the URL wrong on purpose
  // Should expect BAD_REQUEST instead of RESOURCE_NOT_FOUND
  @Test
  public void testIncorrectPathThrowsSpecificException() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.patch(CURRENT_MAJOR_VERSION_PREFIX + "/databases/db/aclPolicy/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(RequestConstants.TEST_UPDATE_ACL_POLICIES_REQUEST_BODY.toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is(400));
  }

  @AfterEach
  private void cleanUpHelper(TestInfo info) {
    if (!info.getTags().contains("cleanUp")) {
      return;
    }
    try {
      // clean up the table if exists
      deleteTableAndValidateResponse(GET_TABLE_RESPONSE_BODY);
      deleteTableAndValidateResponse(GET_TABLE_RESPONSE_BODY_SAME_DB);
      deleteTableAndValidateResponse(GET_TABLE_RESPONSE_BODY_DIFF_DB);
    } catch (Exception exception) {
      log.warn("Cleaning up process interrupted with exception: {}", exception);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // A view-only database must not appear in the database listing
  // ---------------------------------------------------------------------------------------------

  /**
   * Raw pointer rows must be seeded through the pointer repository directly, because a VIEW row is
   * invisible to the table HTTP API and therefore cannot be created or cleaned up through it. Every
   * seeded key is removed in {@link #deleteSeededPointers()}.
   */
  @Autowired HouseTableRepository houseTablesRepository;

  private final List<HouseTablePrimaryKey> seededPointerKeys = new ArrayList<>();

  @AfterEach
  void deleteSeededPointers() {
    for (HouseTablePrimaryKey key : seededPointerKeys) {
      try {
        houseTablesRepository.deleteById(key);
      } catch (Exception e) {
        log.warn("Failed to clean up raw pointer {}: {}", key.getTableId(), e.toString());
      }
    }
    seededPointerKeys.clear();
  }

  private void seedRawPointer(String databaseId, String tableId, String entityType) {
    houseTablesRepository.save(
        HouseTable.builder()
            .databaseId(databaseId)
            .tableId(tableId)
            .clusterId("test-cluster")
            .tableUri(String.format("test-cluster.%s.%s", databaseId, tableId))
            .tableUUID(UUID.randomUUID().toString())
            .tableLocation(
                String.format("/base/%s/%s-uuid/00001-x.metadata.json", databaseId, tableId))
            .tableVersion("INITIAL_VERSION")
            .entityType(entityType)
            .build());
    seededPointerKeys.add(
        HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(tableId).build());
  }

  /**
   * Canonical database fixture: seven databases with exactly one pointer each; three of them hold
   * only a view. Only the four table databases may be listed.
   */
  private void seedCanonicalDatabases() {
    seedRawPointer("db00_legacy", "t1", null);
    seedRawPointer("db01_view_only", "t1", "VIEW");
    seedRawPointer("db02_explicit", "t1", "TABLE");
    seedRawPointer("db03_view_only", "t1", "VIEW");
    seedRawPointer("db04_legacy", "t1", null);
    seedRawPointer("db05_view_only", "t1", "VIEW");
    seedRawPointer("db06_explicit", "t1", "TABLE");
  }

  /**
   * The two database-listing tests below assert a GLOBAL result count, so a row leaked by another
   * method in this class would make them fail for an unrelated reason. Asserting the precondition
   * up front keeps that failure diagnosable as leakage rather than as a filtering bug.
   */
  private void assertPointerTableIsEmpty() {
    List<HouseTable> existing = new ArrayList<>();
    houseTablesRepository.findAll().forEach(existing::add);
    Assertions.assertTrue(
        existing.isEmpty(),
        "This test asserts a global database count and requires a clean pointer table; "
            + "a previous test leaked rows: "
            + existing.stream()
                .map(h -> h.getDatabaseId() + "." + h.getTableId())
                .collect(Collectors.toList()));
  }

  @Test
  public void testGetAllDatabasesExcludesViewOnlyDatabases() throws Exception {
    assertPointerTableIsEmpty();
    seedCanonicalDatabases();

    mvc.perform(
            MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.results", hasSize(4)))
        .andExpect(
            jsonPath(
                "$.results[*].databaseId",
                containsInAnyOrder("db00_legacy", "db02_explicit", "db04_legacy", "db06_explicit")))
        .andExpect(jsonPath("$.results[*].databaseId", not(hasItem("db01_view_only"))))
        .andExpect(jsonPath("$.results[*].databaseId", not(hasItem("db03_view_only"))))
        .andExpect(jsonPath("$.results[*].databaseId", not(hasItem("db05_view_only"))));
  }

  /**
   * Anti-post-filter assertion for the paginated database listing: filtering the returned page
   * would report totalElements=7/totalPages=4 with a 1-row first page.
   */
  @Test
  public void testGetAllDatabasesFiltersBeforePagination() throws Exception {
    assertPointerTableIsEmpty();
    seedCanonicalDatabases();

    mvc.perform(
            MockMvcRequestBuilders.get("/v2/databases")
                .param("page", "0")
                .param("size", "2")
                .param("sortBy", "databaseId")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.pageResults.totalElements", is(4)))
        .andExpect(jsonPath("$.pageResults.totalPages", is(2)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(2)))
        .andExpect(jsonPath("$.pageResults.content[0].databaseId", is("db00_legacy")))
        .andExpect(jsonPath("$.pageResults.content[1].databaseId", is("db02_explicit")));

    mvc.perform(
            MockMvcRequestBuilders.get("/v2/databases")
                .param("page", "1")
                .param("size", "2")
                .param("sortBy", "databaseId")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageResults.totalElements", is(4)))
        .andExpect(jsonPath("$.pageResults.totalPages", is(2)))
        .andExpect(jsonPath("$.pageResults.content", hasSize(2)))
        .andExpect(jsonPath("$.pageResults.content[0].databaseId", is("db04_legacy")))
        .andExpect(jsonPath("$.pageResults.content[1].databaseId", is("db06_explicit")));
  }
}
