package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX;
import static com.linkedin.openhouse.tables.model.DatabaseModelConstants.GET_DATABASE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.DatabaseModelConstants.GET_DATABASE_RESPONSE_BODY_DIFF_DB;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY_DIFF_DB;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY_SAME_DB;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.mock.RequestConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import java.util.ArrayList;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
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
}
