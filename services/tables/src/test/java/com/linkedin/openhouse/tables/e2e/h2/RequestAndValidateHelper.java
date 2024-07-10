package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.common.TableType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.directory.api.util.Strings;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;

/**
 * The helper utility class that stores high-level helper methods for validation in a "stateful"
 * fashion, meaning that validation taking previous responseBody and use it towards subsequent
 * request is a common pattern.
 */
public class RequestAndValidateHelper {

  static String getCurrentTableLocation(MockMvc mvc, String databaseId, String tableId)
      throws Exception {
    MvcResult getTableResult =
        mvc.perform(
                MockMvcRequestBuilders.get(
                        String.format(
                            CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/%s",
                            databaseId,
                            tableId))
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON))
            .andReturn();
    return obtainTableLocationFromMvcResult(getTableResult);
  }

  static String obtainTableLocationFromMvcResult(MvcResult mvcResult) throws Exception {
    return JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.tableLocation");
  }

  static void updateTableAndValidateResponse(
      MockMvc mvc,
      StorageManager storageManager,
      GetTableResponseBody getTableResponseBody,
      String previousTableLocation,
      boolean trueUpdate)
      throws Exception {
    MvcResult mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s",
                            getTableResponseBody.getDatabaseId(),
                            getTableResponseBody.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(buildCreateUpdateTableRequestBody(getTableResponseBody).toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andDo(MockMvcResultHandlers.print())
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.tableId", is(equalTo(getTableResponseBody.getTableId()))))
            .andExpect(jsonPath("$.databaseId", is(equalTo(getTableResponseBody.getDatabaseId()))))
            .andExpect(jsonPath("$.clusterId", is(equalTo(getTableResponseBody.getClusterId()))))
            .andExpect(jsonPath("$.tableUri", is(equalTo(getTableResponseBody.getTableUri()))))
            .andExpect(jsonPath("$.tableVersion", is(equalTo(previousTableLocation))))
            .andReturn();
    ValidationUtilities.validateUUID(mvcResult, getTableResponseBody.getTableUUID());
    ValidationUtilities.validateSchema(mvcResult, getTableResponseBody.getSchema());
    validateWritableTableProperties(mvcResult, getTableResponseBody.getTableProperties());
    ValidationUtilities.validateLocation(
        mvcResult, storageManager.getDefaultStorage().getClient().getRootPrefix());

    ValidationUtilities.validateTimestamp(
        mvcResult,
        getTableResponseBody.getLastModifiedTime(),
        getTableResponseBody.getCreationTime(),
        trueUpdate);
  }

  static void updateTableAndValidateResponse(
      MockMvc mvc,
      StorageManager storageManager,
      GetTableResponseBody getTableResponseBody,
      String previousTableLocation)
      throws Exception {
    updateTableAndValidateResponse(
        mvc, storageManager, getTableResponseBody, previousTableLocation, true);
  }

  /**
   * // TODO: Replace with updateAndValidate
   *
   * <p>This method validate updateTable request's result and return the raw {@link MvcResult} to be
   * chained with other requests. It has both side effect and return type to make it effective for
   * the get-modify-put pattern.
   *
   * @param getTableResponseBody represents the source of information to assemble request body, with
   *     previous responseBody as a way to chain multiple requests.
   */
  static MvcResult updateTablePropsAndValidateResponse(
      MockMvc mvc, GetTableResponseBody getTableResponseBody) throws Exception {
    MvcResult mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.get(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s",
                            getTableResponseBody.getDatabaseId(),
                            getTableResponseBody.getTableId()))
                    .accept(MediaType.APPLICATION_JSON))
            .andReturn();

    Map<String, String> previousProps =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.tableProperties");

    // Overwrite the previous props and drop keys that are no longer appearing in provided map
    // which is the equivalence of the put operation in backend,
    Map<String, String> updatedProps = new HashMap<>();
    updatedProps.putAll(previousProps);
    updatedProps.putAll(getTableResponseBody.getTableProperties());
    previousProps.forEach(
        (k, v) -> {
          if (!getTableResponseBody.getTableProperties().containsKey(k)) {
            updatedProps.remove(k);
          }
        });

    mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s",
                            getTableResponseBody.getDatabaseId(),
                            getTableResponseBody.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(buildCreateUpdateTableRequestBody(getTableResponseBody).toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andReturn();

    Map<String, String> newProps =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.tableProperties");

    Assertions.assertTrue(
        ValidationUtilities.tblPropsResponseComparisonHelper(updatedProps, newProps));
    ValidationUtilities.validateUUID(mvcResult, getTableResponseBody.getTableUUID());
    return mvcResult;
  }

  static MvcResult createTableAndValidateResponse(
      GetTableResponseBody getTableResponseBody, MockMvc mockMvc, StorageManager storageManager)
      throws Exception {
    return createTableAndValidateResponse(getTableResponseBody, mockMvc, storageManager, false);
  }

  static MvcResult createTableAndValidateResponse(
      GetTableResponseBody getTableResponseBody,
      MockMvc mockMvc,
      StorageManager storageManager,
      boolean stageCreate)
      throws Exception {
    MvcResult result =
        mockMvc
            .perform(
                MockMvcRequestBuilders.post(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/",
                            getTableResponseBody.getDatabaseId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        buildCreateUpdateTableRequestBody(getTableResponseBody, stageCreate)
                            .toBuilder()
                            .baseTableVersion(INITIAL_TABLE_VERSION)
                            .build()
                            .toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isCreated())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.tableId", is(equalTo(getTableResponseBody.getTableId()))))
            .andExpect(jsonPath("$.databaseId", is(equalTo(getTableResponseBody.getDatabaseId()))))
            .andExpect(jsonPath("$.clusterId", is(equalTo(getTableResponseBody.getClusterId()))))
            .andExpect(jsonPath("$.tableUri", is(equalTo(getTableResponseBody.getTableUri()))))
            .andExpect(jsonPath("$.tableVersion", is(equalTo(INITIAL_TABLE_VERSION))))
            .andExpect(
                jsonPath(
                    "$.tableType",
                    is(
                        equalTo(
                            getTableResponseBody.getTableType() != null
                                ? getTableResponseBody.getTableType().toString()
                                : TableType.PRIMARY_TABLE.toString()))))
            .andExpect(
                jsonPath("$.tableCreator", is(equalTo(getTableResponseBody.getTableCreator()))))
            .andReturn();
    ValidationUtilities.validateSchema(result, getTableResponseBody.getSchema());
    validateWritableTableProperties(result, getTableResponseBody.getTableProperties());
    ValidationUtilities.validatePolicies(result, getTableResponseBody.getPolicies());
    ValidationUtilities.validateLocation(
        result, storageManager.getDefaultStorage().getClient().getRootPrefix());
    Assertions.assertTrue(
        (Long) JsonPath.read(result.getResponse().getContentAsString(), "$.creationTime") > 0);
    // Return result if validation all passed.
    // This is useful in case there are follow-up steps after table creation that requires
    // information
    // from the response body
    return result;
  }

  // TODO: Replace with updateAndValidate
  static void updateTableWithReservedPropsAndValidateResponse(
      MockMvc mvc, GetTableResponseBody getTableResponseBody, String badKey) throws Exception {
    MvcResult mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s",
                            getTableResponseBody.getDatabaseId(),
                            getTableResponseBody.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(buildCreateUpdateTableRequestBody(getTableResponseBody).toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andReturn();

    String returnMsg =
        new Gson()
            .fromJson(mvcResult.getResponse().getContentAsString(), JsonObject.class)
            .get("message")
            .getAsString();
    assertTrue(returnMsg.contains("Bad tblproperties provided"));

    if (!Strings.isEmpty(badKey)) {
      // Digging into the bad props a bit more if badKey appears
      String remainingSubstring =
          returnMsg.substring(
              returnMsg.indexOf("provided table properties:")
                  + "provided table properties:".length());
      JsonObject mapDiff = new Gson().fromJson(remainingSubstring, JsonObject.class);
      Assertions.assertTrue(
          /* These are binding to the implementation of errorMsg which is using Maps.diff*/
          ((JsonObject) (mapDiff.get("onlyOnRight"))).has(badKey)
              || ((JsonObject) (mapDiff.get("onlyOnLeft"))).has(badKey));
    }
  }

  static void listAllAndValidateResponse(
      MockMvc mvc, String databaseId, List<GetTableResponseBody> inputTableList) throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/",
                        databaseId))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(MockMvcResultHandlers.print())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.results", hasSize(inputTableList.size())))
        .andExpect(
            jsonPath(
                "$.results[*].tableId",
                oneOf(
                    inputTableList.stream()
                        .map(GetTableResponseBody::getTableId)
                        .collect(Collectors.toList()))))
        .andExpect(
            jsonPath(
                "$.results[*].databaseId",
                oneOf(
                    inputTableList.stream()
                        .map(GetTableResponseBody::getDatabaseId)
                        .collect(Collectors.toList()))))
        .andExpect(
            jsonPath(
                "$.results[*].clusterId",
                oneOf(
                    inputTableList.stream()
                        .map(GetTableResponseBody::getClusterId)
                        .collect(Collectors.toList()))))
        .andExpect(
            jsonPath(
                "$.results[*].tableUri",
                oneOf(
                    inputTableList.stream()
                        .map(GetTableResponseBody::getTableUri)
                        .collect(Collectors.toList()))));
  }

  static void deleteTableAndValidateResponse(MockMvc mvc, GetTableResponseBody getTableResponseBody)
      throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.delete(
                String.format(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/%s",
                    getTableResponseBody.getDatabaseId(),
                    getTableResponseBody.getTableId())))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));
  }

  static MvcResult putSnapshotsAndValidateResponse(
      Catalog catalog,
      MockMvc mvc,
      IcebergSnapshotsRequestBody icebergSnapshotsRequestBody,
      boolean isTableCreated)
      throws Exception {
    CreateUpdateTableRequestBody createUpdateTableRequestBody =
        icebergSnapshotsRequestBody.getCreateUpdateTableRequestBody();
    String clusterId = createUpdateTableRequestBody.getClusterId();
    String databaseId = createUpdateTableRequestBody.getDatabaseId();
    String tableId = createUpdateTableRequestBody.getTableId();
    TableType tableType =
        createUpdateTableRequestBody.getTableType() == null
            ? TableType.PRIMARY_TABLE
            : createUpdateTableRequestBody.getTableType();
    MvcResult result =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(
                            CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s/iceberg/v2/snapshots",
                            databaseId,
                            tableId))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(icebergSnapshotsRequestBody.toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(isTableCreated ? status().isCreated() : status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.tableId", is(equalTo(tableId))))
            .andExpect(jsonPath("$.databaseId", is(equalTo(databaseId))))
            .andExpect(jsonPath("$.clusterId", is(equalTo(clusterId))))
            .andExpect(jsonPath("$.tableType", is(equalTo(tableType.toString()))))
            .andExpect(
                jsonPath(
                    "$.tableVersion",
                    is(equalTo(icebergSnapshotsRequestBody.getBaseTableVersion()))))
            .andReturn();

    validateSnapshots(catalog, result, icebergSnapshotsRequestBody);
    validateMetadataInPutSnapshotsRequest(result, icebergSnapshotsRequestBody);
    return result;
  }
}
