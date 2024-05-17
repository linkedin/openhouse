package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;
import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.*;
import static com.linkedin.openhouse.tables.model.DatabaseModelConstants.*;
import static com.linkedin.openhouse.tables.model.ServiceAuditModelConstants.*;
import static com.linkedin.openhouse.tables.model.TableAuditModelConstants.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildGetTableResponseBody;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.PolicyTag;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.RetentionColumnPattern;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.ServiceAuditModelConstants;
import com.linkedin.openhouse.tables.model.TableAuditModelConstants;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.types.Types;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@WithMockUser(username = "testUser")
public class TablesControllerTest {

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired Catalog catalog;

  @Autowired MockMvc mvc;

  @Autowired StorageManager storageManager;

  @Captor private ArgumentCaptor<ServiceAuditEvent> argCaptorServiceAudit;

  @Captor private ArgumentCaptor<TableAuditEvent> argCaptorTableAudit;

  @Autowired private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  @Autowired private AuditHandler<TableAuditEvent> tableAuditHandler;

  @Test
  public void testCrudTables() throws Exception {

    // Create tables
    MvcResult mvcResultT1d1 =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY, mvc, storageManager);
    MvcResult mvcResultT2d1 =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY_SAME_DB, mvc, storageManager);
    MvcResult mvcResultT1d2 =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY_DIFF_DB, mvc, storageManager);

    String tableLocation = RequestAndValidateHelper.obtainTableLocationFromMvcResult(mvcResultT1d1);
    String tableSameDbLocation =
        RequestAndValidateHelper.obtainTableLocationFromMvcResult(mvcResultT2d1);
    String tableDiffDbLocation =
        RequestAndValidateHelper.obtainTableLocationFromMvcResult(mvcResultT1d2);

    // Sending the same object for update should expect no new object returned and status code being
    // 200.
    RequestAndValidateHelper.updateTableAndValidateResponse(
        mvc,
        storageManager,
        buildGetTableResponseBody(mvcResultT1d1),
        INITIAL_TABLE_VERSION,
        false);
    RequestAndValidateHelper.updateTableAndValidateResponse(
        mvc,
        storageManager,
        buildGetTableResponseBody(mvcResultT2d1),
        INITIAL_TABLE_VERSION,
        false);
    RequestAndValidateHelper.updateTableAndValidateResponse(
        mvc,
        storageManager,
        buildGetTableResponseBody(mvcResultT1d2),
        INITIAL_TABLE_VERSION,
        false);

    // Sending the object with updated schema, expecting version moving ahead.
    // Creating a container GetTableResponseBody to update schema ONLY
    RequestAndValidateHelper.updateTableAndValidateResponse(
        mvc, storageManager, evolveDummySchema(mvcResultT1d1), tableLocation);
    RequestAndValidateHelper.updateTableAndValidateResponse(
        mvc, storageManager, evolveDummySchema(mvcResultT2d1), tableSameDbLocation);
    RequestAndValidateHelper.updateTableAndValidateResponse(
        mvc, storageManager, evolveDummySchema(mvcResultT1d2), tableDiffDbLocation);

    RequestAndValidateHelper.listAllAndValidateResponse(
        mvc,
        GET_TABLE_RESPONSE_BODY.getDatabaseId(),
        Arrays.asList(GET_TABLE_RESPONSE_BODY, GET_TABLE_RESPONSE_BODY_SAME_DB));
    RequestAndValidateHelper.listAllAndValidateResponse(
        mvc,
        GET_TABLE_RESPONSE_BODY_DIFF_DB.getDatabaseId(),
        Arrays.asList(GET_TABLE_RESPONSE_BODY_DIFF_DB));

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY_SAME_DB);
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY_DIFF_DB);
  }

  @Test
  public void testUpdateProperties() throws Exception {
    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY, mvc, storageManager);

    Map<String, String> baseTblProps = new HashMap<>();
    baseTblProps.putAll(TABLE_PROPS);

    // add a prop
    GetTableResponseBody container =
        /* null value makes buildGetTableResponseBody to set return tblproperties from previous response;
         * We will then update properties afterwards which will effectively making it get-modify-put.
         * The "container" object is a pattern to essentially create a deep copy of GetTableResponseBody that is immutable.
         */
        GetTableResponseBody.builder().tableProperties(null).build();
    GetTableResponseBody addProp = buildGetTableResponseBody(mvcResult, container);
    addProp.getTableProperties().put("user.new", "value");
    mvcResult = RequestAndValidateHelper.updateTablePropsAndValidateResponse(mvc, addProp);

    // alter a prop.
    container = GetTableResponseBody.builder().tableProperties(null).build();
    GetTableResponseBody alteredProp = buildGetTableResponseBody(mvcResult, container);
    alteredProp.getTableProperties().put("user.new", "b1");
    mvcResult = RequestAndValidateHelper.updateTablePropsAndValidateResponse(mvc, alteredProp);

    // unset a prop: drop user.new
    container = GetTableResponseBody.builder().tableProperties(null).build();
    GetTableResponseBody unsetProp = buildGetTableResponseBody(mvcResult, container);
    unsetProp.getTableProperties().remove("user.new");
    mvcResult = RequestAndValidateHelper.updateTablePropsAndValidateResponse(mvc, unsetProp);

    // Below requests are meant to be failed
    // add a prop under openhouse namespace
    container = GetTableResponseBody.builder().tableProperties(null).build();
    GetTableResponseBody addOpenhouseProp = buildGetTableResponseBody(mvcResult, container);
    addOpenhouseProp.getTableProperties().put("openhouse.new", "value");
    RequestAndValidateHelper.updateTableWithReservedPropsAndValidateResponse(
        mvc, addOpenhouseProp, "openhouse.new");

    // alter a prop under openhouse namespace
    container = GetTableResponseBody.builder().tableProperties(null).build();
    GetTableResponseBody alterOpenhouseProp = buildGetTableResponseBody(mvcResult, container);
    alterOpenhouseProp.getTableProperties().put("openhouse.key", "t2");
    RequestAndValidateHelper.updateTableWithReservedPropsAndValidateResponse(
        mvc, alterOpenhouseProp, null);

    // drop a prop under openhouse namespace
    container = GetTableResponseBody.builder().tableProperties(null).build();
    GetTableResponseBody dropOpenhouseProp = buildGetTableResponseBody(mvcResult, container);
    dropOpenhouseProp.getTableProperties().remove("openhouse.clusterId");
    RequestAndValidateHelper.updateTableWithReservedPropsAndValidateResponse(
        mvc, dropOpenhouseProp, "openhouse.clusterId");

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testInvalidURLNotThrow404() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/not_found/tabless/not_found") /* tabless is a deliberate typo */
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testReadNotFoundTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/not_found/tables/not_found")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testDeleteNotFoundTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/not_found/tables/not_found")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testEmptyDatabase() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/not_found/tables/")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    GetAllTablesResponseBody.builder()
                        .results(new ArrayList<>())
                        .build()
                        .toJson()));
  }

  @Test
  public void testCreateTableAlreadyExists() throws Exception {
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY, mvc, storageManager);

    mvc.perform(
            MockMvcRequestBuilders.post(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/",
                        GET_TABLE_RESPONSE_BODY.getDatabaseId()))
                .contentType(MediaType.APPLICATION_JSON)
                .content(buildCreateUpdateTableRequestBody(GET_TABLE_RESPONSE_BODY).toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isConflict())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.CONFLICT.getReasonPhrase()))))
        .andExpect(
            jsonPath(
                "$.message",
                is(
                    equalTo(
                        String.format(
                            "Table %s.%s already exists",
                            GET_TABLE_RESPONSE_BODY.getDatabaseId(),
                            GET_TABLE_RESPONSE_BODY.getTableId())))))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.CONFLICT.getReasonPhrase()))));

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testCrudTablesWithPartitioning() throws Exception {
    Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "stringId", Types.StringType.get()),
            Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone()));
    String schemaJson = getSchemaJsonFromSchema(schema);
    TimePartitionSpec timePartitionSpec =
        TimePartitionSpec.builder()
            .columnName("timestampCol")
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .build();
    GetTableResponseBody getTableResponseBodyWithPartitioning =
        GET_TABLE_RESPONSE_BODY
            .toBuilder()
            .schema(schemaJson)
            .timePartitioning(timePartitionSpec)
            .clustering(Arrays.asList(ClusteringColumn.builder().columnName("stringId").build()))
            .build();

    MvcResult previousMvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            getTableResponseBodyWithPartitioning, mvc, storageManager);

    GetTableResponseBody getTableResponseBody = GetTableResponseBody.builder().build();
    RequestAndValidateHelper.updateTableAndValidateResponse(
        mvc,
        storageManager,
        // There's no actual updates for this check and it is just updating the partitioning fields
        buildGetTableResponseBody(previousMvcResult, getTableResponseBody),
        INITIAL_TABLE_VERSION,
        false);

    RequestAndValidateHelper.deleteTableAndValidateResponse(
        mvc, getTableResponseBodyWithPartitioning);
  }

  @Test
  public void testPartitioningValidationNullColumnName() throws Exception {
    GetTableResponseBody getTableResponseBodyWithPartitioning =
        GET_TABLE_RESPONSE_BODY
            .toBuilder()
            .timePartitioning(
                TimePartitionSpec.builder()
                    // null columnName
                    .granularity(TimePartitionSpec.Granularity.HOUR)
                    .build())
            .build();

    mvc.perform(
            MockMvcRequestBuilders.post(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/",
                        GET_TABLE_RESPONSE_BODY.getDatabaseId()))
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    buildCreateUpdateTableRequestBody(getTableResponseBodyWithPartitioning)
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(jsonPath("$.message", containsString("columnName cannot be empty")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))));
  }

  @Test
  public void testPartitioningValidationNullGranularity() throws Exception {
    GetTableResponseBody getTableResponseBodyWithPartitioning =
        GET_TABLE_RESPONSE_BODY
            .toBuilder()
            .timePartitioning(TimePartitionSpec.builder().columnName("timestampCol").build())
            .build();

    mvc.perform(
            MockMvcRequestBuilders.post(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/",
                        GET_TABLE_RESPONSE_BODY.getDatabaseId()))
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    buildCreateUpdateTableRequestBody(getTableResponseBodyWithPartitioning)
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(jsonPath("$.message", containsString("granularity cannot be null")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))));
  }

  @Test
  public void testPartitioningValidationForValidGranularity() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/",
                        GET_TABLE_RESPONSE_BODY.getDatabaseId()))
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    buildCreateUpdateTableRequestBody(GET_TABLE_RESPONSE_BODY)
                        .toJson()
                        .replace("HOUR", "MINUTE"))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(
            jsonPath(
                "$.message", containsString("The value must be one of: [HOUR, DAY, MONTH, YEAR].")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))));
  }

  @Test
  public void testCreateTableWithNonNullFieldBeingNull() throws Exception {

    // tblprops set to null which is invalid.
    mvc.perform(
            MockMvcRequestBuilders.post(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/",
                        GET_TABLE_RESPONSE_BODY_NULL_PROP.getDatabaseId()))
                .contentType(MediaType.APPLICATION_JSON)
                .content(GET_TABLE_RESPONSE_BODY_NULL_PROP.toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(
            jsonPath(
                "$.message",
                containsString("CreateUpdateTableRequestBody.tableProperties : must not be null")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))));
  }

  @Test
  public void testPoliciesValidationForNullPolicies() throws Exception {
    GetTableResponseBody getTableResponseBodyWithNullPolicies =
        GET_TABLE_RESPONSE_BODY.toBuilder().policies(null).build();
    CreateUpdateTableRequestBody requestWithPoliciesNull =
        buildCreateUpdateTableRequestBody(getTableResponseBodyWithNullPolicies)
            .toBuilder()
            .baseTableVersion(INITIAL_TABLE_VERSION)
            .build();
    MvcResult result =
        mvc.perform(
                MockMvcRequestBuilders.post(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/",
                            GET_TABLE_RESPONSE_BODY.getDatabaseId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestWithPoliciesNull.toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andReturn();
    String policiesInResponse =
        JsonPath.read(result.getResponse().getContentAsString(), "$.policies");
    Assertions.assertNull(policiesInResponse);
    RequestAndValidateHelper.deleteTableAndValidateResponse(
        mvc, getTableResponseBodyWithNullPolicies);
  }

  @Test
  public void testUpdatePolicies() throws Exception {
    // Create tables with string-type partition(cluster) column but not typical
    // timestamp-partitioned,
    // so that we can provide modification on retention column pattern.
    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY
                .toBuilder()
                .timePartitioning(null)
                .policies(TABLE_POLICIES_COMPLEX)
                .build(),
            mvc,
            storageManager);

    LinkedHashMap<String, LinkedHashMap> currentPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");

    Retention retention =
        Retention.builder()
            .count(4)
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .columnPattern(
                RetentionColumnPattern.builder()
                    .pattern("yyyy-MM-dd-HH")
                    .columnName("timestampCol")
                    .build())
            .build();
    Policies newPolicies = Policies.builder().retention(retention).build();
    GetTableResponseBody container = GetTableResponseBody.builder().policies(newPolicies).build();
    GetTableResponseBody changedPolicies = buildGetTableResponseBody(mvcResult, container);
    mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s",
                            changedPolicies.getDatabaseId(),
                            changedPolicies.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(buildCreateUpdateTableRequestBody(changedPolicies).toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn();

    LinkedHashMap<String, LinkedHashMap> updatedPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");
    Assertions.assertNotEquals(
        currentPolicies.get("retention").get("count"),
        updatedPolicies.get("retention").get("count"));
    Assertions.assertEquals(updatedPolicies.get("retention").get("count"), 4);
    Assertions.assertEquals(
        ((HashMap) updatedPolicies.get("retention").get("columnPattern")).get("columnName"),
        "timestampCol");
    Assertions.assertEquals(
        ((HashMap) updatedPolicies.get("retention").get("columnPattern")).get("pattern"),
        "yyyy-MM-dd-HH");

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testCreateRequestSucceedsForNullRetentionObject() throws Exception {
    GetTableResponseBody responseBodyWithNullPolicies =
        GET_TABLE_RESPONSE_BODY
            .toBuilder()
            .policies(Policies.builder().retention(null).build())
            .build();

    MvcResult mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s",
                            responseBodyWithNullPolicies.getDatabaseId(),
                            responseBodyWithNullPolicies.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        buildCreateUpdateTableRequestBody(responseBodyWithNullPolicies)
                            .toBuilder()
                            .baseTableVersion(INITIAL_TABLE_VERSION)
                            .build()
                            .toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isCreated())
            .andReturn();
    LinkedHashMap<String, LinkedHashMap> updatedPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");
    Assertions.assertNull(updatedPolicies.get("retention"));
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testCreateRequestFailsForWithNullGranularityInRetentionObject() throws Exception {
    Retention retention = Retention.builder().count(3).build();
    GetTableResponseBody responseBodyWithNullPolicies =
        TableModelConstants.buildGetTableResponseBodyWithPolicy(
            GET_TABLE_RESPONSE_BODY, Policies.builder().retention(retention).build());

    ResultActions rs =
        mvc.perform(
            MockMvcRequestBuilders.put(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/%s/tables/%s",
                        responseBodyWithNullPolicies.getDatabaseId(),
                        responseBodyWithNullPolicies.getTableId()))
                .contentType(MediaType.APPLICATION_JSON)
                .content(buildCreateUpdateTableRequestBody(responseBodyWithNullPolicies).toJson())
                .accept(MediaType.APPLICATION_JSON));

    rs.andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(
            jsonPath(
                "$.message",
                containsString(
                    "CreateUpdateTableRequestBody.policies.retention.granularity : Incorrect granularity specified. retention.granularity cannot be null")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))))
        .andReturn();
  }

  @Test
  public void testCreateRequestFailsForWithNullCountInRetentionObject() throws Exception {
    Retention retention =
        Retention.builder().granularity(TimePartitionSpec.Granularity.DAY).build();
    GetTableResponseBody responseBodyWithNullPolicies =
        TableModelConstants.buildGetTableResponseBodyWithPolicy(
            GET_TABLE_RESPONSE_BODY, Policies.builder().retention(retention).build());

    ResultActions rs =
        mvc.perform(
            MockMvcRequestBuilders.put(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/%s/tables/%s",
                        responseBodyWithNullPolicies.getDatabaseId(),
                        responseBodyWithNullPolicies.getTableId()))
                .contentType(MediaType.APPLICATION_JSON)
                .content(buildCreateUpdateTableRequestBody(responseBodyWithNullPolicies).toJson())
                .accept(MediaType.APPLICATION_JSON));

    rs.andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(
            jsonPath(
                "$.message",
                containsString(
                    "CreateUpdateTableRequestBody.policies.retention.count : Incorrect count specified. retention.count has to be a positive integer")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))))
        .andReturn();
  }

  @Test
  public void testCreateRequestFailsForWithGranularityDifferentFromTimePartitionSpec()
      throws Exception {
    Retention retention =
        Retention.builder().granularity(TimePartitionSpec.Granularity.YEAR).count(4).build();
    GetTableResponseBody responseBodyWithPolicies =
        TableModelConstants.buildGetTableResponseBodyWithPolicy(
            GET_TABLE_RESPONSE_BODY, Policies.builder().retention(retention).build());

    ResultActions rs =
        mvc.perform(
            MockMvcRequestBuilders.put(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/%s/tables/%s",
                        responseBodyWithPolicies.getDatabaseId(),
                        responseBodyWithPolicies.getTableId()))
                .contentType(MediaType.APPLICATION_JSON)
                .content(buildCreateUpdateTableRequestBody(responseBodyWithPolicies).toJson())
                .accept(MediaType.APPLICATION_JSON));

    rs.andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(
            jsonPath(
                "$.message",
                containsString(
                    "Policies granularity must be equal to or lesser than time partition spec granularity")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))))
        .andReturn();
  }

  @Test
  public void testCreateTableWithTableType() throws Exception {
    // Create tables with tableType set in CreateUpdateTableRequest
    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY_WITH_TABLE_TYPE, mvc, storageManager);

    String tableType = JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.tableType");
    // Sending the same object for update should expect no new object returned and status code being
    // 200.
    RequestAndValidateHelper.updateTableAndValidateResponse(
        mvc, storageManager, buildGetTableResponseBody(mvcResult), INITIAL_TABLE_VERSION, false);
    Assertions.assertEquals(
        tableType, GET_TABLE_RESPONSE_BODY_WITH_TABLE_TYPE.getTableType().toString());
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testCreateTableWithIncorrectTableTypeThrowsException() throws Exception {
    String requestJson =
        buildCreateUpdateTableRequestBody(GET_TABLE_RESPONSE_BODY_WITH_TABLE_TYPE).toJson();
    String invalidTableType = "invalid";
    // Convert JSON string to JSONObject
    JSONObject jsonObject = new JSONObject(requestJson);
    // Update the 'tableType' field
    jsonObject.put("tableType", invalidTableType);
    String updatedJsonStr = jsonObject.toString();
    ResultActions rs =
        mvc.perform(
            MockMvcRequestBuilders.put(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/%s/tables/%s",
                        GET_TABLE_RESPONSE_BODY_WITH_TABLE_TYPE.getDatabaseId(),
                        GET_TABLE_RESPONSE_BODY_WITH_TABLE_TYPE.getTableId()))
                .contentType(MediaType.APPLICATION_JSON)
                .content(updatedJsonStr)
                .accept(MediaType.APPLICATION_JSON));

    rs.andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(
            jsonPath(
                "$.message",
                containsString(
                    String.format(
                        "Invalid enum value: '%s' for the field: 'tableType'. The value must be one of: [%s].",
                        invalidTableType, StringUtils.join(TableType.values(), ", ")))))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))))
        .andReturn();
  }

  @Test
  public void testGetAllDatabases() throws Exception {
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY, mvc, storageManager);
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY_SAME_DB, mvc, storageManager);
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY_DIFF_DB, mvc, storageManager);

    mvc.perform(
            MockMvcRequestBuilders.get(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/")
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

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY_SAME_DB);
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY_DIFF_DB);
  }

  @Test
  public void testGetAllDatabasesEmptyResult() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/")
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

  @SneakyThrows
  @Test
  public void testStagedCreateDoesntExistInConsecutiveCalls() {
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY, mvc, storageManager, true);
    // Staged table should not exist
    mvc.perform(
            MockMvcRequestBuilders.get(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/%s/tables/%s",
                        GET_TABLE_RESPONSE_BODY.getDatabaseId(),
                        GET_TABLE_RESPONSE_BODY.getTableId()))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testServiceAuditGetTableSucceed() throws Exception {
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY, mvc, storageManager);
    mvc.perform(
        MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d1/tables/t1")
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(serviceAuditHandler, atLeastOnce()).audit(argCaptorServiceAudit.capture());
    ServiceAuditEvent actualEvent = argCaptorServiceAudit.getValue();
    assertTrue(
        new ReflectionEquals(
                SERVICE_AUDIT_EVENT_END_TO_END, ServiceAuditModelConstants.EXCLUDE_FIELDS)
            .matches(actualEvent));
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testTableAuditSucceed() throws Exception {
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY, mvc, storageManager);
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptorTableAudit.capture());
    TableAuditEvent actualEvent = argCaptorTableAudit.getValue();
    assertTrue(
        new ReflectionEquals(
                TABLE_AUDIT_EVENT_CREATE_TABLE_SUCCESS_E2E, TableAuditModelConstants.EXCLUDE_FIELDS)
            .matches(actualEvent));
    assertNotNull(actualEvent.getCurrentTableRoot());
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testSearchTablesWithDatabaseId() throws Exception {
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY, mvc, storageManager);
    RequestAndValidateHelper.createTableAndValidateResponse(
        GET_TABLE_RESPONSE_BODY_SAME_DB, mvc, storageManager);

    mvc.perform(
            MockMvcRequestBuilders.post(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/%s/tables/search",
                        GET_TABLE_RESPONSE_BODY.getDatabaseId()))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    GetAllTablesResponseBody.builder()
                        .results(
                            new ArrayList<>(
                                Arrays.asList(
                                    GET_TABLE_RESPONSE_BODY_IDENTIFIER,
                                    GET_TABLE_RESPONSE_BODY_SAME_DB_IDENTIFIER)))
                        .build()
                        .toJson()));

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY_SAME_DB);
  }

  @Test
  public void testUpdateSucceedsForColumnTags() throws Exception {
    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY, mvc, storageManager);

    LinkedHashMap<String, LinkedHashMap> currentPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");

    PolicyTag policyTags =
        PolicyTag.builder().tags(new HashSet<>(Arrays.asList(PolicyTag.Tag.PII))).build();
    Map<String, PolicyTag> columnTags =
        new HashMap() {
          {
            put("col1", policyTags);
          }
        };
    Policies newPolicies = Policies.builder().columnTags(columnTags).build();
    GetTableResponseBody container = GetTableResponseBody.builder().policies(newPolicies).build();
    GetTableResponseBody addProp = buildGetTableResponseBody(mvcResult, container);
    mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s",
                            addProp.getDatabaseId(),
                            addProp.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(buildCreateUpdateTableRequestBody(addProp).toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn();

    LinkedHashMap<String, LinkedHashMap> updatedPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");

    Assertions.assertNotEquals(currentPolicies, updatedPolicies);
    Assertions.assertTrue(updatedPolicies.get("columnTags").containsKey("col1"));
    Assertions.assertEquals(
        ((HashMap) updatedPolicies.get("columnTags").get("col1")).get("tags").toString(),
        "[\"PII\"]");
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testCreateRequestSucceedsForNullColumnTags() throws Exception {
    GetTableResponseBody responseBodyWithNullPolicies =
        GET_TABLE_RESPONSE_BODY
            .toBuilder()
            .policies(Policies.builder().columnTags(null).build())
            .build();

    MvcResult mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s",
                            responseBodyWithNullPolicies.getDatabaseId(),
                            responseBodyWithNullPolicies.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        buildCreateUpdateTableRequestBody(responseBodyWithNullPolicies)
                            .toBuilder()
                            .baseTableVersion(INITIAL_TABLE_VERSION)
                            .build()
                            .toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isCreated())
            .andReturn();
    LinkedHashMap<String, LinkedHashMap> updatedPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");
    Assertions.assertNull(updatedPolicies.get("columnTags"));
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }
}
