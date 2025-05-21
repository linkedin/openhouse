package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;
import static com.linkedin.openhouse.tables.config.TablesMvcConstants.*;
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
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.History;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.PolicyTag;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.RetentionColumnPattern;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.config.TblPropsToggleRegistryBaseImpl;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.ServiceAuditModelConstants;
import com.linkedin.openhouse.tables.model.TableAuditModelConstants;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
public class TablesControllerTest {

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired Catalog catalog;

  @Autowired MockMvc mvc;

  @Autowired StorageManager storageManager;

  @Captor private ArgumentCaptor<ServiceAuditEvent> argCaptorServiceAudit;

  @Captor private ArgumentCaptor<TableAuditEvent> argCaptorTableAudit;

  @Autowired private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  @Autowired private AuditHandler<TableAuditEvent> tableAuditHandler;

  @Autowired private SimpleMeterRegistry registry;

  @Autowired private ClusterProperties clusterProperties;

  @Autowired private ToggleStatusesRepository inMemToggleStatusRepo;

  @Test
  public void testSwaggerDocsWithoutAuth() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/tables/swagger-ui/index.html").header("Authorization", ""))
        .andExpect(status().isOk());
  }

  @Test
  public void testGetDatabasesWithoutAuth401() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases")
                .header("Authorization", "")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isUnauthorized());
  }

  @Test
  @DirtiesContext
  public void testMetricsWithClientNameHeader() throws Exception {
    String anyTestClientName = clusterProperties.getAllowedClientNameValues().get(0);
    mvc.perform(
        MockMvcRequestBuilders.get(ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases")
            .header(HTTP_HEADER_CLIENT_NAME, anyTestClientName)
            .accept(MediaType.APPLICATION_JSON));
    Assertions.assertNotNull(
        this.registry
            .get("http.server.requests")
            .tags(METRIC_KEY_CLIENT_NAME, anyTestClientName)
            .timer());
  }

  @Test
  @DirtiesContext
  public void testMetricsWithClientNameHeaderList() throws Exception {
    String anyTestClientName = clusterProperties.getAllowedClientNameValues().get(1);
    mvc.perform(
        MockMvcRequestBuilders.get(ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases")
            .header(HTTP_HEADER_CLIENT_NAME, anyTestClientName)
            .accept(MediaType.APPLICATION_JSON));
    Assertions.assertNotNull(
        this.registry
            .get("http.server.requests")
            .tags(METRIC_KEY_CLIENT_NAME, anyTestClientName)
            .timer());
  }

  @Test
  @DirtiesContext
  public void testMetricsWithClientNameHeaderInvalidValue() throws Exception {
    String invalidTestClientName = "thisdoesntexist";
    Assertions.assertFalse(
        clusterProperties.getAllowedClientNameValues().contains(invalidTestClientName));
    mvc.perform(
        MockMvcRequestBuilders.get(ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases")
            .header(HTTP_HEADER_CLIENT_NAME, invalidTestClientName)
            .accept(MediaType.APPLICATION_JSON));
    Assertions.assertNotNull(
        this.registry
            .get("http.server.requests")
            .tags(METRIC_KEY_CLIENT_NAME, CLIENT_NAME_DEFAULT_VALUE)
            .timer());
  }

  @Test
  @DirtiesContext
  public void testMetricsWithClientNameHeaderNotPresentDefaultValue() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases")
            .accept(MediaType.APPLICATION_JSON));
    Assertions.assertNotNull(
        this.registry
            .get("http.server.requests")
            .tags("client_name", CLIENT_NAME_DEFAULT_VALUE)
            .timer());
  }

  @Test
  @DirtiesContext
  public void testMetricsWithClientNameHeaderNotPresentUnexpectedValue() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases")
            .accept(MediaType.APPLICATION_JSON));
    assertThrows(
        MeterNotFoundException.class,
        () ->
            this.registry
                .get("http.server.requests")
                .tags("client_name", clusterProperties.getAllowedClientNameValues().get(0))
                .timer());
  }

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

    try {
      String tableLocation =
          RequestAndValidateHelper.obtainTableLocationFromMvcResult(mvcResultT1d1);
      String tableSameDbLocation =
          RequestAndValidateHelper.obtainTableLocationFromMvcResult(mvcResultT2d1);
      String tableDiffDbLocation =
          RequestAndValidateHelper.obtainTableLocationFromMvcResult(mvcResultT1d2);

      // Sending the same object for update should expect no new object returned and status code
      // being
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
    } finally {
      RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
      RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY_SAME_DB);
      RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY_DIFF_DB);
    }
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
  public void testTblPropsThruFeatureToggle() throws Exception {
    /**
     * This is just to ensure the ToggleStatusesRepository#findById is activated to the correct
     * path.
     */
    inMemToggleStatusRepo.save(
        TableToggleStatus.builder()
            .featureId(TblPropsToggleRegistryBaseImpl.ENABLE_TBLTYPE)
            .tableId(GET_TABLE_RESPONSE_BODY.getTableId())
            .databaseId(GET_TABLE_RESPONSE_BODY.getDatabaseId())
            .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
            .build());

    GetTableResponseBody trickFeatureToggleResponseBody =
        GET_TABLE_RESPONSE_BODY
            .toBuilder()
            .tableId(GET_TABLE_RESPONSE_BODY.getTableId())
            .tableUri(
                TableUri.builder()
                    .tableId(GET_TABLE_RESPONSE_BODY.getTableId())
                    .databaseId(GET_TABLE_RESPONSE_BODY.getDatabaseId())
                    .clusterId(GET_TABLE_RESPONSE_BODY.getClusterId())
                    .build()
                    .toString())
            .build();

    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            trickFeatureToggleResponseBody, mvc, storageManager);

    // alter a prop with feature-toggle allowed, otherwise rejected
    GetTableResponseBody container = GetTableResponseBody.builder().tableProperties(null).build();
    GetTableResponseBody toggledOnProp = buildGetTableResponseBody(mvcResult, container);
    toggledOnProp.getTableProperties().put("openhouse.tableType", "REPLICA_TABLE");
    // This update will otherwise fail if feature-toggle for enable-tableType is not turned on.
    RequestAndValidateHelper.updateTablePropsAndValidateResponse(mvc, toggledOnProp);

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, trickFeatureToggleResponseBody);
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
    RequestAndValidateHelper.deleteTableAndValidateResponse(
        mvc, GET_TABLE_RESPONSE_BODY_WITH_TABLE_TYPE);
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

  @Test
  public void testUpdateSucceedsForReplicationConfig() throws Exception {
    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY, mvc, storageManager);

    LinkedHashMap<String, LinkedHashMap> currentPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");

    ReplicationConfig replicationConfig =
        ReplicationConfig.builder().destination("clusterA").interval("12H").build();
    Replication replication =
        Replication.builder().config(Arrays.asList(replicationConfig)).build();
    Policies newPolicies = Policies.builder().replication(replication).build();

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

    LinkedHashMap<String, String> updatedReplication =
        JsonPath.read(
            mvcResult.getResponse().getContentAsString(), "$.policies.replication.config[0]");

    Assertions.assertEquals(updatedReplication.get("destination"), "CLUSTERA");
    Assertions.assertEquals(updatedReplication.get("interval"), "12H");
    Assertions.assertTrue(
        RequestAndValidateHelper.validateCronSchedule(updatedReplication.get("cronSchedule")));

    Replication nullReplication = Replication.builder().config(new ArrayList<>()).build();
    Policies newPoliciesNullRepl = Policies.builder().replication(nullReplication).build();

    GetTableResponseBody newContainer =
        GetTableResponseBody.builder().policies(newPoliciesNullRepl).build();
    GetTableResponseBody addNullProp = buildGetTableResponseBody(mvcResult, newContainer);
    mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s",
                            addProp.getDatabaseId(),
                            addProp.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(buildCreateUpdateTableRequestBody(addNullProp).toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn();

    LinkedHashMap<String, String> updatedNullReplication =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies.replication");

    Assertions.assertTrue(updatedNullReplication.containsKey("config"));
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testUpdateSucceedsForReplicationAndRetention() throws Exception {
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

    ReplicationConfig replicationConfig =
        ReplicationConfig.builder().destination("clusterA").interval("").build();
    Replication replication =
        Replication.builder().config(Arrays.asList(replicationConfig)).build();
    Retention retention =
        Retention.builder()
            .count(4)
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .columnPattern(
                RetentionColumnPattern.builder()
                    .pattern("yyyy-MM-dd")
                    .columnName("timestampCol")
                    .build())
            .build();
    Policies newPolicies = Policies.builder().replication(replication).retention(retention).build();

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
    Assertions.assertEquals(updatedPolicies.get("retention").get("count"), 4);
    Assertions.assertEquals(
        ((HashMap) updatedPolicies.get("retention").get("columnPattern")).get("columnName"),
        "timestampCol");
    Assertions.assertEquals(
        ((HashMap) updatedPolicies.get("retention").get("columnPattern")).get("pattern"),
        "yyyy-MM-dd");

    LinkedHashMap<String, String> updatedReplication =
        JsonPath.read(
            mvcResult.getResponse().getContentAsString(), "$.policies.replication.config[0]");

    Assertions.assertEquals(updatedReplication.get("destination"), "CLUSTERA");
    Assertions.assertEquals(updatedReplication.get("interval"), "1D");
    Assertions.assertTrue(
        RequestAndValidateHelper.validateCronSchedule(updatedReplication.get("cronSchedule")));

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testUpdateSucceedsForMultipleReplicationConfig() throws Exception {
    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY, mvc, storageManager);

    LinkedHashMap<String, LinkedHashMap> currentPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");

    ReplicationConfig replicationConfig1 =
        ReplicationConfig.builder().destination("clusterA").interval("").build();
    ReplicationConfig replicationConfig2 =
        ReplicationConfig.builder().destination("clusterB").interval("12H").build();
    Replication replication =
        Replication.builder().config(Arrays.asList(replicationConfig1, replicationConfig2)).build();
    Policies newPolicies = Policies.builder().replication(replication).build();

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

    LinkedHashMap<String, String> updatedReplication =
        JsonPath.read(
            mvcResult.getResponse().getContentAsString(), "$.policies.replication.config[0]");

    Assertions.assertEquals(updatedReplication.get("destination"), "CLUSTERA");
    Assertions.assertEquals(updatedReplication.get("interval"), "1D");
    Assertions.assertTrue(
        RequestAndValidateHelper.validateCronSchedule(updatedReplication.get("cronSchedule")));
    updatedReplication =
        JsonPath.read(
            mvcResult.getResponse().getContentAsString(), "$.policies.replication.config[1]");

    Assertions.assertEquals(updatedReplication.get("destination"), "CLUSTERB");
    Assertions.assertEquals(updatedReplication.get("interval"), "12H");
    Assertions.assertTrue(
        RequestAndValidateHelper.validateCronSchedule(updatedReplication.get("cronSchedule")));

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testUpdateSucceedsForHistoryPolicy() throws Exception {
    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY, mvc, storageManager);

    LinkedHashMap<String, LinkedHashMap> currentPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");

    History history =
        History.builder().maxAge(3).granularity(TimePartitionSpec.Granularity.DAY).build();

    Policies newPolicies = Policies.builder().history(history).build();

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

    Assertions.assertEquals(updatedPolicies.get("history").get("maxAge"), 3);
    Assertions.assertEquals(updatedPolicies.get("history").get("granularity"), "DAY");

    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void testCreateRequestFailsWithInvalidHistoryPolicy() throws Exception {
    History history = History.builder().granularity(TimePartitionSpec.Granularity.DAY).build();
    GetTableResponseBody responseBodyWithNullPolicies =
        TableModelConstants.buildGetTableResponseBodyWithPolicy(
            GET_TABLE_RESPONSE_BODY, Policies.builder().history(history).build());

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
                    "Must define either a time based retention or count based retention for snapshots in table")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))))
        .andReturn();
  }

  @Test
  public void createSucceedsForLockPolicyOnTable() throws Exception {
    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY, mvc, storageManager);

    LinkedHashMap<String, LinkedHashMap> currentPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");
    Assertions.assertNull(currentPolicies.get("lockState"));

    mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.post(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s/lock",
                            GET_TABLE_RESPONSE_BODY.getDatabaseId(),
                            GET_TABLE_RESPONSE_BODY.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        CreateUpdateLockRequestBody.builder()
                            .locked(true)
                            .message("setting lock")
                            .build()
                            .toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isCreated())
            .andReturn();

    mvcResult =
        getTable(GET_TABLE_RESPONSE_BODY.getDatabaseId(), GET_TABLE_RESPONSE_BODY.getTableId());

    LinkedHashMap<String, LinkedHashMap> updatedPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");
    Assertions.assertEquals(updatedPolicies.get("lockState").get("locked"), true);
    Assertions.assertNotNull(updatedPolicies.get("lockState").get("creationTime"));
    RequestAndValidateHelper.deleteLockOnTableAndValidate(mvc, GET_TABLE_RESPONSE_BODY);
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  public void deleteSucceedsForLockPolicyOnTable() throws Exception {
    MvcResult mvcResult =
        RequestAndValidateHelper.createTableAndValidateResponse(
            GET_TABLE_RESPONSE_BODY, mvc, storageManager);

    LinkedHashMap<String, LinkedHashMap> currentPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");
    Assertions.assertNull(currentPolicies.get("lockState"));

    mvcResult =
        mvc.perform(
                MockMvcRequestBuilders.post(
                        String.format(
                            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                                + "/databases/%s/tables/%s/lock",
                            GET_TABLE_RESPONSE_BODY.getDatabaseId(),
                            GET_TABLE_RESPONSE_BODY.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        CreateUpdateLockRequestBody.builder()
                            .locked(true)
                            .message("setting lock")
                            .build()
                            .toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isCreated())
            .andReturn();

    mvcResult =
        getTable(GET_TABLE_RESPONSE_BODY.getDatabaseId(), GET_TABLE_RESPONSE_BODY.getTableId());

    LinkedHashMap<String, LinkedHashMap> updatedPolicies =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");
    Assertions.assertEquals(updatedPolicies.get("lockState").get("locked"), true);

    mvc.perform(
            MockMvcRequestBuilders.delete(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/%s/tables/%s/lock",
                        GET_TABLE_RESPONSE_BODY.getDatabaseId(),
                        GET_TABLE_RESPONSE_BODY.getTableId()))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNoContent())
        .andReturn();
    mvcResult =
        getTable(GET_TABLE_RESPONSE_BODY.getDatabaseId(), GET_TABLE_RESPONSE_BODY.getTableId());
    LinkedHashMap<String, LinkedHashMap> policiesAfterLockDelete =
        JsonPath.read(mvcResult.getResponse().getContentAsString(), "$.policies");
    Assertions.assertNull(policiesAfterLockDelete.get("lockState"));
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  private MvcResult getTable(String databaseId, String tableId) throws Exception {
    return mvc.perform(
            MockMvcRequestBuilders.get(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/%s/tables/%s",
                        databaseId,
                        tableId))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andReturn();
  }
}
