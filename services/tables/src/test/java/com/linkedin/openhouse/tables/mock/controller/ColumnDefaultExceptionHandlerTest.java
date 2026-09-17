package com.linkedin.openhouse.tables.mock.controller;

import static com.linkedin.openhouse.tables.mock.RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY;
import static com.linkedin.openhouse.tables.mock.RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseIcebergSnapshotsApiHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseTablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.validator.IcebergSnapshotsApiValidator;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.controller.ColumnDefaultExceptionHandler;
import com.linkedin.openhouse.tables.controller.IcebergSnapshotsController;
import com.linkedin.openhouse.tables.controller.TablesController;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException.Origin;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException.Reason;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.services.IcebergSnapshotsServiceImpl;
import com.linkedin.openhouse.tables.services.TablesServiceImpl;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.reactive.function.client.WebClientResponseException;

class ColumnDefaultExceptionHandlerTest {
  private static final String TABLE_PATH = "/v1/databases/db1/tables/tb1";
  private static final String SECRET = "hdfs://private/metadata.json raw-default-secret";
  private static final TableDto TABLE =
      TableDto.builder().databaseId("db1").tableId("tb1").tableLocation(SECRET).build();

  private final ReadBridgeStripProtection protection = mock(ReadBridgeStripProtection.class);
  private final OpenHouseInternalRepository repository = mock(OpenHouseInternalRepository.class);
  private final TablesServiceImpl tablesService = new TablesServiceImpl();
  private final IcebergSnapshotsServiceImpl snapshotsService = new IcebergSnapshotsServiceImpl();
  private MockMvc mvc;

  @BeforeEach
  void setUp() {
    TablesMapper mapper = mock(TablesMapper.class);
    when(mapper.toTableDto(any(TableDto.class), any(CreateUpdateTableRequestBody.class)))
        .thenReturn(TABLE);
    when(mapper.toTableDto(any(TableDto.class), any(IcebergSnapshotsRequestBody.class)))
        .thenReturn(TABLE);
    when(repository.findById(any(TableDtoPrimaryKey.class))).thenReturn(Optional.empty());
    TableUUIDGenerator uuidGenerator = mock(TableUUIDGenerator.class);
    when(uuidGenerator.generateUUID(any(CreateUpdateTableRequestBody.class)))
        .thenReturn(UUID.randomUUID());
    when(uuidGenerator.generateUUID(any(IcebergSnapshotsRequestBody.class)))
        .thenReturn(UUID.randomUUID());

    for (Object service : new Object[] {tablesService, snapshotsService}) {
      ReflectionTestUtils.setField(service, "openHouseInternalRepository", repository);
      ReflectionTestUtils.setField(service, "tablesMapper", mapper);
      ReflectionTestUtils.setField(service, "tableUUIDGenerator", uuidGenerator);
      ReflectionTestUtils.setField(service, "authorizationUtils", mock(AuthorizationUtils.class));
      ReflectionTestUtils.setField(service, "readBridgeStripProtection", protection);
    }
    OpenHouseTablesApiHandler tablesApi = new OpenHouseTablesApiHandler();
    ReflectionTestUtils.setField(tablesApi, "tableService", tablesService);
    ReflectionTestUtils.setField(tablesApi, "tablesApiValidator", mock(TablesApiValidator.class));
    ReflectionTestUtils.setField(tablesApi, "clusterProperties", mock(ClusterProperties.class));
    OpenHouseIcebergSnapshotsApiHandler snapshotsApi = new OpenHouseIcebergSnapshotsApiHandler();
    ReflectionTestUtils.setField(snapshotsApi, "icebergSnapshotsService", snapshotsService);
    ReflectionTestUtils.setField(
        snapshotsApi, "icebergSnapshotsApiValidator", mock(IcebergSnapshotsApiValidator.class));
    ReflectionTestUtils.setField(snapshotsApi, "clusterProperties", mock(ClusterProperties.class));
    TablesController tablesController = new TablesController();
    ReflectionTestUtils.setField(tablesController, "tablesApiHandler", tablesApi);
    IcebergSnapshotsController snapshotsController = new IcebergSnapshotsController();
    ReflectionTestUtils.setField(snapshotsController, "icebergSnapshotsApiHandler", snapshotsApi);
    mvc =
        MockMvcBuilders.standaloneSetup(tablesController, snapshotsController)
            // Register the catch-all first: annotation priority, not registration order, must win.
            .setControllerAdvice(
                new OpenHouseExceptionHandler(), new ColumnDefaultExceptionHandler())
            .build();
  }

  @ParameterizedTest
  @CsvSource({
    "INVALID_SCHEMA, INCOMING, 400",
    "INVALID_VALUE, INCOMING, 400",
    "TYPE_MISMATCH, INCOMING, 400",
    "OUT_OF_RANGE, INCOMING, 400",
    "INVALID_SCHEMA, STORED, 500",
    "INVALID_VALUE, STORED, 500",
    "TYPE_MISMATCH, STORED, 500",
    "OUT_OF_RANGE, STORED, 500",
    "INVALID_SCHEMA, UNKNOWN, 500",
    "INVALID_VALUE, UNKNOWN, 500",
    "TYPE_MISMATCH, UNKNOWN, 500",
    "OUT_OF_RANGE, UNKNOWN, 500"
  })
  void validationOriginDeterminesWhetherTheCallerCanRepairTheWrite(
      Reason reason, Origin origin, int expectedStatus) throws Exception {
    failWith(failure(reason, origin, new IllegalArgumentException(SECRET)));
    ResultActions response = typedResponse(create(), reason, expectedStatus, false);
    response
        .andExpect(jsonPath("$.message", containsString("db1.tb1")))
        .andExpect(jsonPath("$.message", containsString("country")))
        .andExpect(jsonPath("$.message", containsString("string")))
        .andExpect(jsonPath("$.message", containsString("int")));
  }

  @ParameterizedTest
  @CsvSource({"REMOVED", "REWRITE"})
  void updateGuardReasonsReachTheTypedHandler(Reason reason) throws Exception {
    failWith(failure(reason, Origin.INCOMING, new IllegalArgumentException(SECRET)));
    typedResponse(update(), reason, 400, false);
  }

  @Test
  void snapshotLookupFailureIsTransientAndHandledBeforeTheCatchAll() throws Exception {
    TableFeatureToggle toggle = mock(TableFeatureToggle.class);
    when(toggle.isFeatureActivatedWithOverride(any(TableDto.class), any()))
        .thenThrow(
            WebClientResponseException.create(
                503,
                "Unavailable",
                HttpHeaders.EMPTY,
                SECRET.getBytes(StandardCharsets.UTF_8),
                StandardCharsets.UTF_8));
    ReadBridgeConfigResolver resolver =
        new ReadBridgeConfigResolver(table -> Collections.emptyMap(), toggle);
    ReflectionTestUtils.setField(
        snapshotsService, "readBridgeStripProtection", new ReadBridgeStripProtection(resolver));
    typedResponse(snapshots(), Reason.UNAVAILABLE, 503, true);
  }

  @Test
  void internalBugIsServerFailureAndItsPrivateCauseIsCorrelatedOnlyInLogs() throws Exception {
    NullPointerException bug = new NullPointerException(SECRET);
    TableFeatureToggle toggle = mock(TableFeatureToggle.class);
    when(toggle.isFeatureActivatedWithOverride(any(TableDto.class), any())).thenReturn(true);
    ReadBridgeConfigResolver resolver =
        new ReadBridgeConfigResolver(
            table -> {
              throw bug;
            },
            toggle);
    ReflectionTestUtils.setField(
        tablesService, "readBridgeStripProtection", new ReadBridgeStripProtection(resolver));
    List<LogEvent> events = new ArrayList<>();
    AbstractAppender appender =
        new AbstractAppender("column-default-test", null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    Logger logger = (Logger) LogManager.getLogger(ColumnDefaultExceptionHandler.class);
    appender.start();
    logger.addAppender(appender);
    try {
      String json =
          typedResponse(update(), Reason.INTERNAL, 500, false)
              .andReturn()
              .getResponse()
              .getContentAsString();
      JsonNode body = new ObjectMapper().readTree(json);
      String requestId = body.get("requestId").asText();
      assertNotEquals("client-supplied-id", requestId);
      assertTrue(body.get("message").asText().contains(requestId));
      LogEvent logged =
          events.stream()
              .filter(
                  event ->
                      event.getMessage().getFormattedMessage().contains("requestId=" + requestId))
              .findFirst()
              .orElseThrow(() -> new AssertionError("Missing correlated server diagnostic"));
      assertTrue(logged.getThrown() instanceof ColumnDefaultException);
      assertSame(bug, ExceptionUtils.getRootCause(logged.getThrown()));
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  @Test
  void unsafeContextCannotExposeSchemaOrMetadataLocations() throws Exception {
    failWith(
        new ColumnDefaultException(
                Reason.TYPE_MISMATCH,
                TABLE.toBuilder().databaseId(SECRET).tableId(SECRET).build(),
                2,
                SECRET,
                "{\"default\":\"raw-default-secret\"}",
                SECRET,
                new IllegalArgumentException(SECRET))
            .withOrigin(Origin.INCOMING));
    typedResponse(create(), Reason.TYPE_MISMATCH, 400, false)
        .andExpect(jsonPath("$.message", containsString("field ID 2")));
  }

  @Test
  void unrelatedValidationStillUsesTheCommonHandler() throws Exception {
    doThrow(new RequestValidationFailureException("ordinary validation"))
        .when(protection)
        .prepare(isNull(), any(TableDto.class));
    mvc.perform(create())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value("ordinary validation"))
        .andExpect(jsonPath("$.code").doesNotHaveJsonPath())
        .andExpect(jsonPath("$.requestId").doesNotHaveJsonPath())
        .andExpect(jsonPath("$.retryable").doesNotHaveJsonPath())
        .andExpect(jsonPath("$.stacktrace").exists())
        .andExpect(jsonPath("$.cause").exists());
  }

  private static ColumnDefaultException failure(Reason reason, Origin origin, Throwable cause) {
    return new ColumnDefaultException(reason, TABLE, 2, "country", "string", "int", cause)
        .withOrigin(origin);
  }

  private void failWith(ColumnDefaultException failure) throws ColumnDefaultException {
    doThrow(failure).when(protection).prepare(isNull(), any(TableDto.class));
  }

  private ResultActions typedResponse(
      MockHttpServletRequestBuilder request, Reason reason, int expectedStatus, boolean retryable)
      throws Exception {
    ResultActions response =
        mvc.perform(request)
            .andExpect(status().is(expectedStatus))
            .andExpect(jsonPath("$.code").value("COLUMN_DEFAULT_" + reason.name()))
            .andExpect(jsonPath("$.retryable").value(retryable))
            .andExpect(jsonPath("$.requestId").isNotEmpty())
            .andExpect(jsonPath("$.message", not(containsString("raw-default-secret"))))
            .andExpect(jsonPath("$.message", not(containsString("hdfs://"))))
            .andExpect(jsonPath("$.stacktrace").doesNotHaveJsonPath())
            .andExpect(jsonPath("$.cause").doesNotHaveJsonPath());
    String json = response.andReturn().getResponse().getContentAsString();
    assertFalse(json.contains("raw-default-secret"));
    assertFalse(json.contains("hdfs://"));
    verify(repository, never()).save(any(TableDto.class));
    return response;
  }

  private static MockHttpServletRequestBuilder create() {
    return post("/v1/databases/db1/tables")
        .accept(MediaType.APPLICATION_JSON)
        .contentType(MediaType.APPLICATION_JSON)
        .content(TEST_CREATE_TABLE_REQUEST_BODY.toJson());
  }

  private static MockHttpServletRequestBuilder update() {
    return put(TABLE_PATH)
        .header("X-Request-Id", "client-supplied-id")
        .accept(MediaType.APPLICATION_JSON)
        .contentType(MediaType.APPLICATION_JSON)
        .content(TEST_CREATE_TABLE_REQUEST_BODY.toJson());
  }

  private static MockHttpServletRequestBuilder snapshots() {
    return put(TABLE_PATH + "/iceberg/v2/snapshots")
        .accept(MediaType.APPLICATION_JSON)
        .contentType(MediaType.APPLICATION_JSON)
        .content(TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY.toJson());
  }
}
