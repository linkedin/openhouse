package com.linkedin.openhouse.tables.mock.audit;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.gson.JsonParser;
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.CachingRequestBodyFilter;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.api.handler.ViewsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.audit.model.ViewOperationType;
import com.linkedin.openhouse.tables.controller.ViewsController;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.exception.ViewRequestValidationFailureException;
import com.linkedin.openhouse.tables.exception.ViewValidationErrorCode;
import com.linkedin.openhouse.tables.mock.MockViewsApiHandler;
import com.linkedin.openhouse.tables.mock.logging.Log4j2LogCapture;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import java.time.Instant;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * Handler-level view operation audit: one event per create, replace or drop handler invocation,
 * none for reads or for requests rejected before the handler.
 *
 * <p>Routed requests go through the real controller to the advised {@link MockViewsApiHandler}.
 * Direct calls go to a second, scripted handler bean, advised the same way through the {@link
 * ViewsApiHandler} interface, so the exact response and throwable can be compared by identity.
 */
@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
@Import(ViewsApiHandlerAuditTest.ScriptedHandlerBeans.class)
@WithMockUser(username = ViewsApiHandlerAuditTest.DIRECT_CALLER)
public class ViewsApiHandlerAuditTest {

  static final String DIRECT_CALLER = "direct-caller";

  /** Passed as the handler's principal argument; the audit must use the authenticated user. */
  private static final String METHOD_PRINCIPAL = "method-argument-principal";

  private static final String ROUTED_CALLER = "DUMMY_ANONYMOUS_USER";
  private static final String SECRET =
      "select secret_sql from secret_schema at file:/secret-base.metadata.json";
  private static final String SINK_FAILURE_DIAGNOSTIC = "View operation audit emission failed";
  private static final String VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";

  /** Exactly the reduced operation-event schema: no identity, prior pointer, dialect or cause. */
  private static final Set<String> EVENT_FIELDS =
      Set.of(
          "eventTimestamp",
          "clusterName",
          "databaseName",
          "viewName",
          "user",
          "operationType",
          "operationStatus",
          "newMetadataLocation",
          "failureCode",
          "failureMessage");

  @Autowired private ViewsController viewsController;
  @Autowired private OpenHouseExceptionHandler openHouseExceptionHandler;
  @Autowired private ClusterProperties clusterProperties;
  @Autowired private MeterRegistry meterRegistry;
  @Autowired private Script script;

  @Autowired
  @Qualifier("scriptedViewsApiHandler")
  private ViewsApiHandler advisedHandler;

  @MockBean private AuditHandler<ViewAuditEvent> viewAuditHandler;

  private MockMvc mvc;
  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws Exception {
    mvc =
        MockMvcBuilders.standaloneSetup(viewsController)
            .setControllerAdvice(openHouseExceptionHandler)
            .addInterceptors(new DummyTokenInterceptor())
            .addFilter(new CachingRequestBodyFilter())
            .build();
    jwtAccessToken = new DummyTokenInterceptor.DummySecurityJWT(ROUTED_CALLER).buildNoopJWT();
    script.outcome = null;
    script.handlerEntry = null;
    assertNotEquals(
        ViewModelConstants.CLUSTER_ID,
        clusterProperties.getClusterName(),
        "Precondition: the server cluster differs from the clusterId the fixtures respond with.");
  }

  // --- Success: the original response is returned, and its pointer is the one audited ---

  static Stream<Arguments> successfulWrites() {
    CreateUpdateViewRequestBody request = ViewModelConstants.fullyPopulatedRequest();
    return Stream.of(
        Arguments.of(
            "POST",
            HttpStatus.CREATED,
            ViewOperationType.CREATE,
            (Function<ViewsApiHandler, ApiResponse<?>>)
                handler -> handler.createView(request, METHOD_PRINCIPAL)),
        Arguments.of(
            "PUT creating",
            HttpStatus.CREATED,
            ViewOperationType.CREATE,
            (Function<ViewsApiHandler, ApiResponse<?>>)
                handler -> handler.updateView(request, METHOD_PRINCIPAL)),
        Arguments.of(
            "PUT replacing",
            HttpStatus.OK,
            ViewOperationType.REPLACE,
            (Function<ViewsApiHandler, ApiResponse<?>>)
                handler -> handler.updateView(request, METHOD_PRINCIPAL)),
        Arguments.of(
            "DELETE",
            HttpStatus.NO_CONTENT,
            ViewOperationType.DROP,
            (Function<ViewsApiHandler, ApiResponse<?>>)
                handler ->
                    handler.deleteView(
                        ViewModelConstants.DATABASE_ID,
                        ViewModelConstants.VIEW_ID,
                        METHOD_PRINCIPAL)));
  }

  /**
   * The returned status, not the request's base token, decides CREATE versus REPLACE for a PUT. The
   * audited pointer is the response's metadata location: not its view version, not the request's
   * base pointer, and absent for a drop. Naming comes from the request, not the response, and the
   * user is the authenticated caller, not the handler's principal argument. The timestamp is taken
   * before the handler is entered, and the cluster comes from server configuration, not the
   * response.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("successfulWrites")
  public void successReturnsTheOriginalResponseAndAuditsItsPointer(
      String route,
      HttpStatus status,
      ViewOperationType expectedType,
      Function<ViewsApiHandler, ApiResponse<?>> call) {
    boolean drop = expectedType == ViewOperationType.DROP;
    ApiResponse<?> original =
        ApiResponse.builder()
            .httpStatus(status)
            .responseBody(
                drop
                    ? null
                    : ViewModelConstants.pointerResponseWithDistinctPointers()
                        .toBuilder()
                        .databaseId("response_database")
                        .viewId("response_view")
                        .build())
            .build();
    script.outcome = () -> original;
    assertNotEquals(
        ViewModelConstants.fullyPopulatedRequest().getBaseMetadataLocation(),
        ViewModelConstants.DISTINCT_METADATA_LOCATION,
        "Precondition: the response pointer differs from the request's base pointer.");

    Instant before = Instant.now();
    ApiResponse<?> returned = call.apply(advisedHandler);
    Instant after = Instant.now();

    assertSame(original, returned);
    ViewAuditEvent event = singleEvent();
    assertEquals(EVENT_FIELDS, JsonParser.parseString(event.toJson()).getAsJsonObject().keySet());
    assertEquals(OperationStatus.SUCCESS, event.getOperationStatus());
    assertEquals(expectedType, event.getOperationType());
    assertEquals(ViewModelConstants.DATABASE_ID, event.getDatabaseName());
    assertEquals(ViewModelConstants.VIEW_ID, event.getViewName());
    assertEquals(DIRECT_CALLER, event.getUser());
    assertEquals(clusterProperties.getClusterName(), event.getClusterName());
    assertEquals(
        drop ? null : ViewModelConstants.DISTINCT_METADATA_LOCATION,
        event.getNewMetadataLocation());
    assertNull(event.getFailureCode());
    assertNull(event.getFailureMessage());
    assertNotNull(event.getEventTimestamp());
    assertFalse(event.getEventTimestamp().isBefore(before), "audited no earlier than the call");
    assertFalse(event.getEventTimestamp().isAfter(after), "audited no later than the call");
    assertFalse(
        event.getEventTimestamp().isAfter(script.handlerEntry),
        "audited before the handler is entered");
  }

  // --- Failure: the original throwable propagates, and only a controlled code/message is kept ---

  static Stream<Arguments> handlerFailures() {
    return Stream.of(
        Arguments.of(
            new ViewRequestValidationFailureException(
                ViewValidationErrorCode.UNSUPPORTED_VIEW_SCHEMA, SECRET),
            "UNSUPPORTED_VIEW_SCHEMA",
            "Bad Request"),
        Arguments.of(
            new IllegalStateException(SECRET, new RuntimeException(SECRET)),
            "INTERNAL_VIEW_ERROR",
            "Internal Server Error"),
        Arguments.of(new AssertionError(SECRET), "INTERNAL_VIEW_ERROR", "Internal Server Error"));
  }

  @ParameterizedTest
  @MethodSource("handlerFailures")
  public void failureRethrowsTheOriginalThrowableWithOnlyAControlledCodeAndMessage(
      Throwable failure, String expectedCode, String expectedMessage) {
    script.outcome =
        () -> {
          throw unchecked(failure);
        };

    Throwable thrown =
        assertThrows(
            Throwable.class,
            () ->
                advisedHandler.createView(
                    ViewModelConstants.fullyPopulatedRequest(), DIRECT_CALLER));

    assertSame(failure, thrown);
    ViewAuditEvent event = singleEvent();
    assertEquals(OperationStatus.FAILED, event.getOperationStatus());
    assertEquals(ViewOperationType.CREATE, event.getOperationType());
    assertEquals(expectedCode, event.getFailureCode());
    assertEquals(expectedMessage, event.getFailureMessage());
    assertNull(event.getNewMetadataLocation());
    assertEquals(DIRECT_CALLER, event.getUser());
    assertEquals(clusterProperties.getClusterName(), event.getClusterName());
    String json = event.toJson();
    assertEquals(EVENT_FIELDS, JsonParser.parseString(json).getAsJsonObject().keySet());
    assertFalse(json.contains("secret"), json);
    assertFalse(json.contains(failure.getClass().getName()), json);
    assertFalse(json.contains("java.lang"), json);
  }

  /**
   * A failed PUT has no response to classify, so it reports the requested operation: the initial
   * base version is a create; any other base, including none, is a replace. A missing base must not
   * break the advice or mask the original failure.
   */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {INITIAL_TABLE_VERSION})
  public void failedPutReportsTheRequestedOperationFromItsBaseVersion(String baseMetadataLocation) {
    ViewApiException failure =
        new ViewApiException(ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, SECRET);
    script.outcome =
        () -> {
          throw failure;
        };
    CreateUpdateViewRequestBody request =
        ViewModelConstants.fullyPopulatedRequest()
            .toBuilder()
            .baseMetadataLocation(baseMetadataLocation)
            .build();

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class, () -> advisedHandler.updateView(request, DIRECT_CALLER));

    assertSame(failure, thrown);
    ViewAuditEvent event = singleEvent();
    assertEquals(OperationStatus.FAILED, event.getOperationStatus());
    assertEquals(
        baseMetadataLocation == null ? ViewOperationType.REPLACE : ViewOperationType.CREATE,
        event.getOperationType());
    assertEquals("CONCURRENT_VIEW_MODIFICATION", event.getFailureCode());
    assertEquals("Conflict", event.getFailureMessage());
  }

  // --- Sink delivery is isolated: one attempt, a fixed diagnostic and the failed-audit metric ---

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void sinkFailureIsContainedWithOneAttemptAFixedDiagnosticAndTheFailedAuditMetric(
      boolean handlerFails) {
    assertTrue(
        Metrics.globalRegistry.getRegistries().contains(meterRegistry),
        "Precondition: this context's meter registry backs the global registry.");
    doThrow(new IllegalStateException(SECRET, new RuntimeException(SECRET)))
        .when(viewAuditHandler)
        .audit(any(ViewAuditEvent.class));
    ApiResponse<?> original =
        ApiResponse.builder()
            .httpStatus(HttpStatus.CREATED)
            .responseBody(ViewModelConstants.pointerResponse())
            .build();
    ViewApiException failure =
        new ViewApiException(ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, "handler failure");
    script.outcome =
        () -> {
          if (handlerFails) {
            throw failure;
          }
          return original;
        };
    double failedViewAuditsBefore = failedAudits("view_audit_failed_service_audit");
    double failedServiceAuditsBefore = failedAudits("service_audit_failed_service_audit");

    String logs;
    try (Log4j2LogCapture capture = new Log4j2LogCapture()) {
      if (handlerFails) {
        assertSame(
            failure,
            assertThrows(
                ViewApiException.class,
                () ->
                    advisedHandler.createView(
                        ViewModelConstants.fullyPopulatedRequest(), DIRECT_CALLER)));
      } else {
        assertSame(
            original,
            advisedHandler.createView(ViewModelConstants.fullyPopulatedRequest(), DIRECT_CALLER));
      }
      logs = capture.renderedEvents();
    }

    assertEquals(
        handlerFails ? OperationStatus.FAILED : OperationStatus.SUCCESS,
        singleEvent().getOperationStatus(),
        "Exactly one delivery attempt, never a second FAILED event for the sink's own failure.");
    assertEquals(failedViewAuditsBefore + 1, failedAudits("view_audit_failed_service_audit"));
    assertEquals(failedServiceAuditsBefore, failedAudits("service_audit_failed_service_audit"));
    assertTrue(logs.contains(SINK_FAILURE_DIAGNOSTIC), logs);
    assertFalse(logs.contains("secret"), logs);
    assertFalse(logs.contains(IllegalStateException.class.getName()), logs);
  }

  // --- Routed through the real controller ---

  static Stream<Arguments> routedFailures() {
    return Stream.of(
        Arguments.of(
            "POST",
            MockViewsApiHandler.databaseIdFor(ViewErrorCode.VIEWS_DISABLED),
            ViewOperationType.CREATE,
            "VIEWS_DISABLED",
            "Not Found"),
        Arguments.of(
            "PUT",
            MockViewsApiHandler.ACCESS_DENIED_DATABASE_ID,
            ViewOperationType.REPLACE,
            "ACCESS_DENIED",
            "Forbidden"),
        Arguments.of(
            "PUT",
            MockViewsApiHandler.databaseIdFor(ViewErrorCode.COMMIT_STATE_UNKNOWN),
            ViewOperationType.REPLACE,
            "COMMIT_STATE_UNKNOWN",
            "Service Unavailable"),
        Arguments.of(
            "DELETE",
            MockViewsApiHandler.UNAVAILABLE_DATABASE_ID,
            ViewOperationType.DROP,
            "VIEW_SERVICE_UNAVAILABLE",
            "Service Unavailable"));
  }

  /**
   * Each failure the views advice maps is audited once, named from the request, with the mapped
   * code and the status reason phrase: never the handler's own message.
   */
  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("routedFailures")
  public void routedHandlerFailuresAreAuditedOnceWithTheMappedCodeAndReasonPhrase(
      String method,
      String databaseId,
      ViewOperationType expectedType,
      String expectedCode,
      String expectedMessage)
      throws Exception {
    String body =
        ViewModelConstants.fullyPopulatedRequest()
            .toBuilder()
            .databaseId(databaseId)
            .build()
            .toJson();
    String viewsPath = "/v1/databases/" + databaseId + "/views";
    String viewPath = viewsPath + "/" + ViewModelConstants.VIEW_ID;
    MockHttpServletRequestBuilder request;
    if ("POST".equals(method)) {
      request =
          MockMvcRequestBuilders.post(viewsPath)
              .contentType(MediaType.APPLICATION_JSON)
              .content(body);
    } else if ("PUT".equals(method)) {
      request =
          MockMvcRequestBuilders.put(viewPath)
              .contentType(MediaType.APPLICATION_JSON)
              .content(body);
    } else {
      request = MockMvcRequestBuilders.delete(viewPath);
    }

    mvc.perform(withToken(request));

    ViewAuditEvent event = singleEvent();
    assertEquals(OperationStatus.FAILED, event.getOperationStatus());
    assertEquals(expectedType, event.getOperationType());
    assertEquals(databaseId, event.getDatabaseName());
    assertEquals(ViewModelConstants.VIEW_ID, event.getViewName());
    assertEquals(ROUTED_CALLER, event.getUser());
    assertEquals(clusterProperties.getClusterName(), event.getClusterName());
    assertEquals(expectedCode, event.getFailureCode());
    assertEquals(expectedMessage, event.getFailureMessage());
    assertNull(event.getNewMetadataLocation());
    String json = event.toJson();
    assertFalse(json.contains(MockViewsApiHandler.VIEW_FAILURE_MESSAGE), json);
    assertFalse(json.contains(MockViewsApiHandler.ACCESS_DENIED_MESSAGE), json);
    assertFalse(json.contains(MockViewsApiHandler.UNAVAILABLE_MESSAGE), json);
  }

  /**
   * Reads are not audited, and a write rejected by the controller before the handler runs (an
   * identifier mismatch) is request-audit-only.
   */
  @Test
  public void readsAndRequestsRejectedBeforeTheHandlerHaveNoOperationAudit() throws Exception {
    mvc.perform(
            withToken(MockMvcRequestBuilders.get(VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID)))
        .andExpect(status().isOk());
    mvc.perform(withToken(MockMvcRequestBuilders.get(VIEWS_PATH))).andExpect(status().isOk());
    mvc.perform(
            withToken(
                MockMvcRequestBuilders.post(VIEWS_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        ViewModelConstants.fullyPopulatedRequest()
                            .toBuilder()
                            .databaseId("some_other_database")
                            .build()
                            .toJson())))
        .andExpect(status().isBadRequest());

    verifyNoInteractions(viewAuditHandler);
  }

  private MockHttpServletRequestBuilder withToken(MockHttpServletRequestBuilder request) {
    return request
        .accept(MediaType.APPLICATION_JSON)
        .header("Authorization", "Bearer " + jwtAccessToken);
  }

  private ViewAuditEvent singleEvent() {
    ArgumentCaptor<ViewAuditEvent> event = ArgumentCaptor.forClass(ViewAuditEvent.class);
    verify(viewAuditHandler, times(1)).audit(event.capture());
    return event.getValue();
  }

  private double failedAudits(String metricName) {
    Counter counter = meterRegistry.find(metricName).counter();
    return counter == null ? 0.0 : counter.count();
  }

  private static RuntimeException unchecked(Throwable failure) {
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    return (RuntimeException) failure;
  }

  /** The outcome every write of the scripted handler produces next. */
  static class Script {
    private volatile Supplier<ApiResponse<?>> outcome;
    private volatile Instant handlerEntry;
  }

  /** A second {@link ViewsApiHandler}, advised through the interface like any implementation. */
  static class ScriptedViewsApiHandler implements ViewsApiHandler {
    private final Script script;

    ScriptedViewsApiHandler(Script script) {
      this.script = script;
    }

    @Override
    public ApiResponse<GetViewResponseBody> getView(
        String databaseId, String viewId, String actingPrincipal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiResponse<GetAllViewsResponseBody> getAllViews(
        String databaseId, String pageToken, int size, String sortBy, String actingPrincipal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiResponse<GetViewResponseBody> createView(
        CreateUpdateViewRequestBody requestBody, String actingPrincipal) {
      return next();
    }

    @Override
    public ApiResponse<GetViewResponseBody> updateView(
        CreateUpdateViewRequestBody requestBody, String actingPrincipal) {
      return next();
    }

    @Override
    public ApiResponse<Void> deleteView(String databaseId, String viewId, String actingPrincipal) {
      return next();
    }

    @SuppressWarnings("unchecked")
    private <T> ApiResponse<T> next() {
      script.handlerEntry = Instant.now();
      return (ApiResponse<T>) script.outcome.get();
    }
  }

  /**
   * Imported rather than annotated: the mock application scans this package without the test-type
   * exclude filter, so an annotated nested configuration would leak into other test contexts.
   */
  static class ScriptedHandlerBeans {
    @Bean
    Script script() {
      return new Script();
    }

    @Bean
    ViewsApiHandler scriptedViewsApiHandler(Script script) {
      return new ScriptedViewsApiHandler(script);
    }
  }
}
