package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.ServiceAuditPayloadRedactor;
import com.linkedin.openhouse.common.audit.ServiceAuditUriRedactor;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.ViewCommitEngine;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.authorization.OpaHandler;
import com.linkedin.openhouse.tables.controller.ViewsController;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewExceptionHandler;
import com.linkedin.openhouse.tables.mock.audit.AuditEventInspection;
import com.linkedin.openhouse.tables.mock.logging.Log4j2LogCapture;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.services.DatabasesService;
import com.linkedin.openhouse.tables.services.ViewPageCursor;
import com.linkedin.openhouse.tables.services.ViewPageTokenCodec;
import com.linkedin.openhouse.tables.services.ViewPaginationAdapter;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.util.NestedServletException;

@SpringBootTest(classes = {SpringH2Application.class, ViewsManagedFailureAuditTest.Config.class})
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@TestPropertySource(
    properties = {
      "cluster.security.token.interceptor.classname=com.linkedin.openhouse.common.security.DummyTokenInterceptor",
      "cluster.security.tables.authorization.enabled=true",
      "cluster.security.tables.authorization.opa.base-uri=http://opa.test"
    })
public class ViewsManagedFailureAuditTest {

  private static final String PRINCIPAL = "failure-user";
  private static final String VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";
  private static final String VIEW_PATH = VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID;
  private static final String DATABASE_ROUTE = "/v1/databases/" + ViewModelConstants.DATABASE_ID;
  private static final String SECRET_SQL = "select very_secret_sql";
  private static final String SECRET_SCHEMA = "very_secret_schema";
  private static final String SECRET_BASE = "file:/very-secret-base.metadata.json";
  private static final String SECRET_PAGE_TOKEN = "very-secret-page-token";
  /**
   * Forbidden on every diagnostic surface (responses, rendered logs including thrown causes) and in
   * the identity-only operation audit event.
   */
  private static final String[] SECRETS = {
    SECRET_SQL, SECRET_SCHEMA, SECRET_BASE, SECRET_PAGE_TOKEN
  };

  /**
   * Forbidden in the request audit event. The request audit keeps the supplied view definition
   * (schema and SQL) as request provenance, so only the CAS and page tokens are listed here.
   */
  private static final String[] REQUEST_TOKEN_SECRETS = {SECRET_BASE, SECRET_PAGE_TOKEN};

  private static final String REQUEST_SINK_FAILURE = "request-audit-sink-failure-marker";
  private static final String[] PARSER_DETAILS = {
    "com.google.gson", "MalformedJsonException", "JsonSyntaxException", "End of input"
  };
  private static final String CAPTURED_UUID = "view-uuid";
  private static final String SESSION_ID_HEADER = "session-id";
  private static final String SINK_FAILURE = "operation-audit-sink-failure-marker";
  private static final String SIZE_BINDING_MESSAGE = "size : must be an integer";
  private static final String[] BINDING_PARSER_DETAILS = {
    "NumberFormatException", "For input string", "Failed to convert", "java.lang"
  };

  @Autowired private MockMvc mvc;
  @Autowired private MeterRegistry meterRegistry;

  @MockBean private ViewsFeatureGate viewsFeatureGate;
  @MockBean private DatabasesService databasesService;
  // DatabasesService is mocked, so no real House Table path is needed: capture, read, list and
  // delete failures are injected at a mock of the primary H2 House Table bean the views service
  // injects, commit failures at the engine mock.
  @MockBean(name = "houseTablesH2Repository")
  private HouseTablesH2Repository houseTableRepository;

  @MockBean private ViewCommitEngine viewCommitEngine;
  @MockBean private OpaHandler opaHandler;
  // SpringH2Application defines bean "serviceAuditHandler" and the scanned DummyServiceAuditHandler
  // is a second candidate; ServiceAuditAspect injects by that field name, so replace that bean.
  @MockBean(name = "serviceAuditHandler")
  private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  @MockBean private AuditHandler<ViewAuditEvent> viewAuditHandler;

  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws Exception {
    Mockito.reset(
        viewsFeatureGate,
        databasesService,
        houseTableRepository,
        viewCommitEngine,
        opaHandler,
        serviceAuditHandler,
        viewAuditHandler);
    jwtAccessToken = new DummyTokenInterceptor.DummySecurityJWT(PRINCIPAL).buildNoopJWT();
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases())
        .thenReturn(
            Collections.singletonList(
                DatabaseDto.builder().databaseId(ViewModelConstants.DATABASE_ID).build()));
    when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(Optional.empty());
  }

  /**
   * A body that Gson (the request-audit parser) also rejects, not merely Jackson, so the audit's
   * parse-failure path runs: the payload is dropped rather than retained raw, the existing parsing
   * failure metric is counted, and neither the body nor parser details reach the logs.
   */
  @Test
  public void malformedJsonHasRequestAuditNoOperationAuditAndNoSensitiveWireOrLogs()
      throws Exception {
    String malformed =
        "{\"viewId\":\""
            + ViewModelConstants.VIEW_ID
            + "\",\"schema\":\""
            + SECRET_SCHEMA
            + "\",\"representations\":[{\"sql\":\""
            + SECRET_SQL
            + "\"}],";
    double parseFailuresBefore = serviceAuditCount(MetricsConstant.FAILED_PARSING_REQUEST_PAYLOAD);

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.post(VIEWS_PATH)
                          .contentType(MediaType.APPLICATION_JSON)
                          .content(malformed)
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)))
              .andExpect(status().isBadRequest())
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
      for (String parserDetail : PARSER_DETAILS) {
        assertFalse(logs.renderedEvents().contains(parserDetail), logs.renderedEvents());
      }
    }
    ServiceAuditEvent event = captureServiceAudit();
    assertNull(event.getRequestPayload(), "An unparseable body is dropped, never audited raw.");
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    assertEquals(
        parseFailuresBefore + 1,
        serviceAuditCount(MetricsConstant.FAILED_PARSING_REQUEST_PAYLOAD),
        "The audit's parse-failure path ran and was counted.");
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  @Test
  public void successfulCreateEmitsOneRequestAndOneSuccessOperationAuditWithCommittedPointer()
      throws Exception {
    ViewCommitResult committed = ViewsManagedAuthMatrixBase.createdResult();
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(committed);
    String sessionId = "view-session-success";

    mvc.perform(
            MockMvcRequestBuilders.post(VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestWithSensitiveMarkers())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken)
                .header(SESSION_ID_HEADER, sessionId))
        .andExpect(status().isCreated());

    assertRequestAuditKeepsSuppliedDefinition(captureServiceAudit(), requestWithSensitiveMarkers());
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(OperationStatus.SUCCESS, viewEvent.getOperationStatus());
    assertEquals(
        ViewsManagedAuthMatrixBase.CREATED_VIEW_UUID,
        viewEvent.getViewUUID(),
        "Create audit UUID is the engine-acknowledged UUID.");
    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    assertNotEquals(
        intent.getValue().getViewUuid(),
        viewEvent.getViewUUID(),
        "The tentative allocation UUID never stands in for the acknowledged one.");
    assertNull(viewEvent.getOldMetadataLocation(), "A create has no prior pointer.");
    assertEquals(committed.getPointer().getMetadataLocation(), viewEvent.getNewMetadataLocation());
    assertEquals(ViewModelConstants.DATABASE_ID, viewEvent.getDatabaseName());
    assertEquals(ViewModelConstants.VIEW_ID, viewEvent.getViewName());
    assertEquals(PRINCIPAL, viewEvent.getUser());
    assertEquals(ViewModelConstants.SOURCE_DIALECT, viewEvent.getSourceDialect());
    AuditEventInspection.assertNoSensitiveProperties(viewEvent, SECRETS);
    assertCorrelated(sessionId, OperationStatus.SUCCESS);
  }

  @Test
  public void successfulReplaceAuditsCapturedIdentityAndOldPointerWithCommittedNewPointer()
      throws Exception {
    ViewCommitResult committed = ViewsManagedAuthMatrixBase.replacedResult();
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(Optional.of(ViewsManagedAuthMatrixBase.viewRow()));
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(committed);

    mvc.perform(
            MockMvcRequestBuilders.put(VIEW_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(ViewModelConstants.fullyPopulatedRequest().toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk());

    assertRequestAuditKeepsSuppliedDefinition(
        captureServiceAudit(), ViewModelConstants.fullyPopulatedRequest().toJson());
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(OperationStatus.SUCCESS, viewEvent.getOperationStatus());
    assertEquals(CAPTURED_UUID, viewEvent.getViewUUID());
    assertEquals(ViewModelConstants.METADATA_LOCATION, viewEvent.getOldMetadataLocation());
    assertEquals(committed.getPointer().getMetadataLocation(), viewEvent.getNewMetadataLocation());
    assertEquals(PRINCIPAL, viewEvent.getUser());
    AuditEventInspection.assertNoSensitiveProperties(viewEvent, SECRETS);
  }

  @Test
  public void successfulDropAuditsCapturedIdentityAndOldPointerWithoutNewPointer()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    stubDeletableView();

    mvc.perform(
            MockMvcRequestBuilders.delete(VIEW_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNoContent());

    verify(houseTableRepository).deleteViewById(ViewsManagedAuthMatrixBase.viewKey());
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(OperationStatus.SUCCESS, viewEvent.getOperationStatus());
    assertEquals(CAPTURED_UUID, viewEvent.getViewUUID());
    assertEquals(ViewModelConstants.METADATA_LOCATION, viewEvent.getOldMetadataLocation());
    assertNull(viewEvent.getNewMetadataLocation(), "A drop publishes no new pointer.");
    assertEquals(PRINCIPAL, viewEvent.getUser());
    AuditEventInspection.assertNoSensitiveProperties(viewEvent, SECRETS);
  }

  @Test
  public void accessDeniedMapsToForbiddenThroughViewAdviceWithOneFailedOperationAudit()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(false);

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.post(VIEWS_PATH)
                          .contentType(MediaType.APPLICATION_JSON)
                          .content(requestWithSensitiveMarkers())
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)))
              .andExpect(status().isForbidden())
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    verify(viewCommitEngine, never()).commit(any());
    assertRequestAuditKeepsSuppliedDefinition(captureServiceAudit(), requestWithSensitiveMarkers());
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(OperationStatus.FAILED, viewEvent.getOperationStatus());
    assertNull(viewEvent.getNewMetadataLocation());
    AuditEventInspection.assertNoSensitiveProperties(viewEvent, SECRETS);
  }

  @Test
  public void structuralValidationMessageIsPreservedExactlyByViewAdvice() throws Exception {
    String expected = "page : is not supported; use pageToken for continuation";

    expectNoCauseOrStacktrace(
            mvc.perform(
                MockMvcRequestBuilders.get(VIEWS_PATH + "?page=1")
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value(expected));

    ServiceAuditEvent event = captureServiceAudit();
    assertEquals(expected, event.getResponseErrorMessage());
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  /**
   * A nonnumeric and an int-overflowing LIST {@code size} both fail Spring binding with
   * MethodArgumentTypeMismatchException before the handler runs. They must keep the existing 400
   * with the fixed safe message, never the residual 500. The raw size may legitimately remain in
   * the audited request URI, so only rendered error text, throwable surfaces and logs are checked.
   */
  @Test
  public void malformedAndOverflowingSizeKeepFixedSafe400BeforeService() throws Exception {
    String malformedSize = "abcNotAnIntegerSize";
    String overflowingSize = "99999999999";

    assertPreServiceSizeBindingFailure(malformedSize);
    Mockito.reset(serviceAuditHandler);
    assertPreServiceSizeBindingFailure(overflowingSize);
  }

  private void assertPreServiceSizeBindingFailure(String rawSize) throws Exception {
    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.get(VIEWS_PATH)
                          .param("size", rawSize)
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)))
              .andExpect(status().isBadRequest())
              .andExpect(jsonPath("$.message").value(SIZE_BINDING_MESSAGE))
              .andReturn();

      String wire = result.getResponse().getContentAsString();
      String renderedLogs = logs.renderedEvents();
      assertFalse(wire.contains(rawSize), wire);
      assertFalse(renderedLogs.contains(rawSize), renderedLogs);
      for (String parserDetail : BINDING_PARSER_DETAILS) {
        assertFalse(wire.contains(parserDetail), wire);
        assertFalse(renderedLogs.contains(parserDetail), renderedLogs);
      }
    }

    ServiceAuditEvent event = captureServiceAudit();
    assertEquals(SIZE_BINDING_MESSAGE, event.getResponseErrorMessage());
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    Mockito.verifyNoInteractions(
        viewsFeatureGate, databasesService, houseTableRepository, viewCommitEngine, opaHandler);
  }

  @Test
  public void unexpectedRuntimeFailureMapsToSanitized500() throws Exception {
    when(houseTableRepository.findViewById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenThrow(new RuntimeException(SECRET_SQL));

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.get(VIEW_PATH)
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)))
              .andExpect(status().isInternalServerError())
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  /**
   * Spring MVC wraps a handler {@code Error} in {@link NestedServletException} before exception
   * resolution, so the managed request cannot observe the raw Error. The invariant is proven here
   * against Spring's own resolver: the view advice resolves neither the Error nor its wrapper, and
   * it only declares handlers for {@link Exception} types. Service-level propagation of the same
   * Error instance is covered by {@code ViewsServiceImplTest}.
   */
  @Test
  public void viewAdviceNeverResolvesFatalErrorsDirectlyOrWrapped() {
    ExceptionHandlerMethodResolver resolver =
        new ExceptionHandlerMethodResolver(ViewExceptionHandler.class);
    Error fatal = new AssertionError("fatal");

    assertNull(resolver.resolveMethodByThrowable(fatal));
    assertNull(
        resolver.resolveMethodByThrowable(
            new NestedServletException("Handler dispatch failed", fatal)));

    Set<Class<?>> handled = new HashSet<>();
    for (Method method : ViewExceptionHandler.class.getDeclaredMethods()) {
      ExceptionHandler annotation = method.getAnnotation(ExceptionHandler.class);
      if (annotation == null) {
        continue;
      }
      if (annotation.value().length > 0) {
        handled.addAll(Arrays.asList(annotation.value()));
      } else {
        for (Class<?> parameter : method.getParameterTypes()) {
          if (Throwable.class.isAssignableFrom(parameter)) {
            handled.add(parameter);
          }
        }
      }
    }
    for (Class<?> type : handled) {
      assertTrue(
          Exception.class.isAssignableFrom(type),
          "View advice must not handle non-Exception throwable type " + type.getName());
    }
    assertTrue(
        handled.containsAll(
            Arrays.asList(
                ViewApiException.class,
                AccessDeniedException.class,
                AuthorizationServiceException.class,
                HttpMessageNotReadableException.class,
                MethodArgumentTypeMismatchException.class,
                RuntimeException.class)),
        "View advice must declare the planned explicit handlers: " + handled);
  }

  @Test
  public void opaUnavailableWriteSanitizesWireLogsRequestAuditAndHasOneFailedOperationAudit()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenThrow(new RuntimeException(SECRET_SQL + " " + SECRET_SCHEMA + " " + SECRET_BASE));

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.post(VIEWS_PATH)
                          .contentType(MediaType.APPLICATION_JSON)
                          .content(requestWithSensitiveMarkers())
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)))
              .andExpect(status().isServiceUnavailable())
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    verify(viewCommitEngine, never()).commit(any());
    assertRequestAuditKeepsSuppliedDefinition(captureServiceAudit(), requestWithSensitiveMarkers());
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(OperationStatus.FAILED, viewEvent.getOperationStatus());
    assertNull(viewEvent.getNewMetadataLocation());
    AuditEventInspection.assertNoSensitiveProperties(viewEvent, SECRETS);
  }

  @Test
  public void unknownCommitHasOneUnknownOperationAuditWithOnlyReachedContext() throws Exception {
    RuntimeException raw =
        new RuntimeException(SECRET_SQL + " " + SECRET_SCHEMA + " " + SECRET_BASE);
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenThrow(new CommitStateUnknownException(raw));
    String sessionId = "view-session-unknown";

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.post(VIEWS_PATH)
                          .contentType(MediaType.APPLICATION_JSON)
                          .content(requestWithSensitiveMarkers())
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)
                          .header(SESSION_ID_HEADER, sessionId)))
              .andExpect(status().isServiceUnavailable())
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }

    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    assertEquals(Boolean.TRUE, intent.getValue().getIsCreate());
    verify(houseTableRepository, never()).deleteViewById(any());
    assertRequestAuditKeepsSuppliedDefinition(captureServiceAudit(), requestWithSensitiveMarkers());
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(OperationStatus.UNKNOWN, viewEvent.getOperationStatus());
    // Observed absence supplies no identity or prior pointer, and an unacknowledged commit
    // returns no result; nothing may be fabricated for these fields.
    assertNull(viewEvent.getViewUUID());
    assertNull(viewEvent.getOldMetadataLocation());
    assertNull(viewEvent.getNewMetadataLocation());
    assertEquals(ViewModelConstants.DATABASE_ID, viewEvent.getDatabaseName());
    assertEquals(ViewModelConstants.VIEW_ID, viewEvent.getViewName());
    assertEquals(PRINCIPAL, viewEvent.getUser());
    AuditEventInspection.assertNoSensitiveProperties(viewEvent, SECRETS);
    assertCorrelated(sessionId, OperationStatus.UNKNOWN);
  }

  @Test
  public void queryPageTokenFailureRedactsTokenFromRequestAuditWireAndLogs() throws Exception {
    String token =
        new ViewPageTokenCodec()
            .encode(
                new ViewPageCursor(
                    ViewModelConstants.DATABASE_ID,
                    "viewId",
                    ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE,
                    0,
                    0));
    when(houseTableRepository.findAllViewsByDatabaseId(any(), any()))
        .thenThrow(new RuntimeException(SECRET_PAGE_TOKEN));

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.get(
                              VIEWS_PATH + "?pageToken=" + token + "&sortBy=viewId&size=1")
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)))
              .andExpect(status().isInternalServerError())
              .andReturn();

      String wire = result.getResponse().getContentAsString();
      String renderedLogs = logs.renderedEvents();
      assertNoSensitive(wire, renderedLogs);
      assertFalse(wire.contains(token), wire);
      assertFalse(renderedLogs.contains(token), renderedLogs);
    }
    ServiceAuditEvent event = captureServiceAudit();
    AuditEventInspection.assertNoSensitiveProperties(
        event, SECRET_SQL, SECRET_SCHEMA, SECRET_BASE, SECRET_PAGE_TOKEN, token);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  @Test
  public void malformedPageTokenRedactsRawQueryStringAndDoesNotReachBackend() throws Exception {
    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.get(
                              VIEWS_PATH + "?pageToken=" + SECRET_PAGE_TOKEN + "&sortBy=viewId")
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)))
              .andExpect(status().isBadRequest())
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    verify(houseTableRepository, never()).findAllViewsByDatabaseId(any(), any());
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  /**
   * Spring MVC routes a trailing-slash, a percent-encoded view segment, and both combined to the
   * same view list handler as the canonical path. The request audit records the raw request URI, so
   * on every accepted spelling the continuation must be replaced while the raw route and the other
   * fields are kept. Requests use {@code URI.create} so the {@code %76} is sent once-encoded.
   */
  @ParameterizedTest
  @ValueSource(strings = {"/views", "/views/", "/%76iews", "/%76iews/"})
  public void validContinuationOnEveryListAliasIsRedactedFromRequestAudit(String routeSuffix)
      throws Exception {
    String rawRoute = DATABASE_ROUTE + routeSuffix;
    String token = validContinuation(3);
    when(houseTableRepository.findAllViewsByDatabaseId(any(), any()))
        .thenAnswer(
            invocation ->
                new PageImpl<HouseTable>(
                    Collections.emptyList(), invocation.<Pageable>getArgument(1), 0));

    MvcResult result =
        assertListAliasAudit(
            rawRoute,
            "?pageToken=" + token + "&sortBy=viewId&size=1",
            "?pageToken=" + ServiceAuditUriRedactor.REDACTED_VALUE + "&sortBy=viewId&size=1",
            200,
            token);

    assertTrue(result.getResponse().getContentAsString().contains("\"results\""));
    ArgumentCaptor<Pageable> pageable = ArgumentCaptor.forClass(Pageable.class);
    verify(houseTableRepository).findAllViewsByDatabaseId(any(), pageable.capture());
    assertEquals(3, pageable.getValue().getPageNumber(), "The continuation must bind and apply.");
  }

  @ParameterizedTest
  @ValueSource(strings = {"/views", "/views/", "/%76iews", "/%76iews/"})
  public void malformedContinuationOnEveryListAliasIsRedactedFromFailureRequestAudit(
      String routeSuffix) throws Exception {
    assertListAliasAudit(
        DATABASE_ROUTE + routeSuffix,
        "?pageToken=" + SECRET_PAGE_TOKEN + "&sortBy=viewId&size=1",
        "?pageToken=" + ServiceAuditUriRedactor.REDACTED_VALUE + "&sortBy=viewId&size=1",
        400,
        SECRET_PAGE_TOKEN);

    verify(houseTableRepository, never()).findAllViewsByDatabaseId(any(), any());
  }

  @Test
  public void backendFailureOnAnEncodedListAliasIsRedactedFromFailureRequestAudit()
      throws Exception {
    String token = validContinuation(0);
    when(houseTableRepository.findAllViewsByDatabaseId(any(), any()))
        .thenThrow(new RuntimeException(SECRET_SQL + " " + token));

    assertListAliasAudit(
        DATABASE_ROUTE + "/%76iews",
        "?pageToken=" + token + "&sortBy=viewId&size=1",
        "?pageToken=" + ServiceAuditUriRedactor.REDACTED_VALUE + "&sortBy=viewId&size=1",
        500,
        token);
  }

  /** The existing duplicate literal/encoded name policy still applies on an alias route. */
  @Test
  public void mixedDuplicatePageTokensOnAnEncodedTrailingAliasAreAllRedacted() throws Exception {
    String literal = SECRET_PAGE_TOKEN + "-literal";
    String encoded = SECRET_PAGE_TOKEN + "-encoded";

    assertListAliasAudit(
        DATABASE_ROUTE + "/%76iews/",
        "?pageToken=" + literal + "&sortBy=viewId&%70ageToken=" + encoded,
        "?pageToken="
            + ServiceAuditUriRedactor.REDACTED_VALUE
            + "&sortBy=viewId&%70ageToken="
            + ServiceAuditUriRedactor.REDACTED_VALUE,
        400,
        literal,
        encoded);

    verify(houseTableRepository, never()).findAllViewsByDatabaseId(any(), any());
  }

  /**
   * Performs a list request on a raw route, proves real dispatch to the view list handler, and
   * asserts one request audit with the expected redacted URI, no operation audit, and no secret in
   * the event, body, or logs.
   */
  private MvcResult assertListAliasAudit(
      String rawRoute,
      String rawQuery,
      String expectedAuditedQuery,
      int expectedStatus,
      String... secrets)
      throws Exception {
    MvcResult result;
    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      withToken(
                          MockMvcRequestBuilders.get(URI.create(rawRoute + rawQuery))
                              .accept(MediaType.APPLICATION_JSON))))
              .andReturn();

      assertEquals(expectedStatus, result.getResponse().getStatus(), rawRoute);
      assertEquals(rawRoute, result.getRequest().getRequestURI(), "The raw path is preserved.");
      assertViewListHandler(result);
      String wire = result.getResponse().getContentAsString();
      String renderedLogs = logs.renderedEvents();
      assertNoSensitive(wire, renderedLogs);
      for (String secret : secrets) {
        assertFalse(wire.contains(secret), wire);
        assertFalse(renderedLogs.contains(secret), renderedLogs);
      }
    }
    ServiceAuditEvent event = captureServiceAudit();
    AuditEventInspection.assertNoSensitiveProperties(event, secrets);
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    assertEquals(rawRoute + expectedAuditedQuery, event.getUri());
    assertEquals(expectedStatus, event.getStatusCode());
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    return result;
  }

  private static void assertViewListHandler(MvcResult result) {
    assertTrue(result.getHandler() instanceof HandlerMethod, String.valueOf(result.getHandler()));
    HandlerMethod handler = (HandlerMethod) result.getHandler();
    assertEquals(ViewsController.class, handler.getBeanType());
    assertEquals("getAllViews", handler.getMethod().getName());
  }

  private static String validContinuation(int sourcePageIndex) {
    return new ViewPageTokenCodec()
        .encode(
            new ViewPageCursor(
                ViewModelConstants.DATABASE_ID,
                "viewId",
                ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE,
                sourcePageIndex,
                0));
  }

  /**
   * A create at an MVC-accepted encoded collection alias succeeds normally, and its request audit
   * keeps the supplied definition exactly as on the canonical route.
   */
  @ParameterizedTest
  @ValueSource(strings = {"/%76iews", "/%76iews/"})
  public void createOnAnEncodedCollectionAliasKeepsTheDefinitionInRequestAudit(String routeSuffix)
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.createdResult());

    assertWriteAliasRequestAudit(
        MockMvcRequestBuilders.post(URI.create(DATABASE_ROUTE + routeSuffix)),
        requestWithSensitiveMarkers(),
        201);

    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
    assertEquals(OperationStatus.SUCCESS, captureViewAudit().getOperationStatus());
  }

  /**
   * A replace at an MVC-accepted encoded item alias succeeds normally, and its request audit keeps
   * the supplied definition while the CAS base metadata location is redacted.
   */
  @Test
  public void replaceOnAnEncodedItemAliasKeepsTheDefinitionAndRedactsTheCasToken()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(
            Optional.of(
                ViewsManagedAuthMatrixBase.viewRow()
                    .toBuilder()
                    .tableLocation(SECRET_BASE)
                    .build()));
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.replacedResult());

    assertWriteAliasRequestAudit(
        MockMvcRequestBuilders.put(
            URI.create(DATABASE_ROUTE + "/%76iews/" + ViewModelConstants.VIEW_ID)),
        sensitiveRequest(SECRET_BASE),
        200);

    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
    assertEquals(OperationStatus.SUCCESS, captureViewAudit().getOperationStatus());
  }

  /**
   * A structurally invalid definition at an encoded alias is rejected before service entry; the
   * exception-advice request audit still keeps the supplied definition with the CAS token redacted,
   * and no operation audit is emitted.
   */
  @Test
  public void invalidDefinitionOnAnEncodedCollectionAliasKeepsTheDefinitionInFailureRequestAudit()
      throws Exception {
    String invalid =
        com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody.builder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .viewId(ViewModelConstants.VIEW_ID)
            .schema(validSensitiveSchema())
            .representations(
                Collections.singletonList(
                    com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation
                        .builder()
                        .type("sql")
                        .dialect("spark")
                        .sql(SECRET_SQL)
                        .build()))
            .sourceDialect("trino")
            .defaultCatalog(ViewModelConstants.DEFAULT_CATALOG)
            .defaultNamespace(ViewModelConstants.DEFAULT_NAMESPACE)
            .baseMetadataLocation(SECRET_BASE)
            .build()
            .toJson();

    assertWriteAliasRequestAudit(
        MockMvcRequestBuilders.post(URI.create(DATABASE_ROUTE + "/%76iews")), invalid, 400);

    Mockito.verifyNoInteractions(houseTableRepository, viewCommitEngine, opaHandler);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  private void assertWriteAliasRequestAudit(
      org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request,
      String body,
      int expectedStatus)
      throws Exception {
    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      withToken(
                          request
                              .contentType(MediaType.APPLICATION_JSON)
                              .content(body)
                              .accept(MediaType.APPLICATION_JSON))))
              .andReturn();

      assertEquals(
          expectedStatus,
          result.getResponse().getStatus(),
          "The alias must reach the view write handler: "
              + result.getResponse().getContentAsString());
      assertTrue(result.getHandler() instanceof HandlerMethod, String.valueOf(result.getHandler()));
      assertEquals(ViewsController.class, ((HandlerMethod) result.getHandler()).getBeanType());
      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    ServiceAuditEvent event = captureServiceAudit();
    assertEquals(expectedStatus, event.getStatusCode());
    assertRequestAuditKeepsSuppliedDefinition(event, body);
  }

  /**
   * The server-owned {@code replace.drop-dialect.allowed} view property is rejected by the API
   * validator as a sanitized 400 on create and replace, before the service is entered: no House
   * Table capture, authorization, admission, storage allocation, engine commit, or operation audit.
   * The request audit records the 400 and keeps the supplied definition.
   */
  @ParameterizedTest
  @ValueSource(strings = {"POST", "PUT"})
  public void serverOwnedDropDialectPropertyIsASanitized400BeforeTheService(String method)
      throws Exception {
    String body =
        ViewModelConstants.fullyPopulatedRequest()
            .toBuilder()
            .baseMetadataLocation(
                "POST".equals(method) ? null : ViewModelConstants.METADATA_LOCATION)
            .viewProperties(Collections.singletonMap("replace.drop-dialect.allowed", "false"))
            .build()
            .toJson();
    org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request =
        "POST".equals(method)
            ? MockMvcRequestBuilders.post(VIEWS_PATH)
            : MockMvcRequestBuilders.put(VIEW_PATH);
    // Were the request to reach the service, it would be authorized, capture a view (PUT) and
    // reach the engine, which rejects this caller-supplied server-owned key as the real engine
    // does. The validator must reject it first.
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    if ("PUT".equals(method)) {
      when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
          .thenReturn(Optional.of(ViewsManagedAuthMatrixBase.viewRow()));
    }
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenThrow(
            new org.apache.iceberg.exceptions.BadRequestException(
                "Property %s is owned by OpenHouse and cannot be set by a caller",
                "replace.drop-dialect.allowed"));

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      withToken(
                          request
                              .contentType(MediaType.APPLICATION_JSON)
                              .content(body)
                              .accept(MediaType.APPLICATION_JSON))))
              .andExpect(status().isBadRequest())
              .andExpect(
                  jsonPath("$.message")
                      .value(
                          org.hamcrest.Matchers.containsString(
                              "viewProperties : reserved keys are not allowed:"
                                  + " replace.drop-dialect.allowed")))
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    verify(opaHandler, never()).checkAccessDecision(any(), any(DatabaseDto.class), any());
    verify(houseTableRepository, never()).findEntityById(any());
    verify(viewCommitEngine, never()).commit(any());
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    ServiceAuditEvent event = captureServiceAudit();
    assertEquals(400, event.getStatusCode());
    assertRequestAuditKeepsSuppliedDefinition(event, body);
  }

  /**
   * Servlet binding percent-decodes parameter names, so {@code %70ageToken} binds as {@code
   * pageToken}. The continuation must reach the backend, yet the request audit must not keep it.
   */
  @Test
  public void percentEncodedPageTokenNameIsRedactedFromSuccessfulListAudit() throws Exception {
    String token =
        new ViewPageTokenCodec()
            .encode(
                new ViewPageCursor(
                    ViewModelConstants.DATABASE_ID,
                    "viewId",
                    ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE,
                    3,
                    0));
    when(houseTableRepository.findAllViewsByDatabaseId(any(), any()))
        .thenAnswer(
            invocation ->
                new PageImpl<HouseTable>(
                    Collections.emptyList(), invocation.<Pageable>getArgument(1), 0));

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          mvc.perform(
                  MockMvcRequestBuilders.get(
                          URI.create(
                              VIEWS_PATH + "?%70ageToken=" + token + "&sortBy=viewId&size=1"))
                      .accept(MediaType.APPLICATION_JSON)
                      .header("Authorization", "Bearer " + jwtAccessToken))
              .andExpect(status().isOk())
              .andReturn();

      assertFalse(result.getResponse().getContentAsString().contains(token));
      assertFalse(logs.renderedEvents().contains(token), logs.renderedEvents());
    }
    ArgumentCaptor<Pageable> pageable = ArgumentCaptor.forClass(Pageable.class);
    verify(houseTableRepository).findAllViewsByDatabaseId(any(), pageable.capture());
    assertEquals(
        3, pageable.getValue().getPageNumber(), "The encoded name must bind as the continuation.");
    ServiceAuditEvent event = captureServiceAudit();
    AuditEventInspection.assertNoSensitiveProperties(event, token);
    assertTrue(String.valueOf(event.getUri()).contains("size=1"), event.getUri());
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  @Test
  public void mixedLiteralAndEncodedDuplicatePageTokensAreRedactedFromFailureAudit()
      throws Exception {
    String literal = SECRET_PAGE_TOKEN + "-literal";
    String encoded = SECRET_PAGE_TOKEN + "-encoded";

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.get(
                              URI.create(
                                  VIEWS_PATH
                                      + "?pageToken="
                                      + literal
                                      + "&sortBy=viewId&%70ageToken="
                                      + encoded))
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)))
              .andExpect(status().isBadRequest())
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    verify(houseTableRepository, never()).findAllViewsByDatabaseId(any(), any());
    ServiceAuditEvent event = captureServiceAudit();
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    assertTrue(String.valueOf(event.getUri()).contains("sortBy=viewId"), event.getUri());
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  /**
   * A syntactically valid body whose representations is an object fails Jackson binding, but the
   * request audit still parses it and keeps it exactly as supplied; its SQL must not reach the
   * response or the logs.
   */
  @Test
  public void objectShapedRepresentationsAreKeptInRequestAuditButNeverReachWireOrLogs()
      throws Exception {
    String body =
        "{\"databaseId\":\""
            + ViewModelConstants.DATABASE_ID
            + "\",\"viewId\":\""
            + ViewModelConstants.VIEW_ID
            + "\",\"schema\":\""
            + SECRET_SCHEMA
            + "\",\"representations\":{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\""
            + SECRET_SQL
            + "\"}}";

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          expectNoCauseOrStacktrace(
                  mvc.perform(
                      MockMvcRequestBuilders.post(VIEWS_PATH)
                          .contentType(MediaType.APPLICATION_JSON)
                          .content(body)
                          .accept(MediaType.APPLICATION_JSON)
                          .header("Authorization", "Bearer " + jwtAccessToken)))
              .andExpect(status().isBadRequest())
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    assertRequestAuditKeepsSuppliedDefinition(captureServiceAudit(), body);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    Mockito.verifyNoInteractions(houseTableRepository, viewCommitEngine, opaHandler);
  }

  // --- F4: every service-entered write failure emits exactly one FAILED operation event ---

  @Test
  public void disabledWriteEmitsOneFailedOperationAuditWithRequestIdentityOnly() throws Exception {
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(false);

    mvc.perform(withToken(postView())).andExpect(status().isNotFound());

    assertEarlyFailedOperationAudit();
    verify(houseTableRepository, never()).findEntityById(any());
    Mockito.verifyNoInteractions(opaHandler);
  }

  @Test
  public void missingDatabaseWriteEmitsOneFailedOperationAuditWithRequestIdentityOnly()
      throws Exception {
    when(databasesService.getAllDatabases()).thenReturn(Collections.emptyList());

    mvc.perform(withToken(postView())).andExpect(status().isNotFound());

    assertEarlyFailedOperationAudit();
    verify(houseTableRepository, never()).findEntityById(any());
    Mockito.verifyNoInteractions(opaHandler);
  }

  @Test
  public void deniedDropEmitsOneFailedOperationAuditBeforeAnyCapture() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(false);
    String sessionId = "view-session-denied-drop";

    mvc.perform(withToken(deleteView()).header(SESSION_ID_HEADER, sessionId))
        .andExpect(status().isForbidden());

    assertEarlyFailedOperationAudit();
    assertCorrelated(sessionId, OperationStatus.FAILED);
    verify(houseTableRepository, never()).findViewById(any());
    verify(houseTableRepository, never()).deleteViewById(any());
  }

  @Test
  public void prepareWriteOutageIsTypedUnavailableWithOneFailedOperationAudit() throws Exception {
    when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenThrow(
            new HouseTableRepositoryStateUnknownException(
                SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertPrecommitOutage(withToken(postView()));
    verify(viewCommitEngine, never()).commit(any());
    Mockito.verifyNoInteractions(opaHandler);
  }

  @Test
  public void prepareDeleteOutageIsTypedUnavailableWithOneFailedOperationAudit() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(houseTableRepository.findViewById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenThrow(
            new HouseTableRepositoryStateUnknownException(
                SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertPrecommitOutage(withToken(deleteView()));
    verify(houseTableRepository, never()).deleteViewById(any());
  }

  /**
   * A transient failure while probing the database for a read is a typed 503, not a residual 500.
   */
  @Test
  public void databaseProbeOutageOnReadsIsTypedUnavailableWithoutOperationAudit() throws Exception {
    when(databasesService.getAllDatabases())
        .thenThrow(
            new HouseTableRepositoryStateUnknownException(
                SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertReadProbeOutage();
  }

  @Test
  public void gateProbeOutageOnReadsIsTypedUnavailableWithoutOperationAudit() throws Exception {
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID))
        .thenThrow(
            new HouseTableRepositoryStateUnknownException(
                SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertReadProbeOutage();
  }

  private void assertReadProbeOutage() throws Exception {
    assertReadProbeFailure(503);
  }

  private void assertReadProbeFailure(int expectedStatus) throws Exception {
    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      for (String path : new String[] {VIEW_PATH, VIEWS_PATH}) {
        MvcResult result =
            expectNoCauseOrStacktrace(
                    mvc.perform(
                        withToken(
                            MockMvcRequestBuilders.get(path).accept(MediaType.APPLICATION_JSON))))
                .andReturn();
        assertEquals(expectedStatus, result.getResponse().getStatus(), path);
        assertNoSensitive(result.getResponse().getContentAsString());
      }
      assertNoSensitive(logs.renderedEvents());
    }
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    verify(houseTableRepository, never()).findViewById(any());
    verify(houseTableRepository, never()).findAllViewsByDatabaseId(any(), any());
  }

  // Only the typed state-unknown failure is transient (503); any other failure is a 500 fault.

  @Test
  public void genericPrepareWriteFailureIsInternal500WithOneFailedOperationAudit()
      throws Exception {
    when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenThrow(new IllegalStateException(SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertPrecommitFailure(withToken(postView()), 500);
    verify(viewCommitEngine, never()).commit(any());
  }

  @Test
  public void genericPrepareDeleteFailureIsInternal500WithOneFailedOperationAudit()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(houseTableRepository.findViewById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenThrow(new IllegalStateException(SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertPrecommitFailure(withToken(deleteView()), 500);
    verify(houseTableRepository, never()).deleteViewById(any());
  }

  @Test
  public void genericDatabaseProbeFailureOnReadsIsInternal500WithoutOperationAudit()
      throws Exception {
    when(databasesService.getAllDatabases())
        .thenThrow(new IllegalStateException(SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertReadProbeFailure(500);
  }

  @Test
  public void genericGateProbeFailureOnReadsIsInternal500WithoutOperationAudit() throws Exception {
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID))
        .thenThrow(new IllegalStateException(SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertReadProbeFailure(500);
  }

  @Test
  public void disabledReadsStayWithoutOperationAudit() throws Exception {
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(false);

    mvc.perform(withToken(MockMvcRequestBuilders.get(VIEW_PATH).accept(MediaType.APPLICATION_JSON)))
        .andExpect(status().isNotFound());
    mvc.perform(
            withToken(MockMvcRequestBuilders.get(VIEWS_PATH).accept(MediaType.APPLICATION_JSON)))
        .andExpect(status().isNotFound());

    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  private void assertPrecommitOutage(
      org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request)
      throws Exception {
    assertPrecommitFailure(request, 503);
  }

  private void assertPrecommitFailure(
      org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request,
      int expectedStatus)
      throws Exception {
    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result = expectNoCauseOrStacktrace(mvc.perform(request)).andReturn();
      assertEquals(expectedStatus, result.getResponse().getStatus());

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    ViewAuditEvent event = assertEarlyFailedOperationAudit();
    assertNotEquals(
        OperationStatus.UNKNOWN,
        event.getOperationStatus(),
        "A failure before any publication is known, not ambiguous.");
  }

  private ViewAuditEvent assertEarlyFailedOperationAudit() {
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    ViewAuditEvent event = captureViewAudit();
    assertEquals(OperationStatus.FAILED, event.getOperationStatus());
    assertEquals(ViewModelConstants.DATABASE_ID, event.getDatabaseName());
    assertEquals(ViewModelConstants.VIEW_ID, event.getViewName());
    assertEquals(PRINCIPAL, event.getUser());
    assertNull(event.getViewUUID(), "No capture was reached, so no identity exists.");
    assertNull(event.getOldMetadataLocation());
    assertNull(event.getNewMetadataLocation());
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    return event;
  }

  // --- F5: audit delivery failure after an acknowledged commit stays a success ---

  @Test
  public void auditSinkFailureAfterAcknowledgedCreateStillReturnsCreated() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.createdResult());

    assertSinkFailureKeepsSuccess(withToken(postView()), 201);
    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
  }

  @Test
  public void auditSinkFailureAfterAcknowledgedReplaceStillReturnsOk() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(Optional.of(ViewsManagedAuthMatrixBase.viewRow()));
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.replacedResult());

    assertSinkFailureKeepsSuccess(
        withToken(
            MockMvcRequestBuilders.put(VIEW_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(ViewModelConstants.fullyPopulatedRequest().toJson())
                .accept(MediaType.APPLICATION_JSON)),
        200);
    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
  }

  @Test
  public void auditSinkFailureAfterAcknowledgedDropStillReturnsNoContent() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    stubDeletableView();

    assertSinkFailureKeepsSuccess(withToken(deleteView()), 204);
    verify(houseTableRepository, times(1)).deleteViewById(ViewsManagedAuthMatrixBase.viewKey());
  }

  private void assertSinkFailureKeepsSuccess(
      org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request,
      int expectedStatus)
      throws Exception {
    assertTrue(
        Metrics.globalRegistry.getRegistries().contains(meterRegistry),
        "Precondition: this context's active meter registry backs the global registry.");
    Mockito.doThrow(
            new IllegalStateException(
                SINK_FAILURE + " " + SECRET_SQL,
                new RuntimeException(SECRET_SCHEMA + " " + SECRET_BASE + " " + SECRET_PAGE_TOKEN)))
        .when(viewAuditHandler)
        .audit(any(ViewAuditEvent.class));
    double failedAuditsBefore = failedServiceAuditCount();

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result = mvc.perform(request).andReturn();

      assertEquals(expectedStatus, result.getResponse().getStatus());
      String renderedLogs = logs.renderedEvents();
      assertNoSensitive(result.getResponse().getContentAsString(), renderedLogs);
      assertFalse(result.getResponse().getContentAsString().contains(SINK_FAILURE));
      assertFalse(
          renderedLogs.contains(SINK_FAILURE),
          "The sink failure is reported without its raw message or throwable: " + renderedLogs);
      assertFalse(
          renderedLogs.contains("java.lang.IllegalStateException"),
          "No raw exception or stack trace is logged: " + renderedLogs);
    }

    ViewAuditEvent attempted = captureViewAudit();
    assertEquals(
        OperationStatus.SUCCESS,
        attempted.getOperationStatus(),
        "Exactly one operation event is attempted, and it is the acknowledged SUCCESS.");
    assertEquals(
        failedAuditsBefore + 1,
        failedServiceAuditCount(),
        "The delivery failure is reported through the existing failed-audit metric.");
    assertEquals(expectedStatus, captureServiceAudit().getStatusCode());
  }

  private double failedServiceAuditCount() {
    return serviceAuditCount(MetricsConstant.FAILED_SERVICE_AUDIT);
  }

  private double serviceAuditCount(String metric) {
    Counter counter = meterRegistry.find(MetricsConstant.SERVICE_AUDIT + "_" + metric).counter();
    return counter == null ? 0.0 : counter.count();
  }

  // --- Request-audit sink failure: the event it received may echo the definition ---

  @Test
  public void requestAuditSinkFailureAfterAcknowledgedCreateKeepsCreatedAndLogsNoDetails()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.createdResult());

    assertRequestAuditSinkFailureKeepsOutcome(201);

    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
    assertEquals(OperationStatus.SUCCESS, captureViewAudit().getOperationStatus());
  }

  @Test
  public void requestAuditSinkFailureAfterAFailedResponseKeepsForbiddenAndLogsNoDetails()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(false);

    assertRequestAuditSinkFailureKeepsOutcome(403);

    verify(viewCommitEngine, never()).commit(any());
    assertEquals(OperationStatus.FAILED, captureViewAudit().getOperationStatus());
  }

  /**
   * The request-audit sink receives the full event once and throws an exception whose message,
   * cause and suppressed exception echo that event's JSON. The HTTP outcome and correlation stay as
   * they were, the existing failed-audit metric is counted, and no diagnostic carries the
   * definition, a token, the sink's message, or the raw throwable.
   */
  private void assertRequestAuditSinkFailureKeepsOutcome(int expectedStatus) throws Exception {
    String sessionId = "view-session-request-sink-failure";
    String body = requestWithSensitiveMarkers();
    AtomicReference<ServiceAuditEvent> delivered = new AtomicReference<>();
    Mockito.doAnswer(
            invocation -> {
              ServiceAuditEvent event = invocation.getArgument(0);
              delivered.set(event);
              IllegalStateException failure =
                  new IllegalStateException(
                      REQUEST_SINK_FAILURE + " " + event.toJson() + " " + SECRET_SQL,
                      new RuntimeException(event.toJson() + " " + SECRET_SCHEMA));
              failure.addSuppressed(new RuntimeException(event.toJson() + " " + SECRET_BASE));
              throw failure;
            })
        .when(serviceAuditHandler)
        .audit(any(ServiceAuditEvent.class));
    double failedAuditsBefore = failedServiceAuditCount();

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          mvc.perform(
                  withToken(
                      MockMvcRequestBuilders.post(VIEWS_PATH)
                          .contentType(MediaType.APPLICATION_JSON)
                          .content(body)
                          .accept(MediaType.APPLICATION_JSON)
                          .header(SESSION_ID_HEADER, sessionId)))
              .andReturn();

      assertEquals(expectedStatus, result.getResponse().getStatus());
      String wire = result.getResponse().getContentAsString();
      String renderedLogs = logs.renderedEvents();
      assertNoSensitive(wire, renderedLogs);
      String[] sinkFailureDetails = {
        REQUEST_SINK_FAILURE, "java.lang.IllegalStateException", "java.lang.RuntimeException"
      };
      for (String forbidden : sinkFailureDetails) {
        assertFalse(wire.contains(forbidden), wire);
        assertFalse(renderedLogs.contains(forbidden), renderedLogs);
      }
    }

    verify(serviceAuditHandler, times(1)).audit(any(ServiceAuditEvent.class));
    assertEquals(
        failedAuditsBefore + 1,
        failedServiceAuditCount(),
        "The request-audit delivery failure is reported through the existing metric.");
    ServiceAuditEvent event = delivered.get();
    assertEquals(expectedStatus, event.getStatusCode());
    assertEquals(sessionId, event.getSessionId());
    assertRequestAuditKeepsSuppliedDefinition(event, body);
  }

  /**
   * The request audit keeps the supplied definition exactly: the parsed body as sent, with only a
   * supplied CAS token value replaced (its key kept). No CAS or page token survives anywhere in the
   * event.
   */
  private static void assertRequestAuditKeepsSuppliedDefinition(
      ServiceAuditEvent event, String sentBody) {
    JsonObject expected = JsonParser.parseString(sentBody).getAsJsonObject();
    if (expected.has("baseMetadataLocation")) {
      expected.addProperty("baseMetadataLocation", ServiceAuditPayloadRedactor.REDACTED_VALUE);
    }
    assertEquals(
        expected, event.getRequestPayload(), "Request audit keeps the definition as sent.");
    JsonObject sent = JsonParser.parseString(sentBody).getAsJsonObject();
    JsonObject audited = event.getRequestPayload().getAsJsonObject();
    for (String definitionField : new String[] {"schema", "representations"}) {
      assertTrue(sent.has(definitionField), "Fixture must supply " + definitionField);
      assertEquals(
          sent.get(definitionField),
          audited.get(definitionField),
          "The supplied " + definitionField + " is kept exactly as sent.");
    }
    AuditEventInspection.assertNoSensitiveProperties(event, REQUEST_TOKEN_SECRETS);
  }

  // --- F10: SUCCESS, FAILED and UNKNOWN correlation is asserted in their primary audit cases ---

  /** A correlation id from one request must not carry over to the next request on the thread. */
  @Test
  public void sessionIdDoesNotLeakIntoALaterRequestWithoutTheHeader() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.createdResult());

    mvc.perform(withToken(postView()).header(SESSION_ID_HEADER, "view-session-first"))
        .andExpect(status().isCreated());
    Mockito.clearInvocations(serviceAuditHandler, viewAuditHandler);
    mvc.perform(withToken(postView())).andExpect(status().isCreated());

    ServiceAuditEvent serviceEvent = captureServiceAudit();
    ViewAuditEvent viewEvent = captureViewAudit();
    AuditEventInspection.assertHasProperty(viewEvent, "sessionId");
    assertNull(serviceEvent.getSessionId());
    assertNull(AuditEventInspection.properties(viewEvent).get("sessionId"));
  }

  private void assertCorrelated(String sessionId, OperationStatus expectedStatus) {
    ServiceAuditEvent serviceEvent = captureServiceAudit();
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(expectedStatus, viewEvent.getOperationStatus());
    assertEquals(sessionId, serviceEvent.getSessionId());
    AuditEventInspection.assertHasProperty(viewEvent, "sessionId");
    assertEquals(
        serviceEvent.getSessionId(), AuditEventInspection.properties(viewEvent).get("sessionId"));
  }

  private void stubDeletableView() {
    when(houseTableRepository.findViewById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(Optional.of(ViewsManagedAuthMatrixBase.viewRow()));
    when(houseTableRepository.deleteViewById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(true);
  }

  private org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder withToken(
      org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder builder) {
    return builder.header("Authorization", "Bearer " + jwtAccessToken);
  }

  private static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder
      postView() {
    return MockMvcRequestBuilders.post(VIEWS_PATH)
        .contentType(MediaType.APPLICATION_JSON)
        .content(ViewModelConstants.createRequestWithoutBaseVersion().toJson())
        .accept(MediaType.APPLICATION_JSON);
  }

  private static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder
      deleteView() {
    return MockMvcRequestBuilders.delete(VIEW_PATH).accept(MediaType.APPLICATION_JSON);
  }

  /**
   * A representations array whose elements are bare or nested SQL strings fails binding, but the
   * request audit still parses the body and keeps it exactly as supplied; that SQL must not reach
   * the response or the logs.
   */
  @Test
  public void primitiveAndNestedArrayRepresentationsAreKeptInRequestAuditButNeverReachWireOrLogs()
      throws Exception {
    for (String representations :
        new String[] {
          "[\"" + SECRET_SQL + "\"]",
          "[[\"" + SECRET_SQL + "\"]]",
          "[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"ok\"},\"" + SECRET_SQL + "\"]"
        }) {
      Mockito.reset(serviceAuditHandler);
      String body =
          "{\"databaseId\":\""
              + ViewModelConstants.DATABASE_ID
              + "\",\"viewId\":\""
              + ViewModelConstants.VIEW_ID
              + "\",\"schema\":\""
              + SECRET_SCHEMA
              + "\",\"representations\":"
              + representations
              + "}";

      try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
        MvcResult result =
            expectNoCauseOrStacktrace(
                    mvc.perform(
                        MockMvcRequestBuilders.post(VIEWS_PATH)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body)
                            .accept(MediaType.APPLICATION_JSON)
                            .header("Authorization", "Bearer " + jwtAccessToken)))
                .andExpect(status().isBadRequest())
                .andReturn();

        assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
      }
      assertRequestAuditKeepsSuppliedDefinition(captureServiceAudit(), body);
    }
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    Mockito.verifyNoInteractions(houseTableRepository, viewCommitEngine, opaHandler);
  }

  private static ResultActions expectNoCauseOrStacktrace(ResultActions actions) throws Exception {
    return actions
        .andExpect(jsonPath("$.cause").doesNotExist())
        .andExpect(jsonPath("$.stacktrace").doesNotExist());
  }

  private static String requestWithSensitiveMarkers() {
    return sensitiveRequest(null);
  }

  private static String sensitiveRequest(String baseMetadataLocation) {
    return ViewModelConstants.fullyPopulatedRequest()
        .toBuilder()
        .schema(validSensitiveSchema())
        .representations(
            Collections.singletonList(
                com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation
                    .builder()
                    .type("sql")
                    .dialect("spark")
                    .sql(SECRET_SQL)
                    .build()))
        .baseMetadataLocation(baseMetadataLocation)
        .build()
        .toJson();
  }

  private static String validSensitiveSchema() {
    return "{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
        + "{\"id\":1,\"required\":true,\"name\":\""
        + SECRET_SCHEMA
        + "\",\"type\":\"string\"}]}";
  }

  private ServiceAuditEvent captureServiceAudit() {
    ArgumentCaptor<ServiceAuditEvent> event = ArgumentCaptor.forClass(ServiceAuditEvent.class);
    verify(serviceAuditHandler, times(1)).audit(event.capture());
    return event.getValue();
  }

  private ViewAuditEvent captureViewAudit() {
    ArgumentCaptor<ViewAuditEvent> event = ArgumentCaptor.forClass(ViewAuditEvent.class);
    verify(viewAuditHandler, times(1)).audit(event.capture());
    return event.getValue();
  }

  private static void assertNoSensitive(String... values) {
    for (String value : values) {
      if (value == null) {
        continue;
      }
      for (String secret : SECRETS) {
        assertFalse(value.contains(secret), value);
      }
    }
  }

  @TestConfiguration
  static class Config {
    @Bean
    ViewExceptionHandler viewExceptionHandler() {
      return new ViewExceptionHandler();
    }
  }
}
