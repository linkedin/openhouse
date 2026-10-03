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

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.authorization.OpaHandler;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewExceptionHandler;
import com.linkedin.openhouse.tables.mock.audit.AuditEventInspection;
import com.linkedin.openhouse.tables.mock.logging.Log4j2LogCapture;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalViewRepository;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import com.linkedin.openhouse.tables.repository.impl.PreparedViewOperation;
import com.linkedin.openhouse.tables.services.DatabasesService;
import com.linkedin.openhouse.tables.services.ViewPageCursor;
import com.linkedin.openhouse.tables.services.ViewPageTokenCodec;
import com.linkedin.openhouse.tables.services.ViewPaginationAdapter;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
  private static final String SECRET_SQL = "select very_secret_sql";
  private static final String SECRET_SCHEMA = "very_secret_schema";
  private static final String SECRET_BASE = "file:/very-secret-base.metadata.json";
  private static final String SECRET_PAGE_TOKEN = "very-secret-page-token";
  private static final String[] SECRETS = {
    SECRET_SQL, SECRET_SCHEMA, SECRET_BASE, SECRET_PAGE_TOKEN
  };
  private static final String CAPTURED_UUID = "view-uuid";
  private static final String SESSION_ID_HEADER = "session-id";
  private static final String SINK_FAILURE = "operation-audit-sink-failure-marker";
  private static final String SIZE_BINDING_MESSAGE = "size : must be an integer";
  private static final String[] BINDING_PARSER_DETAILS = {
    "NumberFormatException", "For input string", "Failed to convert", "java.lang"
  };

  @Autowired private MockMvc mvc;

  @MockBean private ViewsFeatureGate viewsFeatureGate;
  @MockBean private DatabasesService databasesService;
  @MockBean private OpenHouseInternalViewRepository viewRepository;
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
        viewRepository,
        opaHandler,
        serviceAuditHandler,
        viewAuditHandler);
    jwtAccessToken = new DummyTokenInterceptor.DummySecurityJWT(PRINCIPAL).buildNoopJWT();
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases())
        .thenReturn(
            Collections.singletonList(
                DatabaseDto.builder().databaseId(ViewModelConstants.DATABASE_ID).build()));
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.observedAbsence());
  }

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
    }
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  @Test
  public void successfulCreateEmitsOneRequestAndOneSuccessOperationAuditWithCommittedPointer()
      throws Exception {
    ViewCommitOutcome committed = ViewsManagedAuthMatrixBase.createdOutcome();
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewRepository.commitCreate(any(), any(), any())).thenReturn(committed);
    String sessionId = "view-session-success";

    mvc.perform(
            MockMvcRequestBuilders.post(VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestWithSensitiveMarkers())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken)
                .header(SESSION_ID_HEADER, sessionId))
        .andExpect(status().isCreated());

    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(OperationStatus.SUCCESS, viewEvent.getOperationStatus());
    assertEquals(
        ViewsManagedAuthMatrixBase.CREATED_VIEW_UUID,
        viewEvent.getViewUUID(),
        "Create audit UUID comes from the committed outcome carrier.");
    assertNull(viewEvent.getOldMetadataLocation(), "A create has no prior pointer.");
    assertEquals(committed.getDto().getMetadataLocation(), viewEvent.getNewMetadataLocation());
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
    ViewCommitOutcome committed = ViewsManagedAuthMatrixBase.replacedOutcome();
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.view(ViewsManagedAuthMatrixBase.viewRow()));
    when(viewRepository.commitReplace(any(), any(), any())).thenReturn(committed);

    mvc.perform(
            MockMvcRequestBuilders.put(VIEW_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(ViewModelConstants.fullyPopulatedRequest().toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk());

    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(OperationStatus.SUCCESS, viewEvent.getOperationStatus());
    assertEquals(CAPTURED_UUID, viewEvent.getViewUUID());
    assertEquals(ViewModelConstants.METADATA_LOCATION, viewEvent.getOldMetadataLocation());
    assertEquals(committed.getDto().getMetadataLocation(), viewEvent.getNewMetadataLocation());
    assertEquals(PRINCIPAL, viewEvent.getUser());
    AuditEventInspection.assertNoSensitiveProperties(viewEvent, SECRETS);
  }

  @Test
  public void successfulDropAuditsCapturedIdentityAndOldPointerWithoutNewPointer()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.view(ViewsManagedAuthMatrixBase.viewRow()));

    mvc.perform(
            MockMvcRequestBuilders.delete(VIEW_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNoContent());

    verify(viewRepository).deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
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
    verify(viewRepository, never()).commitCreate(any(), any(), any());
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
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
    Mockito.verifyNoInteractions(viewsFeatureGate, databasesService, viewRepository, opaHandler);
  }

  @Test
  public void unexpectedRuntimeFailureMapsToSanitized500() throws Exception {
    when(viewRepository.findById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
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
    verify(viewRepository, never()).commitCreate(any(), any(), any());
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
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
    when(viewRepository.commitCreate(any(), any(), any()))
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

    verify(viewRepository, times(1)).commitCreate(any(), any(), any());
    verify(viewRepository, never()).commitReplace(any(), any(), any());
    verify(viewRepository, never()).deleteById(any(), any());
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
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
    when(viewRepository.searchViews(any(), any()))
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
    verify(viewRepository, never()).searchViews(any(), any());
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
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
    when(viewRepository.searchViews(any(), any()))
        .thenAnswer(
            invocation ->
                new PageImpl<ViewDto>(
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
    verify(viewRepository).searchViews(any(), pageable.capture());
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
    verify(viewRepository, never()).searchViews(any(), any());
    ServiceAuditEvent event = captureServiceAudit();
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    assertTrue(String.valueOf(event.getUri()).contains("sortBy=viewId"), event.getUri());
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  /**
   * A syntactically valid body whose representations is an object fails Jackson binding, but the
   * request audit still parses it; its SQL must not reach any surface.
   */
  @Test
  public void objectShapedRepresentationsSqlNeverReachesAuditWireOrLogs() throws Exception {
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
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    Mockito.verifyNoInteractions(viewRepository, opaHandler);
  }

  // --- F4: every service-entered write failure emits exactly one FAILED operation event ---

  @Test
  public void disabledWriteEmitsOneFailedOperationAuditWithRequestIdentityOnly() throws Exception {
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(false);

    mvc.perform(withToken(postView())).andExpect(status().isNotFound());

    assertEarlyFailedOperationAudit();
    verify(viewRepository, never()).prepareWrite(any(), any());
    Mockito.verifyNoInteractions(opaHandler);
  }

  @Test
  public void missingDatabaseWriteEmitsOneFailedOperationAuditWithRequestIdentityOnly()
      throws Exception {
    when(databasesService.getAllDatabases()).thenReturn(Collections.emptyList());

    mvc.perform(withToken(postView())).andExpect(status().isNotFound());

    assertEarlyFailedOperationAudit();
    verify(viewRepository, never()).prepareWrite(any(), any());
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
    verify(viewRepository, never()).prepareDelete(any(), any());
    verify(viewRepository, never()).deleteById(any(), any());
  }

  @Test
  public void prepareWriteOutageIsTypedUnavailableWithOneFailedOperationAudit() throws Exception {
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(
            new HouseTableRepositoryStateUnknownException(
                SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertPrecommitOutage(withToken(postView()));
    verify(viewRepository, never()).commitCreate(any(), any(), any());
    Mockito.verifyNoInteractions(opaHandler);
  }

  @Test
  public void prepareDeleteOutageIsTypedUnavailableWithOneFailedOperationAudit() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(
            new HouseTableRepositoryStateUnknownException(
                SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertPrecommitOutage(withToken(deleteView()));
    verify(viewRepository, never()).deleteById(any(), any());
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
    verify(viewRepository, never()).findById(any(), any());
    verify(viewRepository, never()).searchViews(any(), any());
  }

  // Only the typed state-unknown failure is transient (503); any other failure is a 500 fault.

  @Test
  public void genericPrepareWriteFailureIsInternal500WithOneFailedOperationAudit()
      throws Exception {
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(new IllegalStateException(SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertPrecommitFailure(withToken(postView()), 500);
    verify(viewRepository, never()).commitCreate(any(), any(), any());
  }

  @Test
  public void genericPrepareDeleteFailureIsInternal500WithOneFailedOperationAudit()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(new IllegalStateException(SECRET_SQL, new RuntimeException(SECRET_BASE)));

    assertPrecommitFailure(withToken(deleteView()), 500);
    verify(viewRepository, never()).deleteById(any(), any());
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
    when(viewRepository.commitCreate(any(), any(), any()))
        .thenReturn(ViewsManagedAuthMatrixBase.createdOutcome());

    assertSinkFailureKeepsSuccess(withToken(postView()), 201);
    verify(viewRepository, times(1)).commitCreate(any(), any(), any());
  }

  @Test
  public void auditSinkFailureAfterAcknowledgedReplaceStillReturnsOk() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.view(ViewsManagedAuthMatrixBase.viewRow()));
    when(viewRepository.commitReplace(any(), any(), any()))
        .thenReturn(ViewsManagedAuthMatrixBase.replacedOutcome());

    assertSinkFailureKeepsSuccess(
        withToken(
            MockMvcRequestBuilders.put(VIEW_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(ViewModelConstants.fullyPopulatedRequest().toJson())
                .accept(MediaType.APPLICATION_JSON)),
        200);
    verify(viewRepository, times(1)).commitReplace(any(), any(), any());
  }

  @Test
  public void auditSinkFailureAfterAcknowledgedDropStillReturnsNoContent() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.view(ViewsManagedAuthMatrixBase.viewRow()));

    assertSinkFailureKeepsSuccess(withToken(deleteView()), 204);
    verify(viewRepository, times(1))
        .deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
  }

  private void assertSinkFailureKeepsSuccess(
      org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request,
      int expectedStatus)
      throws Exception {
    assertFalse(
        Metrics.globalRegistry.getRegistries().isEmpty(),
        "Precondition: a meter registry backs the global registry in this context.");
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

  private static double failedServiceAuditCount() {
    Counter counter =
        Metrics.globalRegistry
            .find(MetricsConstant.SERVICE_AUDIT + "_" + MetricsConstant.FAILED_SERVICE_AUDIT)
            .counter();
    return counter == null ? 0.0 : counter.count();
  }

  // --- F10: SUCCESS, FAILED and UNKNOWN correlation is asserted in their primary audit cases ---

  /** A correlation id from one request must not carry over to the next request on the thread. */
  @Test
  public void sessionIdDoesNotLeakIntoALaterRequestWithoutTheHeader() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewRepository.commitCreate(any(), any(), any()))
        .thenReturn(ViewsManagedAuthMatrixBase.createdOutcome());

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
   * request audit still parses the body; that SQL must not reach any surface.
   */
  @Test
  public void primitiveAndNestedArrayRepresentationsSqlNeverReachesAuditWireOrLogs()
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
      AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    }
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    Mockito.verifyNoInteractions(viewRepository, opaHandler);
  }

  private static ResultActions expectNoCauseOrStacktrace(ResultActions actions) throws Exception {
    return actions
        .andExpect(jsonPath("$.cause").doesNotExist())
        .andExpect(jsonPath("$.stacktrace").doesNotExist());
  }

  private static String requestWithSensitiveMarkers() {
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
        .baseMetadataLocation(null)
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
