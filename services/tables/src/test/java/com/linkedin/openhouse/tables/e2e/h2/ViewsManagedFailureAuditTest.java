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

import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.ViewCommitEngine;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.authorization.OpaHandler;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewExceptionHandler;
import com.linkedin.openhouse.tables.mock.audit.AuditEventInspection;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.services.DatabasesService;
import com.linkedin.openhouse.tables.services.ViewAdmissionService;
import com.linkedin.openhouse.tables.services.ViewPageCursor;
import com.linkedin.openhouse.tables.services.ViewPageTokenCodec;
import com.linkedin.openhouse.tables.services.ViewPaginationAdapter;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
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
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
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

/**
 * Managed view failure handling and the operation-audit contract, through real MVC, view advice and
 * service. Request-audit content, request-audit delivery and diagnostic logs are out of scope here:
 * the request-audit aspect still runs (into the mocked request handler below) but is not asserted
 * on.
 */
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
  /** Forbidden in every HTTP response and in the identity-only operation audit event. */
  private static final String[] SECRETS = {
    SECRET_SQL, SECRET_SCHEMA, SECRET_BASE, SECRET_PAGE_TOKEN
  };

  private static final String DROP_DIALECT_SENTINEL = "drop-dialect-value-sentinel";
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
  // Real behavior, observed: the service's admission check and its primary storage selector.
  @SpyBean private ViewAdmissionService viewAdmissionService;

  @SpyBean(name = "provideStorageSelector")
  private StorageSelector storageSelector;

  @MockBean private OpaHandler opaHandler;
  // SpringH2Application defines bean "serviceAuditHandler" and the scanned DummyServiceAuditHandler
  // is a second candidate; ServiceAuditAspect injects by that field name, so replace that bean.
  // Request-audit events reach it but are not asserted on here.
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

  /** A syntactically malformed body is a safe 400 before the service and the operation audit. */
  @Test
  public void malformedJsonIsASafe400WithoutServiceOrOperationAudit() throws Exception {
    String malformed =
        "{\"viewId\":\""
            + ViewModelConstants.VIEW_ID
            + "\",\"schema\":\""
            + SECRET_SCHEMA
            + "\",\"representations\":[{\"sql\":\""
            + SECRET_SQL
            + "\"}],";

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

    assertNoSensitive(result.getResponse().getContentAsString());
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    Mockito.verifyNoInteractions(houseTableRepository, viewCommitEngine, opaHandler);
  }

  @Test
  public void successfulCreateEmitsOneSuccessOperationAuditWithCommittedPointer() throws Exception {
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

    // Positive anchors for the observed admission and storage seams used by negative cases.
    verify(viewAdmissionService, times(1)).admit(any());
    verify(storageSelector, times(1))
        .selectStorage(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
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

    assertNoSensitive(result.getResponse().getContentAsString());
    verify(viewCommitEngine, never()).commit(any());
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

    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  /**
   * A nonnumeric and an int-overflowing LIST {@code size} both fail Spring binding with
   * MethodArgumentTypeMismatchException before the handler runs. They must keep the existing 400
   * with the fixed safe message, never the residual 500, and the response must not echo the raw
   * size or parser details.
   */
  @Test
  public void malformedAndOverflowingSizeKeepFixedSafe400BeforeService() throws Exception {
    assertPreServiceSizeBindingFailure("abcNotAnIntegerSize");
    assertPreServiceSizeBindingFailure("99999999999");
  }

  private void assertPreServiceSizeBindingFailure(String rawSize) throws Exception {
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
    assertFalse(wire.contains(rawSize), wire);
    for (String parserDetail : BINDING_PARSER_DETAILS) {
      assertFalse(wire.contains(parserDetail), wire);
    }
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
    Mockito.verifyNoInteractions(
        viewsFeatureGate, databasesService, houseTableRepository, viewCommitEngine, opaHandler);
  }

  @Test
  public void unexpectedRuntimeFailureMapsToSanitized500() throws Exception {
    when(houseTableRepository.findViewById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenThrow(new RuntimeException(SECRET_SQL));

    MvcResult result =
        expectNoCauseOrStacktrace(
                mvc.perform(
                    MockMvcRequestBuilders.get(VIEW_PATH)
                        .accept(MediaType.APPLICATION_JSON)
                        .header("Authorization", "Bearer " + jwtAccessToken)))
            .andExpect(status().isInternalServerError())
            .andReturn();

    assertNoSensitive(result.getResponse().getContentAsString());
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
  public void opaUnavailableWriteIsASafe503WithOneFailedOperationAudit() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenThrow(new RuntimeException(SECRET_SQL + " " + SECRET_SCHEMA + " " + SECRET_BASE));

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

    assertNoSensitive(result.getResponse().getContentAsString());
    verify(viewCommitEngine, never()).commit(any());
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

    assertNoSensitive(result.getResponse().getContentAsString());
    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    assertEquals(Boolean.TRUE, intent.getValue().getIsCreate());
    verify(houseTableRepository, never()).deleteViewById(any());
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
  public void listBackendFailureWithAContinuationIsASafe500WithoutOperationAudit()
      throws Exception {
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
    assertNoSensitive(wire);
    assertFalse(wire.contains(token), wire);
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  @Test
  public void malformedPageTokenIsASafe400AndDoesNotReachBackend() throws Exception {
    MvcResult result =
        expectNoCauseOrStacktrace(
                mvc.perform(
                    MockMvcRequestBuilders.get(
                            VIEWS_PATH + "?pageToken=" + SECRET_PAGE_TOKEN + "&sortBy=viewId")
                        .accept(MediaType.APPLICATION_JSON)
                        .header("Authorization", "Bearer " + jwtAccessToken)))
            .andExpect(status().isBadRequest())
            .andReturn();

    assertNoSensitive(result.getResponse().getContentAsString());
    verify(houseTableRepository, never()).findAllViewsByDatabaseId(any(), any());
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  /**
   * The server-owned {@code replace.drop-dialect.allowed} view property is rejected by the API
   * validator as a sanitized 400 on create and replace, before the service is entered: no House
   * Table capture, authorization, admission, storage allocation, engine commit, or operation audit.
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
            .viewProperties(
                Collections.singletonMap("replace.drop-dialect.allowed", DROP_DIALECT_SENTINEL))
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

    String wire = result.getResponse().getContentAsString();
    assertNoSensitive(wire);
    assertFalse(wire.contains(DROP_DIALECT_SENTINEL), wire);
    verify(opaHandler, never()).checkAccessDecision(any(), any(DatabaseDto.class), any());
    Mockito.verifyNoInteractions(houseTableRepository, viewCommitEngine);
    verify(viewAdmissionService, never()).admit(any());
    verify(storageSelector, never()).selectStorage(any(), any());
    verify(viewAuditHandler, never()).audit(any(ViewAuditEvent.class));
  }

  /**
   * A syntactically valid body whose representations is an object fails Jackson binding: a safe 400
   * before the service, with no operation audit.
   */
  @Test
  public void objectShapedRepresentationsAreASafe400BeforeTheService() throws Exception {
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

    assertNoSensitive(result.getResponse().getContentAsString());
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
    MvcResult result = expectNoCauseOrStacktrace(mvc.perform(request)).andReturn();
    assertEquals(expectedStatus, result.getResponse().getStatus());
    assertNoSensitive(result.getResponse().getContentAsString());

    ViewAuditEvent event = assertEarlyFailedOperationAudit();
    assertNotEquals(
        OperationStatus.UNKNOWN,
        event.getOperationStatus(),
        "A failure before any publication is known, not ambiguous.");
  }

  private ViewAuditEvent assertEarlyFailedOperationAudit() {
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

  // --- F5: operation-audit delivery failure after an acknowledged commit stays a success ---

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

    MvcResult result = mvc.perform(request).andReturn();

    assertEquals(expectedStatus, result.getResponse().getStatus());
    assertNoSensitive(result.getResponse().getContentAsString());
    assertFalse(result.getResponse().getContentAsString().contains(SINK_FAILURE));
    ViewAuditEvent attempted = captureViewAudit();
    assertEquals(
        OperationStatus.SUCCESS,
        attempted.getOperationStatus(),
        "Exactly one operation event is attempted, and it is the acknowledged SUCCESS.");
    assertEquals(
        failedAuditsBefore + 1,
        failedServiceAuditCount(),
        "The delivery failure is reported through the existing failed-audit metric.");
  }

  private double failedServiceAuditCount() {
    Counter counter =
        meterRegistry
            .find(MetricsConstant.SERVICE_AUDIT + "_" + MetricsConstant.FAILED_SERVICE_AUDIT)
            .counter();
    return counter == null ? 0.0 : counter.count();
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
    Mockito.clearInvocations(viewAuditHandler);
    mvc.perform(withToken(postView())).andExpect(status().isCreated());

    ViewAuditEvent viewEvent = captureViewAudit();
    AuditEventInspection.assertHasProperty(viewEvent, "sessionId");
    assertNull(AuditEventInspection.properties(viewEvent).get("sessionId"));
  }

  /** The operation event carries the request's inbound {@code session-id} header verbatim. */
  private void assertCorrelated(String sessionId, OperationStatus expectedStatus) {
    ViewAuditEvent viewEvent = captureViewAudit();
    assertEquals(expectedStatus, viewEvent.getOperationStatus());
    AuditEventInspection.assertHasProperty(viewEvent, "sessionId");
    assertEquals(sessionId, AuditEventInspection.properties(viewEvent).get("sessionId"));
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
   * A representations array whose elements are bare or nested SQL strings fails binding: a safe 400
   * before the service, with no operation audit.
   */
  @Test
  public void primitiveAndNestedArrayRepresentationsAreASafe400BeforeTheService() throws Exception {
    String[] malformedRepresentations = {
      "[\"" + SECRET_SQL + "\"]",
      "[[\"" + SECRET_SQL + "\"]]",
      "[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"ok\"},\"" + SECRET_SQL + "\"]"
    };
    for (String representations : malformedRepresentations) {
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

      assertNoSensitive(result.getResponse().getContentAsString());
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

  private ViewAuditEvent captureViewAudit() {
    ArgumentCaptor<ViewAuditEvent> event = ArgumentCaptor.forClass(ViewAuditEvent.class);
    verify(viewAuditHandler, times(1)).audit(event.capture());
    return event.getValue();
  }

  /** The HTTP response never echoes a definition, CAS token or continuation. */
  private static void assertNoSensitive(String wire) {
    for (String secret : SECRETS) {
      assertFalse(wire.contains(secret), wire);
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
