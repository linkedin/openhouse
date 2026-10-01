package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.authorization.OpaHandler;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewExceptionHandler;
import com.linkedin.openhouse.tables.mock.audit.AuditEventInspection;
import com.linkedin.openhouse.tables.mock.logging.Log4j2LogCapture;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalViewRepository;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import com.linkedin.openhouse.tables.repository.impl.PreparedViewOperation;
import com.linkedin.openhouse.tables.services.DatabasesService;
import com.linkedin.openhouse.tables.services.ViewPageCursor;
import com.linkedin.openhouse.tables.services.ViewPageTokenCodec;
import com.linkedin.openhouse.tables.services.ViewPaginationAdapter;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import java.lang.reflect.Method;
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

    mvc.perform(
            MockMvcRequestBuilders.post(VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestWithSensitiveMarkers())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
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
