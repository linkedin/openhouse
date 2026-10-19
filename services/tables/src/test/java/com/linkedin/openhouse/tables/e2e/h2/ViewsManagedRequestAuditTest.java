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
import com.linkedin.openhouse.tables.authorization.OpaHandler;
import com.linkedin.openhouse.tables.controller.ViewsController;
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
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.web.method.HandlerMethod;

/**
 * Request audit and request diagnostics for managed view requests, through real MVC, view advice,
 * the request-audit aspect and service: the request event's definition, CAS and continuation
 * redaction on canonical and MVC-accepted alias routes, request-audit delivery failures, parse
 * failures, and the rendered logs (including thrown causes). Scenarios replayed from {@link
 * ViewsManagedFailureAuditTest} assert only the request-audit and diagnostic slice; per-operation
 * view audit, and its correlation with these request events, is deferred to a follow-up.
 */
@SpringBootTest(classes = {SpringH2Application.class, ViewsManagedRequestAuditTest.Config.class})
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@TestPropertySource(
    properties = {
      "cluster.security.token.interceptor.classname=com.linkedin.openhouse.common.security.DummyTokenInterceptor",
      "cluster.security.tables.authorization.enabled=true",
      "cluster.security.tables.authorization.opa.base-uri=http://opa.test"
    })
public class ViewsManagedRequestAuditTest {

  private static final String PRINCIPAL = "failure-user";
  private static final String VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";
  private static final String VIEW_PATH = VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID;
  private static final String DATABASE_ROUTE = "/v1/databases/" + ViewModelConstants.DATABASE_ID;
  private static final String SECRET_SQL = "select very_secret_sql";
  private static final String SECRET_SCHEMA = "very_secret_schema";
  private static final String SECRET_BASE = "file:/very-secret-base.metadata.json";
  private static final String SECRET_PAGE_TOKEN = "very-secret-page-token";
  /** Forbidden on every diagnostic surface: responses and rendered logs, thrown causes included. */
  private static final String[] SECRETS = {
    SECRET_SQL, SECRET_SCHEMA, SECRET_BASE, SECRET_PAGE_TOKEN
  };

  /**
   * Forbidden in the request audit event. The request audit keeps the supplied view definition
   * (schema and SQL) as request provenance, so only the CAS and page tokens are listed here.
   */
  private static final String[] REQUEST_TOKEN_SECRETS = {SECRET_BASE, SECRET_PAGE_TOKEN};

  private static final String DROP_DIALECT_SENTINEL = "drop-dialect-value-sentinel";
  private static final String REQUEST_SINK_FAILURE = "request-audit-sink-failure-marker";
  private static final String[] PARSER_DETAILS = {
    "com.google.gson", "MalformedJsonException", "JsonSyntaxException", "End of input"
  };
  private static final String PARSE_FAILURE_DIAGNOSTIC =
      "Exception during parsing request payload (details omitted)";
  private static final String SUCCESS_DELIVERY_FAILURE_DIAGNOSTIC =
      "Exception during auditing successful requests (details omitted)";
  private static final String ERROR_DELIVERY_FAILURE_DIAGNOSTIC =
      "Exception during auditing failed requests (details omitted)";
  private static final String SESSION_ID_HEADER = "session-id";
  private static final String SIZE_BINDING_MESSAGE = "size : must be an integer";
  private static final String[] BINDING_PARSER_DETAILS = {
    "NumberFormatException", "For input string", "Failed to convert", "java.lang"
  };

  @Autowired private MockMvc mvc;
  @Autowired private MeterRegistry meterRegistry;

  @MockBean private ViewsFeatureGate viewsFeatureGate;
  @MockBean private DatabasesService databasesService;

  @MockBean(name = "houseTablesH2Repository")
  private HouseTablesH2Repository houseTableRepository;

  @MockBean private ViewCommitEngine viewCommitEngine;
  @MockBean private OpaHandler opaHandler;
  // SpringH2Application defines bean "serviceAuditHandler" and the scanned DummyServiceAuditHandler
  // is a second candidate; ServiceAuditAspect injects by that field name, so replace that bean.
  @MockBean(name = "serviceAuditHandler")
  private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws Exception {
    Mockito.reset(
        viewsFeatureGate,
        databasesService,
        houseTableRepository,
        viewCommitEngine,
        opaHandler,
        serviceAuditHandler);
    jwtAccessToken = new DummyTokenInterceptor.DummySecurityJWT(PRINCIPAL).buildNoopJWT();
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases())
        .thenReturn(
            Collections.singletonList(
                DatabaseDto.builder().databaseId(ViewModelConstants.DATABASE_ID).build()));
    when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(Optional.empty());
  }

  // --- Request audit of write outcomes (replayed core scenarios) ---

  /**
   * A body that Gson (the request-audit parser) also rejects, not merely Jackson, so the audit's
   * parse-failure path runs: the payload is dropped rather than retained raw, the existing parsing
   * failure metric is counted, and neither the body nor parser details reach the logs.
   */
  @Test
  public void malformedJsonHasSafeRequestAuditAndNoSensitiveWireOrLogs() throws Exception {
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
      mvc.perform(
              withToken(
                  MockMvcRequestBuilders.post(VIEWS_PATH)
                      .contentType(MediaType.APPLICATION_JSON)
                      .content(malformed)
                      .accept(MediaType.APPLICATION_JSON)))
          .andExpect(status().isBadRequest());

      assertNoSensitive(logs.renderedEvents());
      for (String parserDetail : PARSER_DETAILS) {
        assertFalse(logs.renderedEvents().contains(parserDetail), logs.renderedEvents());
      }
      assertTrue(
          logs.renderedEvents().contains(PARSE_FAILURE_DIAGNOSTIC),
          "The parse failure is still reported, as a fixed safe message: " + logs.renderedEvents());
    }
    ServiceAuditEvent event = captureServiceAudit();
    assertNull(event.getRequestPayload(), "An unparseable body is dropped, never audited raw.");
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    assertEquals(
        parseFailuresBefore + 1,
        serviceAuditCount(MetricsConstant.FAILED_PARSING_REQUEST_PAYLOAD),
        "The audit's parse-failure path ran and was counted.");
  }

  /** The successful create's request event keeps the definition and the inbound correlation id. */
  @Test
  public void successfulCreateRequestAuditKeepsTheDefinitionAndSessionId() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.createdResult());
    String sessionId = "view-session-success";

    mvc.perform(
            withToken(
                MockMvcRequestBuilders.post(VIEWS_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestWithSensitiveMarkers())
                    .accept(MediaType.APPLICATION_JSON)
                    .header(SESSION_ID_HEADER, sessionId)))
        .andExpect(status().isCreated());

    ServiceAuditEvent event = captureServiceAudit();
    assertRequestAuditKeepsSuppliedDefinition(event, requestWithSensitiveMarkers());
    assertEquals(sessionId, event.getSessionId());
  }

  @Test
  public void successfulReplaceRequestAuditKeepsTheDefinitionAndRedactsTheCasToken()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(Optional.of(ViewsManagedAuthMatrixBase.viewRow()));
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.replacedResult());

    mvc.perform(
            withToken(
                MockMvcRequestBuilders.put(VIEW_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(ViewModelConstants.fullyPopulatedRequest().toJson())
                    .accept(MediaType.APPLICATION_JSON)))
        .andExpect(status().isOk());

    assertRequestAuditKeepsSuppliedDefinition(
        captureServiceAudit(), ViewModelConstants.fullyPopulatedRequest().toJson());
  }

  @Test
  public void successfulDropHasASafeRequestAudit() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    stubDeletableView();

    mvc.perform(withToken(deleteView())).andExpect(status().isNoContent());

    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
  }

  @Test
  public void accessDeniedMapsToForbiddenThroughViewAdviceWithASafeRequestAudit() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(false);

    assertWriteRequestAuditAndLogs(
        MockMvcRequestBuilders.post(VIEWS_PATH), requestWithSensitiveMarkers(), 403);
  }

  @Test
  public void opaUnavailableWriteSanitizesLogsAndRequestAudit() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenThrow(new RuntimeException(SECRET_SQL + " " + SECRET_SCHEMA + " " + SECRET_BASE));

    assertWriteRequestAuditAndLogs(
        MockMvcRequestBuilders.post(VIEWS_PATH), requestWithSensitiveMarkers(), 503);
  }

  @Test
  public void unknownCommitHasASafeRequestAuditWithTheSessionId() throws Exception {
    RuntimeException raw =
        new RuntimeException(SECRET_SQL + " " + SECRET_SCHEMA + " " + SECRET_BASE);
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenThrow(new CommitStateUnknownException(raw));
    String sessionId = "view-session-unknown";

    assertWriteRequestAuditAndLogs(
        MockMvcRequestBuilders.post(VIEWS_PATH).header(SESSION_ID_HEADER, sessionId),
        requestWithSensitiveMarkers(),
        503);
    assertEquals(sessionId, captureServiceAudit().getSessionId());
  }

  /**
   * The server-owned {@code replace.drop-dialect.allowed} rejection is audited once with its 400
   * and correlation; the request audit keeps the supplied definition, and neither the value nor the
   * engine's message reaches the logs.
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
    String sessionId = "view-session-reserved-property-" + method;
    MockHttpServletRequestBuilder request =
        "POST".equals(method)
            ? MockMvcRequestBuilders.post(VIEWS_PATH)
            : MockMvcRequestBuilders.put(VIEW_PATH);

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      mvc.perform(
              withToken(
                  request
                      .contentType(MediaType.APPLICATION_JSON)
                      .content(body)
                      .accept(MediaType.APPLICATION_JSON)
                      .header(SESSION_ID_HEADER, sessionId)))
          .andExpect(status().isBadRequest());

      String renderedLogs = logs.renderedEvents();
      assertNoSensitive(renderedLogs);
      assertFalse(renderedLogs.contains(DROP_DIALECT_SENTINEL), renderedLogs);
      assertFalse(renderedLogs.contains("owned by OpenHouse"), renderedLogs);
    }
    ServiceAuditEvent event = captureServiceAudit();
    assertEquals(400, event.getStatusCode());
    assertEquals(sessionId, event.getSessionId());
    assertRequestAuditKeepsSuppliedDefinition(event, body);
  }

  /**
   * A syntactically valid body whose representations is an object fails Jackson binding, but the
   * request audit still parses it and keeps it exactly as supplied; its SQL must not reach the
   * logs.
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

    assertWriteRequestAuditAndLogs(MockMvcRequestBuilders.post(VIEWS_PATH), body, 400);
  }

  /**
   * A representations array whose elements are bare or nested SQL strings fails binding, but the
   * request audit still parses the body and keeps it exactly as supplied; that SQL must not reach
   * the logs.
   */
  @Test
  public void primitiveAndNestedArrayRepresentationsAreKeptInRequestAuditButNeverReachWireOrLogs()
      throws Exception {
    String[] malformedRepresentations = {
      "[\"" + SECRET_SQL + "\"]",
      "[[\"" + SECRET_SQL + "\"]]",
      "[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"ok\"},\"" + SECRET_SQL + "\"]"
    };
    for (String representations : malformedRepresentations) {
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

      assertWriteRequestAuditAndLogs(MockMvcRequestBuilders.post(VIEWS_PATH), body, 400);
    }
  }

  // --- Request audit of read outcomes (replayed core scenarios) ---

  @Test
  public void structuralValidationMessageIsPreservedExactlyByViewAdvice() throws Exception {
    String expected = "page : is not supported; use pageToken for continuation";

    mvc.perform(
            withToken(
                MockMvcRequestBuilders.get(VIEWS_PATH + "?page=1")
                    .accept(MediaType.APPLICATION_JSON)))
        .andExpect(status().isBadRequest());

    ServiceAuditEvent event = captureServiceAudit();
    assertEquals(expected, event.getResponseErrorMessage());
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
  }

  /**
   * The raw size may legitimately remain in the audited request URI, so only the audited error text
   * and the logs are checked.
   */
  @Test
  public void malformedAndOverflowingSizeKeepFixedSafe400BeforeService() throws Exception {
    for (String rawSize : new String[] {"abcNotAnIntegerSize", "99999999999"}) {
      Mockito.reset(serviceAuditHandler);
      try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
        mvc.perform(
                withToken(
                    MockMvcRequestBuilders.get(VIEWS_PATH)
                        .param("size", rawSize)
                        .accept(MediaType.APPLICATION_JSON)))
            .andExpect(status().isBadRequest());

        String renderedLogs = logs.renderedEvents();
        assertFalse(renderedLogs.contains(rawSize), renderedLogs);
        for (String parserDetail : BINDING_PARSER_DETAILS) {
          assertFalse(renderedLogs.contains(parserDetail), renderedLogs);
        }
      }
      ServiceAuditEvent event = captureServiceAudit();
      assertEquals(SIZE_BINDING_MESSAGE, event.getResponseErrorMessage());
      AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    }
  }

  @Test
  public void unexpectedRuntimeFailureMapsToSanitized500() throws Exception {
    when(houseTableRepository.findViewById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenThrow(new RuntimeException(SECRET_SQL));

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      mvc.perform(
              withToken(MockMvcRequestBuilders.get(VIEW_PATH).accept(MediaType.APPLICATION_JSON)))
          .andExpect(status().isInternalServerError());

      assertNoSensitive(logs.renderedEvents());
    }
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
  }

  @Test
  public void queryPageTokenFailureRedactsTokenFromRequestAuditWireAndLogs() throws Exception {
    String token = validContinuation(0);
    when(houseTableRepository.findAllViewsByDatabaseId(any(), any()))
        .thenThrow(new RuntimeException(SECRET_PAGE_TOKEN));

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      mvc.perform(
              withToken(
                  MockMvcRequestBuilders.get(
                          VIEWS_PATH + "?pageToken=" + token + "&sortBy=viewId&size=1")
                      .accept(MediaType.APPLICATION_JSON)))
          .andExpect(status().isInternalServerError());

      String renderedLogs = logs.renderedEvents();
      assertNoSensitive(renderedLogs);
      assertFalse(renderedLogs.contains(token), renderedLogs);
    }
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), token);
  }

  @Test
  public void malformedPageTokenRedactsRawQueryStringAndDoesNotReachBackend() throws Exception {
    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      mvc.perform(
              withToken(
                  MockMvcRequestBuilders.get(
                          VIEWS_PATH + "?pageToken=" + SECRET_PAGE_TOKEN + "&sortBy=viewId")
                      .accept(MediaType.APPLICATION_JSON)))
          .andExpect(status().isBadRequest());

      assertNoSensitive(logs.renderedEvents());
    }
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
  }

  /**
   * Servlet binding percent-decodes parameter names, so {@code %70ageToken} binds as {@code
   * pageToken}. The continuation must reach the backend, yet the request audit must not keep it.
   */
  @Test
  public void percentEncodedPageTokenNameIsRedactedFromSuccessfulListAudit() throws Exception {
    String token = validContinuation(3);
    when(houseTableRepository.findAllViewsByDatabaseId(any(), any()))
        .thenAnswer(
            invocation ->
                new PageImpl<HouseTable>(
                    Collections.emptyList(), invocation.<Pageable>getArgument(1), 0));

    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          mvc.perform(
                  withToken(
                      MockMvcRequestBuilders.get(
                              URI.create(
                                  VIEWS_PATH + "?%70ageToken=" + token + "&sortBy=viewId&size=1"))
                          .accept(MediaType.APPLICATION_JSON)))
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
                      withToken(
                          MockMvcRequestBuilders.get(
                                  URI.create(
                                      VIEWS_PATH
                                          + "?pageToken="
                                          + literal
                                          + "&sortBy=viewId&%70ageToken="
                                          + encoded))
                              .accept(MediaType.APPLICATION_JSON))))
              .andExpect(status().isBadRequest())
              .andReturn();

      assertNoSensitive(result.getResponse().getContentAsString(), logs.renderedEvents());
    }
    verify(houseTableRepository, never()).findAllViewsByDatabaseId(any(), any());
    ServiceAuditEvent event = captureServiceAudit();
    AuditEventInspection.assertNoSensitiveProperties(event, SECRETS);
    assertTrue(String.valueOf(event.getUri()).contains("sortBy=viewId"), event.getUri());
  }

  // --- MVC-accepted alias routes ---

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
  }

  /**
   * A structurally invalid definition at an encoded alias is rejected before service entry; the
   * exception-advice request audit still keeps the supplied definition with the CAS token redacted.
   * The reserved source dialect names no supplied representation, so validation rejects it.
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
            .viewProperties(ViewModelConstants.viewPropertiesWithSourceDialect("trino"))
            .defaultCatalog(ViewModelConstants.DEFAULT_CATALOG)
            .defaultNamespace(ViewModelConstants.DEFAULT_NAMESPACE)
            .baseMetadataLocation(SECRET_BASE)
            .build()
            .toJson();

    assertWriteAliasRequestAudit(
        MockMvcRequestBuilders.post(URI.create(DATABASE_ROUTE + "/%76iews")), invalid, 400);

    Mockito.verifyNoInteractions(houseTableRepository, viewCommitEngine, opaHandler);
  }

  // --- Request audit and logs of early, precommit and read-probe failures (core scenarios) ---

  /**
   * Writes rejected before capture (disabled views, missing database, denied drop) have one request
   * event without secrets; the denied drop's request event carries the inbound correlation id.
   */
  @Test
  public void earlyFailedWritesHaveSafeRequestAuditsAndSessionId() throws Exception {
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(false);
    mvc.perform(withToken(postView())).andExpect(status().isNotFound());
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);

    Mockito.reset(serviceAuditHandler);
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases()).thenReturn(Collections.emptyList());
    mvc.perform(withToken(postView())).andExpect(status().isNotFound());
    AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);

    Mockito.reset(serviceAuditHandler);
    when(databasesService.getAllDatabases())
        .thenReturn(
            Collections.singletonList(
                DatabaseDto.builder().databaseId(ViewModelConstants.DATABASE_ID).build()));
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(false);
    String sessionId = "view-session-denied-drop";
    mvc.perform(withToken(deleteView()).header(SESSION_ID_HEADER, sessionId))
        .andExpect(status().isForbidden());
    ServiceAuditEvent deniedDrop = captureServiceAudit();
    AuditEventInspection.assertNoSensitiveProperties(deniedDrop, SECRETS);
    assertEquals(sessionId, deniedDrop.getSessionId());
  }

  /** Typed and generic precommit failures keep their request event and logs free of secrets. */
  @Test
  public void precommitFailuresHaveSafeRequestAuditsAndLogs() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    RuntimeException[] failures = {
      new HouseTableRepositoryStateUnknownException(SECRET_SQL, new RuntimeException(SECRET_BASE)),
      new IllegalStateException(SECRET_SQL, new RuntimeException(SECRET_BASE))
    };
    for (RuntimeException failure : failures) {
      for (boolean delete : new boolean[] {false, true}) {
        Mockito.reset(houseTableRepository, serviceAuditHandler);
        if (delete) {
          when(houseTableRepository.findViewById(ViewsManagedAuthMatrixBase.viewKey()))
              .thenThrow(failure);
        } else {
          when(houseTableRepository.findEntityById(ViewsManagedAuthMatrixBase.viewKey()))
              .thenThrow(failure);
        }

        try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
          mvc.perform(withToken(delete ? deleteView() : postView())).andReturn();

          assertNoSensitive(logs.renderedEvents());
        }
        AuditEventInspection.assertNoSensitiveProperties(captureServiceAudit(), SECRETS);
      }
    }
  }

  /** Read-probe failures (typed and generic, database and gate) keep the logs free of secrets. */
  @Test
  public void readProbeFailuresKeepLogsSafe() throws Exception {
    RuntimeException[] failures = {
      new HouseTableRepositoryStateUnknownException(SECRET_SQL, new RuntimeException(SECRET_BASE)),
      new IllegalStateException(SECRET_SQL, new RuntimeException(SECRET_BASE))
    };
    for (RuntimeException failure : failures) {
      for (boolean gate : new boolean[] {false, true}) {
        Mockito.reset(viewsFeatureGate, databasesService);
        when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
        if (gate) {
          when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenThrow(failure);
        } else {
          when(databasesService.getAllDatabases()).thenThrow(failure);
        }

        try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
          for (String path : new String[] {VIEW_PATH, VIEWS_PATH}) {
            mvc.perform(
                    withToken(MockMvcRequestBuilders.get(path).accept(MediaType.APPLICATION_JSON)))
                .andReturn();
          }
          assertNoSensitive(logs.renderedEvents());
        }
      }
    }
  }

  // --- Request-audit delivery failure: the event it received may echo the definition ---

  @Test
  public void requestAuditSinkFailureAfterAcknowledgedCreateKeepsCreatedAndLogsNoDetails()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.createdResult());

    assertRequestAuditSinkFailureKeepsOutcome(201);

    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
  }

  @Test
  public void requestAuditSinkFailureAfterAFailedResponseKeepsForbiddenAndLogsNoDetails()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(false);

    assertRequestAuditSinkFailureKeepsOutcome(403);

    verify(viewCommitEngine, never()).commit(any());
  }

  /**
   * The request-audit sink receives the full event once and throws an exception whose message,
   * cause and suppressed exception echo that event's JSON. The HTTP outcome and correlation stay as
   * they were, the existing failed-audit metric is counted, the failure is reported with the fixed
   * safe message of its delivery phase (the success advice for a 2xx, the error advice otherwise),
   * and no diagnostic carries the definition, a token, the sink's message, or the raw throwable.
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
    double failedAuditsBefore = serviceAuditCount(MetricsConstant.FAILED_SERVICE_AUDIT);

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
      String phaseDiagnostic =
          expectedStatus < 300
              ? SUCCESS_DELIVERY_FAILURE_DIAGNOSTIC
              : ERROR_DELIVERY_FAILURE_DIAGNOSTIC;
      assertTrue(
          renderedLogs.contains(phaseDiagnostic),
          "The delivery failure is still reported, as a fixed safe message: " + renderedLogs);
    }

    verify(serviceAuditHandler, times(1)).audit(any(ServiceAuditEvent.class));
    assertEquals(
        failedAuditsBefore + 1,
        serviceAuditCount(MetricsConstant.FAILED_SERVICE_AUDIT),
        "The request-audit delivery failure is reported through the existing metric.");
    ServiceAuditEvent event = delivered.get();
    assertEquals(expectedStatus, event.getStatusCode());
    assertEquals(sessionId, event.getSessionId());
    assertRequestAuditKeepsSuppliedDefinition(event, body);
  }

  /** A correlation id from one request must not carry over to the next request's request audit. */
  @Test
  public void sessionIdDoesNotLeakIntoALaterRequestWithoutTheHeader() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(ViewsManagedAuthMatrixBase.createdResult());

    mvc.perform(withToken(postView()).header(SESSION_ID_HEADER, "view-session-first"))
        .andExpect(status().isCreated());
    Mockito.clearInvocations(serviceAuditHandler);
    mvc.perform(withToken(postView())).andExpect(status().isCreated());

    assertNull(captureServiceAudit().getSessionId());
  }

  // --- Helpers ---

  /**
   * Sends a write, then asserts its one request event keeps the supplied definition (CAS redacted)
   * and that the logs carry no secret.
   */
  private void assertWriteRequestAuditAndLogs(
      MockHttpServletRequestBuilder request, String body, int expectedStatus) throws Exception {
    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      MvcResult result =
          mvc.perform(
                  withToken(
                      request
                          .contentType(MediaType.APPLICATION_JSON)
                          .content(body)
                          .accept(MediaType.APPLICATION_JSON)))
              .andReturn();

      assertEquals(expectedStatus, result.getResponse().getStatus());
      assertNoSensitive(logs.renderedEvents());
    }
    ServiceAuditEvent event = captureServiceAudit();
    assertEquals(expectedStatus, event.getStatusCode());
    assertRequestAuditKeepsSuppliedDefinition(event, body);
  }

  /**
   * Performs a list request on a raw route, proves real dispatch to the view list handler, and
   * asserts one request audit with the expected redacted URI and no secret in the event, body, or
   * logs.
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
    return result;
  }

  private static void assertViewListHandler(MvcResult result) {
    assertTrue(result.getHandler() instanceof HandlerMethod, String.valueOf(result.getHandler()));
    HandlerMethod handler = (HandlerMethod) result.getHandler();
    assertEquals(ViewsController.class, handler.getBeanType());
    assertEquals("getAllViews", handler.getMethod().getName());
  }

  private void assertWriteAliasRequestAudit(
      MockHttpServletRequestBuilder request, String body, int expectedStatus) throws Exception {
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

  private double serviceAuditCount(String metric) {
    Counter counter = meterRegistry.find(MetricsConstant.SERVICE_AUDIT + "_" + metric).counter();
    return counter == null ? 0.0 : counter.count();
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

  private void stubDeletableView() {
    when(houseTableRepository.findViewById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(Optional.of(ViewsManagedAuthMatrixBase.viewRow()));
    when(houseTableRepository.deleteViewById(ViewsManagedAuthMatrixBase.viewKey()))
        .thenReturn(true);
  }

  private MockHttpServletRequestBuilder withToken(MockHttpServletRequestBuilder builder) {
    return builder.header("Authorization", "Bearer " + jwtAccessToken);
  }

  private static MockHttpServletRequestBuilder postView() {
    return MockMvcRequestBuilders.post(VIEWS_PATH)
        .contentType(MediaType.APPLICATION_JSON)
        .content(ViewModelConstants.createRequestWithoutBaseVersion().toJson())
        .accept(MediaType.APPLICATION_JSON);
  }

  private static MockHttpServletRequestBuilder deleteView() {
    return MockMvcRequestBuilders.delete(VIEW_PATH).accept(MediaType.APPLICATION_JSON);
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
