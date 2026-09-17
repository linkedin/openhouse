package com.linkedin.openhouse.tables.mock.controller;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.CachingRequestBodyFilter;
import com.linkedin.openhouse.common.audit.ServiceAuditPayloadRedactor;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.audit.model.ServiceName;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.audit.ViewRequestPayloadRedactor;
import com.linkedin.openhouse.tables.controller.ViewsController;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.exception.ViewRequestValidationFailureException;
import com.linkedin.openhouse.tables.mock.MockViewsApiHandler;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URI;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import org.codehaus.jettison.json.JSONException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * MockMvc coverage of the five /v1 view routes and their service-generated responses, plus
 * annotation checks for the endpoint's published status codes.
 *
 * <p>Error statuses are driven through {@link MockViewsApiHandler}'s database-id switch, so this
 * class exercises controller wiring and the shared exception handler rather than validation. The
 * validator's own rejections are covered by {@code ViewsValidatorTest}; the one rule the controller
 * owns itself, that the path identifiers must match the request body, is covered here.
 *
 * <p><b>No test here asserts an error code in the response JSON.</b> View error codes are internal
 * status selectors: they choose the HTTP status and are never serialized. The assertions are
 * therefore status plus the fixed message, and one explicit assertion that no code field leaked.
 *
 * <p>Paths are written as literal {@code /v1} strings rather than reusing {@code
 * ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX}. Views share the prefix with tables but are a
 * separate resource, so spelling the routes out here keeps this class asserting the exact URIs the
 * controller publishes rather than whatever that constant later becomes.
 */
@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
public class ViewsControllerTest {

  private static final String VIEWS_PATH = "/v1/databases/d200/views";

  /**
   * Write routes have to address the database the request body names: the controller rejects a POST
   * or PUT whose path identifiers do not match the body. The shared fixtures carry {@link
   * ViewModelConstants#DATABASE_ID}, so the write tests below address that database while the read
   * and delete tests keep using the {@code d200} error-signal path, which carries no body at all.
   */
  private static final String WRITE_VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";

  /** The header {@code ServiceAuditAspect} reads the session id from. */
  private static final String SESSION_ID_HEADER = "session-id";

  private static final String SESSION_ID = "fixed-session-id";

  private MockMvc mvc;

  /**
   * A second MockMvc that raises {@link org.springframework.web.servlet.NoHandlerFoundException}
   * for an unmapped path instead of letting the container answer a bare 404. That routes the
   * failure through {@link OpenHouseExceptionHandler#handleNoHandlerFoundException}, which is the
   * behaviour the deployed application has, and it lets the unresolved-route test assert the real
   * "cannot be resolved" response rather than a status that a missing controller would also
   * produce.
   */
  private MockMvc mvcThrowingOnUnmappedPath;

  private String jwtAccessToken;

  @Autowired private ViewsController viewsController;

  @Autowired private OpenHouseExceptionHandler openHouseExceptionHandler;

  @Autowired private ClusterProperties clusterProperties;

  @MockBean private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  /**
   * Spy rather than mock: every other test in this class depends on the real {@link
   * MockViewsApiHandler} behaviour, and a spy leaves it intact while making the arguments the
   * controller passed on observable.
   */
  @SpyBean private MockViewsApiHandler viewsApiHandler;

  /** Spy so a redactor fault can be injected without replacing the real redaction behaviour. */
  @SpyBean private ViewRequestPayloadRedactor viewRequestPayloadRedactor;

  @Captor private ArgumentCaptor<ServiceAuditEvent> argCaptor;

  @BeforeEach
  public void setup() throws IOException, JSONException, ParseException {
    mvc =
        MockMvcBuilders.standaloneSetup(viewsController)
            .setControllerAdvice(openHouseExceptionHandler)
            .addInterceptors(new DummyTokenInterceptor())
            .addFilter(new CachingRequestBodyFilter())
            .build();

    mvcThrowingOnUnmappedPath =
        MockMvcBuilders.standaloneSetup(viewsController)
            .setControllerAdvice(openHouseExceptionHandler)
            .addInterceptors(new DummyTokenInterceptor())
            .addFilter(new CachingRequestBodyFilter())
            .addDispatcherServletCustomizer(
                dispatcherServlet -> dispatcherServlet.setThrowExceptionIfNoHandlerFound(true))
            .build();

    DummyTokenInterceptor.DummySecurityJWT dummySecurityJWT =
        new DummyTokenInterceptor.DummySecurityJWT("DUMMY_ANONYMOUS_USER");
    jwtAccessToken = dummySecurityJWT.buildNoopJWT();
  }

  @Test
  public void getViewReturns200WithPointerBody() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH + "/my_view")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(ViewModelConstants.pointerResponse().toJson()));
  }

  @Test
  public void createViewReturns201WithPointerBody() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post(WRITE_VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(ViewModelConstants.createRequestWithoutBaseVersion().toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(ViewModelConstants.pointerResponse().toJson()));
  }

  /**
   * Binding proof, not routing proof: {@link MockViewsApiHandler} answers 201 whatever the body
   * held, so a green status says nothing about whether the wire key reached the model. The request
   * the controller actually passed on is captured and asserted instead.
   *
   * <p>The body is a literal rather than a serialized fixture so it pins the key a caller sends
   * rather than whatever the fixture happens to emit. Message conversion is per type, not per
   * route, so POST covers the replace route too.
   */
  @Test
  public void createViewBindsTheBaseMetadataLocationKeyOntoTheRequestBody() throws Exception {
    String requestBody =
        "{\"viewId\": \""
            + ViewModelConstants.VIEW_ID
            + "\", \"databaseId\": \""
            + ViewModelConstants.DATABASE_ID
            + "\", \"baseMetadataLocation\": \""
            + ViewModelConstants.METADATA_LOCATION
            + "\"}";

    mvc.perform(
            MockMvcRequestBuilders.post(WRITE_VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isCreated());

    ArgumentCaptor<CreateUpdateViewRequestBody> boundRequest =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    Mockito.verify(viewsApiHandler).createView(boundRequest.capture(), Mockito.any());

    CreateUpdateViewRequestBody captured = boundRequest.getValue();
    Assertions.assertEquals(
        ViewModelConstants.VIEW_ID,
        captured.getViewId(),
        "Precondition: the body binds onto the request model at all.");
    Assertions.assertEquals(
        ViewModelConstants.METADATA_LOCATION,
        captured.getBaseMetadataLocation(),
        "The wire key must populate the request property. The application's converter ignores an"
            + " unknown property, so a stale key binds to null rather than failing the request.");
  }

  @Test
  public void updateViewReplacingExistingViewReturns200() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put(WRITE_VIEWS_PATH + "/my_view")
                .contentType(MediaType.APPLICATION_JSON)
                .content(ViewModelConstants.fullyPopulatedRequest().toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(ViewModelConstants.pointerResponse().toJson()));
  }

  @Test
  public void updateViewCreatingNewViewReturns201() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put(
                    WRITE_VIEWS_PATH + "/" + MockViewsApiHandler.PUT_CREATES_VIEW_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    ViewModelConstants.fullyPopulatedRequest()
                        .toBuilder()
                        .viewId(MockViewsApiHandler.PUT_CREATES_VIEW_ID)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(ViewModelConstants.pointerResponse().toJson()));
  }

  /** Assert the Jackson wire shape; the Gson helper omits nullable item fields. */
  @Test
  public void getAllViewsReturns200WithSparseResultsArray() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.results", Matchers.hasSize(2)))
        .andExpect(jsonPath("$.results[0].viewId", Matchers.is("my_view")))
        .andExpect(jsonPath("$.results[0].databaseId", Matchers.is(ViewModelConstants.DATABASE_ID)))
        .andExpect(jsonPath("$.results[1].viewId", Matchers.is("my_other_view")))
        // Sparse by design: list elements populate identifiers only.
        .andExpect(jsonPath("$.results[0].metadataLocation").doesNotExist())
        // JsonPath treats explicit null as absent, so check the raw body.
        .andExpect(content().string(Matchers.not(Matchers.containsString("nextPageToken"))))
        .andExpect(content().string(Matchers.not(Matchers.containsString("pageResults"))));
  }

  // ---------------------------------------------------------------------------------------------
  // Path and body identifier agreement
  // ---------------------------------------------------------------------------------------------

  /**
   * The write routes carry the identifiers twice, in the path and in the body, and the controller
   * is the only place that can compare them: the handler is handed the body alone. The tests below
   * pin that the comparison happens there, that it is exact, and that a disagreeing request is
   * refused before anything downstream can act on either copy.
   */
  private static String databaseIdMismatch(String pathValue, String bodyValue) {
    return String.format(
        "databaseId : provided %s, doesn't match with the RequestBody %s", pathValue, bodyValue);
  }

  private static String viewIdMismatch(String pathValue, String bodyValue) {
    return String.format(
        "viewId : provided %s, doesn't match with the RequestBody %s", pathValue, bodyValue);
  }

  private void assertWriteRoutesNeverReachedTheHandler() {
    Mockito.verify(viewsApiHandler, Mockito.never()).createView(Mockito.any(), Mockito.any());
    Mockito.verify(viewsApiHandler, Mockito.never()).updateView(Mockito.any(), Mockito.any());
  }

  private MockHttpServletRequestBuilder createRequestTo(
      String path, CreateUpdateViewRequestBody requestBody) {
    return MockMvcRequestBuilders.post(path)
        .contentType(MediaType.APPLICATION_JSON)
        .content(requestBody.toJson())
        .accept(MediaType.APPLICATION_JSON)
        .header("Authorization", "Bearer " + jwtAccessToken);
  }

  private MockHttpServletRequestBuilder replaceRequestTo(
      String path, CreateUpdateViewRequestBody requestBody) {
    return MockMvcRequestBuilders.put(path)
        .contentType(MediaType.APPLICATION_JSON)
        .content(requestBody.toJson())
        .accept(MediaType.APPLICATION_JSON)
        .header("Authorization", "Bearer " + jwtAccessToken);
  }

  @Test
  public void createViewRejectsABodyNamingADifferentDatabase() throws Exception {
    mvc.perform(createRequestTo(VIEWS_PATH, ViewModelConstants.createRequestWithoutBaseVersion()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(databaseIdMismatch("d200", ViewModelConstants.DATABASE_ID))))
        .andExpect(jsonPath("$.status", Matchers.is("BAD_REQUEST")))
        .andExpect(jsonPath("$.errorCode").doesNotExist());

    assertWriteRoutesNeverReachedTheHandler();
  }

  @Test
  public void updateViewRejectsABodyNamingADifferentDatabase() throws Exception {
    mvc.perform(
            replaceRequestTo(
                viewsPath("d200") + "/" + ViewModelConstants.VIEW_ID,
                ViewModelConstants.fullyPopulatedRequest()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(databaseIdMismatch("d200", ViewModelConstants.DATABASE_ID))))
        .andExpect(jsonPath("$.errorCode").doesNotExist());

    assertWriteRoutesNeverReachedTheHandler();
  }

  @Test
  public void updateViewRejectsABodyNamingADifferentView() throws Exception {
    mvc.perform(
            replaceRequestTo(
                WRITE_VIEWS_PATH + "/another_view", ViewModelConstants.fullyPopulatedRequest()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(viewIdMismatch("another_view", ViewModelConstants.VIEW_ID))))
        .andExpect(jsonPath("$.errorCode").doesNotExist());

    assertWriteRoutesNeverReachedTheHandler();
  }

  /** Both disagreements are reported in one response, database first, joined like every other. */
  @Test
  public void updateViewReportsBothIdentifierMismatchesTogether() throws Exception {
    mvc.perform(
            replaceRequestTo(
                viewsPath("d200") + "/another_view", ViewModelConstants.fullyPopulatedRequest()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(
                    databaseIdMismatch("d200", ViewModelConstants.DATABASE_ID)
                        + "; "
                        + viewIdMismatch("another_view", ViewModelConstants.VIEW_ID))))
        .andExpect(jsonPath("$.errorCode").doesNotExist());

    assertWriteRoutesNeverReachedTheHandler();
  }

  /**
   * The comparison is the identifiers as they were sent: identifiers are case sensitive elsewhere
   * in the API, and nothing trims or normalizes them, so a differing case, an empty value or a
   * padded one is a disagreement rather than a match. The padded cases also pin that the value
   * echoed back is the caller's raw string, not a cleaned-up rendering of it.
   */
  @ParameterizedTest(name = "body databaseId=[{0}]")
  @ValueSource(strings = {"My_Database", "MY_DATABASE", "", " my_database", "my_database "})
  public void createViewComparesTheDatabaseIdExactlyAsItWasSent(String bodyDatabaseId)
      throws Exception {
    mvc.perform(
            createRequestTo(
                WRITE_VIEWS_PATH,
                ViewModelConstants.createRequestWithoutBaseVersion()
                    .toBuilder()
                    .databaseId(bodyDatabaseId)
                    .build()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(databaseIdMismatch(ViewModelConstants.DATABASE_ID, bodyDatabaseId))));

    assertWriteRoutesNeverReachedTheHandler();
  }

  /** The replace route compares its database id on the same terms the create route does. */
  @ParameterizedTest(name = "body databaseId=[{0}]")
  @ValueSource(strings = {"My_Database", "MY_DATABASE", "", " my_database", "my_database "})
  public void updateViewComparesTheDatabaseIdExactlyAsItWasSent(String bodyDatabaseId)
      throws Exception {
    mvc.perform(
            replaceRequestTo(
                WRITE_VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID,
                ViewModelConstants.fullyPopulatedRequest()
                    .toBuilder()
                    .databaseId(bodyDatabaseId)
                    .build()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(databaseIdMismatch(ViewModelConstants.DATABASE_ID, bodyDatabaseId))));

    assertWriteRoutesNeverReachedTheHandler();
  }

  @ParameterizedTest(name = "body viewId=[{0}]")
  @ValueSource(strings = {"My_View", "MY_VIEW", "", " my_view", "my_view "})
  public void updateViewComparesTheViewIdExactlyAsItWasSent(String bodyViewId) throws Exception {
    mvc.perform(
            replaceRequestTo(
                WRITE_VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID,
                ViewModelConstants.fullyPopulatedRequest().toBuilder().viewId(bodyViewId).build()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message", Matchers.is(viewIdMismatch(ViewModelConstants.VIEW_ID, bodyViewId))));

    assertWriteRoutesNeverReachedTheHandler();
  }

  /**
   * An omitted identifier is not a disagreement: there is nothing to compare it with. It is a
   * missing required field, which the body validator downstream already reports, so the controller
   * has to let the request through rather than invent a mismatch against a null.
   */
  @Test
  public void createViewWithoutADatabaseIdInTheBodyIsPassedOnRatherThanCalledAMismatch()
      throws Exception {
    mvc.perform(
            createRequestTo(
                WRITE_VIEWS_PATH,
                ViewModelConstants.createRequestWithoutBaseVersion()
                    .toBuilder()
                    .databaseId(null)
                    .build()))
        .andExpect(status().isCreated());

    Mockito.verify(viewsApiHandler).createView(Mockito.any(), Mockito.eq(ACTING_PRINCIPAL));
  }

  @Test
  public void updateViewWithoutIdentifiersInTheBodyIsPassedOnRatherThanCalledAMismatch()
      throws Exception {
    mvc.perform(
            replaceRequestTo(
                WRITE_VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID,
                ViewModelConstants.fullyPopulatedRequest()
                    .toBuilder()
                    .databaseId(null)
                    .viewId(null)
                    .build()))
        .andExpect(status().isOk());

    Mockito.verify(viewsApiHandler).updateView(Mockito.any(), Mockito.eq(ACTING_PRINCIPAL));
  }

  /**
   * The replace route carries two identifiers and each is compared on its own: an omitted one is
   * skipped, and the other is still compared. Skipping both because one was omitted would let a
   * request through that plainly disagrees with its path.
   */
  @Test
  public void updateViewWithoutADatabaseIdInTheBodyStillRejectsADisagreeingViewId()
      throws Exception {
    mvc.perform(
            replaceRequestTo(
                WRITE_VIEWS_PATH + "/another_view",
                ViewModelConstants.fullyPopulatedRequest().toBuilder().databaseId(null).build()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(viewIdMismatch("another_view", ViewModelConstants.VIEW_ID))));

    assertWriteRoutesNeverReachedTheHandler();
  }

  @Test
  public void updateViewWithoutAViewIdInTheBodyStillRejectsADisagreeingDatabaseId()
      throws Exception {
    mvc.perform(
            replaceRequestTo(
                viewsPath("d200") + "/" + ViewModelConstants.VIEW_ID,
                ViewModelConstants.fullyPopulatedRequest().toBuilder().viewId(null).build()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(databaseIdMismatch("d200", ViewModelConstants.DATABASE_ID))));

    assertWriteRoutesNeverReachedTheHandler();
  }

  /**
   * Status and message cannot tell the identifier rule apart from the body rules: the schema and
   * dialect failures answer 400 too, and none of the three codes reaches the wire. The code is
   * therefore read off the exception the controller threw, which is also what pins that the
   * controller reports this as a view validation failure rather than some other 400.
   */
  private static ViewRequestValidationFailureException identifierRejectionOf(MvcResult result) {
    ViewRequestValidationFailureException failure =
        Assertions.assertInstanceOf(
            ViewRequestValidationFailureException.class,
            result.getResolvedException(),
            "An identifier disagreement must be reported as a view request validation failure.");
    Assertions.assertEquals(
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        failure.getErrorCode(),
        "The identifier rule is an invalid view definition, not an unsupported schema or dialect.");
    return failure;
  }

  @Test
  public void bothWriteRoutesReportAnIdentifierMismatchAsAnInvalidViewDefinition()
      throws Exception {
    MvcResult created =
        mvc.perform(
                createRequestTo(VIEWS_PATH, ViewModelConstants.createRequestWithoutBaseVersion()))
            .andExpect(status().isBadRequest())
            .andReturn();
    identifierRejectionOf(created);

    MvcResult replaced =
        mvc.perform(
                replaceRequestTo(
                    viewsPath("d200") + "/another_view",
                    ViewModelConstants.fullyPopulatedRequest()))
            .andExpect(status().isBadRequest())
            .andReturn();
    identifierRejectionOf(replaced);

    assertWriteRoutesNeverReachedTheHandler();
  }

  /**
   * A request that disagrees about its identifiers is refused on that ground alone. Nothing else
   * about the body has been examined at this point, so no other reason may appear beside it.
   */
  @Test
  public void anIdentifierMismatchIsTheOnlyReasonReportedForAnOtherwiseInvalidBody()
      throws Exception {
    mvc.perform(
            createRequestTo(
                VIEWS_PATH,
                ViewModelConstants.createRequestWithoutBaseVersion()
                    .toBuilder()
                    .schema(ViewModelConstants.MALFORMED_SCHEMA_LITERAL)
                    .defaultCatalog("   ")
                    .baseMetadataLocation("not-the-initial-token")
                    .build()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(databaseIdMismatch("d200", ViewModelConstants.DATABASE_ID))))
        .andExpect(jsonPath("$.errorCode").doesNotExist())
        // The rejected body values are the caller's and must not come back in the reason text.
        .andExpect(content().string(Matchers.not(Matchers.containsString("not-the-initial-token"))))
        .andExpect(content().string(Matchers.not(Matchers.containsString("defaultCatalog"))));

    assertWriteRoutesNeverReachedTheHandler();
  }

  /**
   * Agreeing identifiers leave the request untouched: the handler is handed the body the caller
   * sent and the authenticated principal, and nothing derived from the path.
   */
  @Test
  public void matchingIdentifiersForwardTheBodyAndPrincipalUnchanged() throws Exception {
    CreateUpdateViewRequestBody createRequest =
        ViewModelConstants.createRequestWithoutBaseVersion();
    CreateUpdateViewRequestBody replaceRequest = ViewModelConstants.fullyPopulatedRequest();

    mvc.perform(createRequestTo(WRITE_VIEWS_PATH, createRequest)).andExpect(status().isCreated());
    mvc.perform(
            replaceRequestTo(WRITE_VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID, replaceRequest))
        .andExpect(status().isOk());

    ArgumentCaptor<CreateUpdateViewRequestBody> created =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    ArgumentCaptor<CreateUpdateViewRequestBody> replaced =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    Mockito.verify(viewsApiHandler).createView(created.capture(), Mockito.eq(ACTING_PRINCIPAL));
    Mockito.verify(viewsApiHandler).updateView(replaced.capture(), Mockito.eq(ACTING_PRINCIPAL));

    Assertions.assertEquals(
        createRequest,
        created.getValue(),
        "The create handler receives the caller's body verbatim; the controller neither rebuilds it"
            + " nor overwrites its identifiers from the path.");
    Assertions.assertEquals(replaceRequest, replaced.getValue());
  }

  // List parameters

  private static final String UNSUPPORTED_PAGE_REJECTION_MESSAGE =
      "page : is not supported; use pageToken for continuation";

  /** Reserved characters expose accidental token decoding or normalization. */
  private static final String CONTINUATION_TOKEN = "opaque+/=%";

  /** The principal {@link DummyTokenInterceptor} establishes for these requests. */
  private static final String ACTING_PRINCIPAL = "DUMMY_ANONYMOUS_USER";

  private void assertListRouteNeverReachedTheHandler() {
    Mockito.verify(viewsApiHandler, Mockito.never())
        .getAllViews(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any());
  }

  @Test
  public void getAllViewsForwardsTheContinuationTokenCountAndSort() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .param("pageToken", CONTINUATION_TOKEN)
                .param("size", "2")
                .param("sortBy", "viewId")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk());

    Mockito.verify(viewsApiHandler)
        .getAllViews("d200", CONTINUATION_TOKEN, 2, "viewId", ACTING_PRINCIPAL);
  }

  @Test
  public void getAllViewsWithoutQueryParametersForwardsNoTokenAndTheDefaultCount()
      throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk());

    Mockito.verify(viewsApiHandler).getAllViews("d200", null, 50, null, ACTING_PRINCIPAL);
  }

  /** Reject unsupported paging instead of silently ignoring it. */
  @ParameterizedTest(name = "page={0}")
  @ValueSource(strings = {"0", "1", "-1", "abc", ""})
  public void unsupportedPageParameterIsRejectedWith400BeforeTheHandler(String page)
      throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .param("page", page)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", Matchers.is(UNSUPPORTED_PAGE_REJECTION_MESSAGE)))
        .andExpect(jsonPath("$.status", Matchers.is("BAD_REQUEST")))
        .andExpect(jsonPath("$.errorCode").doesNotExist())
        // Rejected values must not be echoed.
        .andExpect(content().string(Matchers.not(Matchers.containsString("provided"))));

    assertListRouteNeverReachedTheHandler();
  }

  /** The unsupported-page guard takes precedence over other invalid list inputs. */
  @Test
  public void unsupportedPageParameterIsReportedAloneWhenOtherListInputsAreAlsoInvalid()
      throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .param("page", "1")
                .param("pageToken", "   ")
                .param("size", "0")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", Matchers.is(UNSUPPORTED_PAGE_REJECTION_MESSAGE)))
        .andExpect(jsonPath("$.errorCode").doesNotExist());

    assertListRouteNeverReachedTheHandler();
  }

  @Test
  public void anUnrelatedUnknownQueryParameterIsStillIgnored() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .param("offset", "10")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk());

    Mockito.verify(viewsApiHandler).getAllViews("d200", null, 50, null, ACTING_PRINCIPAL);
  }

  /** A bare query key can have a null value while remaining present in the parameter map. */
  @Test
  public void aBareUnsupportedPageKeyWithNoValueIsRejected() throws Exception {
    MvcResult result =
        mvc.perform(
                MockMvcRequestBuilders.get(URI.create(VIEWS_PATH + "?page"))
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.message", Matchers.is(UNSUPPORTED_PAGE_REJECTION_MESSAGE)))
            .andReturn();

    MockHttpServletRequest performed = result.getRequest();
    Assertions.assertEquals(
        "page",
        performed.getQueryString(),
        "Precondition: the request really did carry the bare key on the query string.");
    Assertions.assertTrue(
        performed.getParameterMap().containsKey("page"),
        "Precondition: the key is present in the parameter map, which is what the guard reads.");
    Assertions.assertNull(
        performed.getParameter("page"),
        "Precondition: its value is null, so presence is the only usable signal.");

    assertListRouteNeverReachedTheHandler();
  }

  /** The same bare key next to a valid token: the unsupported-parameter error takes precedence. */
  @Test
  public void aBareUnsupportedPageKeyAlongsideATokenIsRejected() throws Exception {
    MvcResult result =
        mvc.perform(
                MockMvcRequestBuilders.get(
                        URI.create(VIEWS_PATH + "?page&pageToken=opaque%2B%2F%3D%25"))
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.message", Matchers.is(UNSUPPORTED_PAGE_REJECTION_MESSAGE)))
            .andReturn();

    MockHttpServletRequest performed = result.getRequest();
    Assertions.assertTrue(
        performed.getQueryString().startsWith("page&"),
        "Precondition: the raw query carries the valueless unsupported key alongside the token.");
    Assertions.assertTrue(performed.getParameterMap().containsKey("page"));
    Assertions.assertEquals(
        CONTINUATION_TOKEN,
        performed.getParameter("pageToken"),
        "Precondition: the token bound too, so this is a mixed request rather than a page-only"
            + " one.");

    assertListRouteNeverReachedTheHandler();
  }

  /** Pin supported OpenAPI parameters without starting a server. */
  @Test
  public void theListRoutePublishesOnlyTokenCountAndSort() {
    Map<String, RequestParam> boundParameters = new LinkedHashMap<>();
    Map<String, Boolean> hiddenByName = new LinkedHashMap<>();
    for (Annotation[] annotations : listRouteMethod().getParameterAnnotations()) {
      RequestParam requestParam = null;
      boolean hidden = false;
      for (Annotation annotation : annotations) {
        if (annotation instanceof RequestParam) {
          requestParam = (RequestParam) annotation;
        } else if (annotation instanceof io.swagger.v3.oas.annotations.Parameter) {
          hidden = ((io.swagger.v3.oas.annotations.Parameter) annotation).hidden();
        }
      }
      if (requestParam != null) {
        String name = requestParam.name().isEmpty() ? requestParam.value() : requestParam.name();
        boundParameters.put(name, requestParam);
        hiddenByName.put(name, hidden);
      }
    }

    Assertions.assertEquals(
        new TreeSet<>(Arrays.asList("pageToken", "size", "sortBy")),
        new TreeSet<>(boundParameters.keySet()),
        "Only pageToken, size, and sortBy are supported query parameters.");

    for (Map.Entry<String, RequestParam> parameter : boundParameters.entrySet()) {
      Assertions.assertFalse(
          parameter.getValue().required(),
          parameter.getKey() + " is optional: a first request supplies none of them.");
      Assertions.assertEquals(
          Boolean.FALSE,
          hiddenByName.get(parameter.getKey()),
          parameter.getKey() + " is part of the published contract and must stay visible.");
    }
    Assertions.assertEquals(
        "50",
        boundParameters.get("size").defaultValue(),
        "An omitted or explicitly empty size keeps the documented default of 50.");

    Class<?>[] parameterTypes = listRouteMethod().getParameterTypes();
    Assertions.assertEquals(
        HttpServletRequest.class,
        parameterTypes[parameterTypes.length - 1],
        "The parameter guard reads the servlet request that Spring already injects; it must not"
            + " reintroduce a bound page parameter of its own.");
    Assertions.assertEquals(
        0,
        listRouteMethod().getParameterAnnotations()[parameterTypes.length - 1].length,
        "The servlet request argument carries no documentation annotation, so springdoc keeps"
            + " ignoring it.");
  }

  private static Method listRouteMethod() {
    return Assertions.assertDoesNotThrow(
        () ->
            ViewsController.class.getMethod(
                "getAllViews",
                String.class,
                String.class,
                int.class,
                String.class,
                HttpServletRequest.class),
        "The list seam takes a database id, an opaque token, a count, a sort field and the servlet"
            + " request the parameter guard inspects.");
  }

  @Test
  public void deleteViewReturns204WithNoBody() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.delete(VIEWS_PATH + "/my_view")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));
  }

  // ---------------------------------------------------------------------------------------------
  // Service audit redaction
  // ---------------------------------------------------------------------------------------------

  /**
   * {@link com.linkedin.openhouse.common.audit.ServiceAuditAspect} records the complete cached
   * request body for every controller call, so the create and replace routes would otherwise write
   * the caller's SQL text and schema document into a service audit event. {@link
   * ViewRequestPayloadRedactor} replaces those values before the event is built; these tests pin
   * that, on the success path and on a failure path, and pin that nothing else in the payload is
   * disturbed.
   *
   * <p>The fixtures carry marker identifiers that appear nowhere else, so the "absent" assertions
   * fail loudly if the redaction is removed rather than passing on a coincidence.
   */
  private static final String SECRET_SQL_MARKER = "secret_sql_marker_column";

  private static final String SECRET_SCHEMA_MARKER = "secret_schema_marker_column";

  private static final String SECRET_SQL =
      "SELECT " + SECRET_SQL_MARKER + " FROM my_database.my_table";

  private static final String SECRET_SCHEMA =
      "{\"type\": \"struct\", \"schema-id\": 0, \"fields\": ["
          + "{\"id\": 1, \"required\": true, \"name\": \""
          + SECRET_SCHEMA_MARKER
          + "\", \"type\": \"string\"}]}";

  private static CreateUpdateViewRequestBody requestCarryingSecretDefinition() {
    return ViewModelConstants.fullyPopulatedRequest()
        .toBuilder()
        .schema(SECRET_SCHEMA)
        .representations(
            Collections.singletonList(
                ViewRepresentation.builder()
                    .type(ViewModelConstants.SQL_REPRESENTATION_TYPE)
                    .sql(SECRET_SQL)
                    .dialect(ViewModelConstants.SOURCE_DIALECT)
                    .build()))
        .build();
  }

  @Test
  public void serviceAuditOnViewCreateRedactsSchemaAndSql() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.post(WRITE_VIEWS_PATH)
            .contentType(MediaType.APPLICATION_JSON)
            .content(requestCarryingSecretDefinition().toJson())
            .accept(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer " + jwtAccessToken));

    ServiceAuditEvent event = capturedAuditEvent();
    Assertions.assertEquals(201, event.getStatusCode(), "Precondition: the create must succeed.");
    assertViewDefinitionRedacted(event);
  }

  @Test
  public void serviceAuditOnViewReplaceRedactsSchemaAndSql() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(WRITE_VIEWS_PATH + "/my_view")
            .contentType(MediaType.APPLICATION_JSON)
            .content(requestCarryingSecretDefinition().toJson())
            .accept(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer " + jwtAccessToken));

    ServiceAuditEvent event = capturedAuditEvent();
    Assertions.assertEquals(200, event.getStatusCode(), "Precondition: the replace must succeed.");
    assertViewDefinitionRedacted(event);
  }

  /**
   * The failure path is the one the reviewer called out: the audit event is emitted from the shared
   * exception handler, after the request body has already been cached, so a rejected request writes
   * its payload just as an accepted one does.
   */
  @Test
  public void serviceAuditOnFailedViewCreateRedactsSchemaAndSql() throws Exception {
    String failingDatabaseId = MockViewsApiHandler.databaseIdFor(ViewErrorCode.VIEWS_DISABLED);

    mvc.perform(
        MockMvcRequestBuilders.post(viewsPath(failingDatabaseId))
            .contentType(MediaType.APPLICATION_JSON)
            // The body has to name the failing database too, or the controller's identifier check
            // would reject the request before the handler could raise the failure under test.
            .content(
                requestCarryingSecretDefinition()
                    .toBuilder()
                    .databaseId(failingDatabaseId)
                    .build()
                    .toJson())
            .accept(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer " + jwtAccessToken));

    ServiceAuditEvent event = capturedAuditEvent();
    Assertions.assertEquals(
        404, event.getStatusCode(), "Precondition: the create must actually be rejected.");
    assertViewDefinitionRedacted(event, failingDatabaseId);
  }

  /**
   * A request the controller itself rejects never reaches the handler, so it is the last place a
   * view definition could still be audited raw. The reason text is server-owned, but the cached
   * payload is the caller's, and it has to be redacted exactly as an accepted request's is.
   */
  @Test
  public void serviceAuditOnAnIdentifierMismatchRedactsSchemaAndSql() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.post(VIEWS_PATH)
            .contentType(MediaType.APPLICATION_JSON)
            .content(requestCarryingSecretDefinition().toJson())
            .accept(MediaType.APPLICATION_JSON)
            .header(SESSION_ID_HEADER, SESSION_ID)
            .header("Authorization", "Bearer " + jwtAccessToken));

    ServiceAuditEvent event = capturedAuditEvent();
    Assertions.assertEquals(
        400, event.getStatusCode(), "Precondition: the mismatched create must be rejected.");
    Assertions.assertEquals(HttpMethod.POST, event.getMethod());
    Assertions.assertEquals(VIEWS_PATH, event.getUri());
    Assertions.assertEquals(ACTING_PRINCIPAL, event.getUser());
    Assertions.assertEquals(SESSION_ID, event.getSessionId());
    assertViewDefinitionRedacted(event);

    assertWriteRoutesNeverReachedTheHandler();
  }

  private ServiceAuditEvent capturedAuditEvent() {
    Mockito.verify(serviceAuditHandler, Mockito.atLeastOnce()).audit(argCaptor.capture());
    return argCaptor.getValue();
  }

  /** A payload that cannot be parsed or redacted is dropped; everything else is still audited. */
  private void assertSingleAuditEventWithoutPayload(HttpMethod method, String uri, int status) {
    Mockito.verify(serviceAuditHandler, Mockito.times(1)).audit(argCaptor.capture());
    ServiceAuditEvent event = argCaptor.getValue();

    Assertions.assertNull(event.getRequestPayload());
    Assertions.assertEquals(status, event.getStatusCode());
    Assertions.assertEquals(method, event.getMethod());
    Assertions.assertEquals(uri, event.getUri());
    Assertions.assertEquals(ServiceName.TABLES_SERVICE, event.getServiceName());
    Assertions.assertEquals(clusterProperties.getClusterName(), event.getClusterName());
    Assertions.assertEquals(ACTING_PRINCIPAL, event.getUser());
    Assertions.assertEquals(SESSION_ID, event.getSessionId());
    Assertions.assertNotNull(
        event.getStartTimestamp(), "CachingRequestBodyFilter supplies the start instant.");
    Assertions.assertNotNull(event.getEndTimestamp());
    Assertions.assertFalse(event.getEndTimestamp().isBefore(event.getStartTimestamp()));
  }

  /** A redactor fault must not leak the payload, alter the response or duplicate the event. */
  @Test
  public void serviceAuditOnAFailingRedactorKeepsTheResponseAndDropsThePayload() throws Exception {
    Mockito.doThrow(new IllegalStateException("redactor failed"))
        .when(viewRequestPayloadRedactor)
        .redact(Mockito.any());

    mvc.perform(
            MockMvcRequestBuilders.post(WRITE_VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestCarryingSecretDefinition().toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header(SESSION_ID_HEADER, SESSION_ID)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isCreated())
        .andExpect(content().json(ViewModelConstants.pointerResponse().toJson()));

    Mockito.verify(viewRequestPayloadRedactor).redact(Mockito.any());
    assertSingleAuditEventWithoutPayload(HttpMethod.POST, WRITE_VIEWS_PATH, 201);
  }

  private void assertViewDefinitionRedacted(ServiceAuditEvent event) {
    assertViewDefinitionRedacted(event, ViewModelConstants.DATABASE_ID);
  }

  private void assertViewDefinitionRedacted(ServiceAuditEvent event, String expectedDatabaseId) {
    JsonElement payload = event.getRequestPayload();
    Assertions.assertNotNull(payload, "The audit event must still carry a request payload.");
    Assertions.assertTrue(payload.isJsonObject(), "The view request payload is a JSON object.");
    JsonObject payloadObject = payload.getAsJsonObject();

    Assertions.assertTrue(
        payloadObject.has("schema"),
        "The key must survive redaction so an auditor can see the field was sent.");
    Assertions.assertEquals(
        ServiceAuditPayloadRedactor.REDACTED_VALUE,
        payloadObject.get("schema").getAsString(),
        "The schema document must not reach the audit event.");

    JsonArray representations = payloadObject.getAsJsonArray("representations");
    Assertions.assertNotNull(representations, "The representations array must survive redaction.");
    Assertions.assertEquals(1, representations.size());
    for (JsonElement representation : representations) {
      JsonObject representationObject = representation.getAsJsonObject();
      Assertions.assertEquals(
          ServiceAuditPayloadRedactor.REDACTED_VALUE,
          representationObject.get("sql").getAsString(),
          "The SQL text must not reach the audit event.");
      // Everything else on the representation is metadata, not caller content.
      Assertions.assertEquals(
          ViewModelConstants.SQL_REPRESENTATION_TYPE,
          representationObject.get("type").getAsString());
      Assertions.assertEquals(
          ViewModelConstants.SOURCE_DIALECT, representationObject.get("dialect").getAsString());
    }

    String serializedPayload = payload.toString();
    Assertions.assertFalse(
        serializedPayload.contains(SECRET_SQL_MARKER),
        "No fragment of the submitted SQL may appear anywhere in the audited payload.");
    Assertions.assertFalse(
        serializedPayload.contains(SECRET_SCHEMA_MARKER),
        "No fragment of the submitted schema may appear anywhere in the audited payload.");

    // The identifying and routing fields are what makes the audit event useful; leave them alone.
    Assertions.assertEquals(ViewModelConstants.VIEW_ID, payloadObject.get("viewId").getAsString());
    Assertions.assertEquals(expectedDatabaseId, payloadObject.get("databaseId").getAsString());
    Assertions.assertEquals(
        clusterProperties.getClusterName(),
        event.getClusterName(),
        "The audited cluster identity comes from this server's configuration, never from the"
            + " request body.");
    Assertions.assertFalse(
        payloadObject.has("clusterId"),
        "A canonical request carries no cluster key, so none reaches the audited payload.");
    Assertions.assertEquals(
        ViewModelConstants.SOURCE_DIALECT, payloadObject.get("sourceDialect").getAsString());
    Assertions.assertEquals(
        ViewModelConstants.DEFAULT_CATALOG, payloadObject.get("defaultCatalog").getAsString());
    Assertions.assertTrue(
        payloadObject.has("baseMetadataLocation"),
        "The audited payload is the raw body the caller sent, so it carries the wire key.");
    Assertions.assertEquals(
        ViewModelConstants.METADATA_LOCATION,
        payloadObject.get("baseMetadataLocation").getAsString());
    Assertions.assertEquals(
        ViewModelConstants.DATABASE_ID,
        payloadObject.getAsJsonArray("defaultNamespace").get(0).getAsString());
    Assertions.assertEquals(
        "openhouse",
        payloadObject.getAsJsonObject("viewProperties").get("owner").getAsString(),
        "View properties are caller metadata, not view definition, and stay auditable.");
  }

  // ---------------------------------------------------------------------------------------------
  // Negative paths
  // ---------------------------------------------------------------------------------------------

  /** Builds a request against one of the five routes for a given database id. */
  @FunctionalInterface
  interface ViewRoute {
    MockHttpServletRequestBuilder request(String databaseId);
  }

  private static String viewsPath(String databaseId) {
    return "/v1/databases/" + databaseId + "/views";
  }

  /** All five routes, so a route cannot quietly skip authentication or exception handling. */
  private static Stream<Arguments> allRoutes() {
    return Stream.of(
        Arguments.of(
            "GET view",
            (ViewRoute)
                databaseId ->
                    MockMvcRequestBuilders.get(viewsPath(databaseId) + "/my_view")
                        .accept(MediaType.APPLICATION_JSON)),
        Arguments.of(
            "GET views",
            (ViewRoute)
                databaseId ->
                    MockMvcRequestBuilders.get(viewsPath(databaseId))
                        .accept(MediaType.APPLICATION_JSON)),
        Arguments.of(
            "POST view",
            (ViewRoute)
                databaseId ->
                    MockMvcRequestBuilders.post(viewsPath(databaseId))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(ViewModelConstants.createRequestWithoutBaseVersion().toJson())
                        .accept(MediaType.APPLICATION_JSON)),
        Arguments.of(
            "PUT view",
            (ViewRoute)
                databaseId ->
                    MockMvcRequestBuilders.put(viewsPath(databaseId) + "/my_view")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(ViewModelConstants.fullyPopulatedRequest().toJson())
                        .accept(MediaType.APPLICATION_JSON)),
        Arguments.of(
            "DELETE view",
            (ViewRoute)
                databaseId ->
                    MockMvcRequestBuilders.delete(viewsPath(databaseId) + "/my_view")
                        .accept(MediaType.APPLICATION_JSON)));
  }

  /**
   * Every internal code selects its declared status and leaves the fixed message untouched. The
   * code itself is deliberately absent from the body: the last assertion is the guard that keeps it
   * that way, because adding a code field would otherwise be an invisible wire change.
   */
  @ParameterizedTest(name = "{0}")
  @EnumSource(ViewErrorCode.class)
  public void everyInternalErrorCodeSelectsItsStatusAndKeepsTheMessageFixed(ViewErrorCode errorCode)
      throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    viewsPath(MockViewsApiHandler.databaseIdFor(errorCode)) + "/my_view")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().is(errorCode.getHttpStatus().value()))
        .andExpect(jsonPath("$.message", Matchers.is(MockViewsApiHandler.VIEW_FAILURE_MESSAGE)))
        .andExpect(jsonPath("$.status", Matchers.is(errorCode.getHttpStatus().name())))
        .andExpect(jsonPath("$.errorCode").doesNotExist());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("allRoutes")
  public void everyRouteRejectsAMissingBearerTokenWith401(String routeName, ViewRoute route)
      throws Exception {
    mvc.perform(route.request("d200")).andExpect(status().isUnauthorized());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("allRoutes")
  public void everyRouteRejectsAMalformedBearerTokenWith401(String routeName, ViewRoute route)
      throws Exception {
    mvc.perform(route.request("d200").header("Authorization", "Bearer not-a-real-jwt"))
        .andExpect(status().isUnauthorized());
  }

  /**
   * Exercises exception mapping: an access denial from the handler becomes HTTP 403 independently
   * of the authorization policy used by the request interceptors.
   */
  @Test
  public void accessDeniedFromTheHandlerIsMappedTo403WithoutPrivilegeEnforcement()
      throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    viewsPath(MockViewsApiHandler.ACCESS_DENIED_DATABASE_ID) + "/my_view")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.message", Matchers.is(MockViewsApiHandler.ACCESS_DENIED_MESSAGE)));
  }

  /** An uncoded infrastructure failure still lands on 503 rather than falling through to 500. */
  @Test
  public void genericInfrastructureFailureIsMappedTo503() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    viewsPath(MockViewsApiHandler.UNAVAILABLE_DATABASE_ID) + "/my_view")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.message", Matchers.is(MockViewsApiHandler.UNAVAILABLE_MESSAGE)))
        .andExpect(jsonPath("$.errorCode").doesNotExist());
  }

  /**
   * Malformed JSON fails during message conversion, before any view code runs, so it must stay on
   * the shared Jackson path and carry no view vocabulary at all. The path names a database the body
   * would have disagreed with had it parsed, which pins that conversion still comes first.
   */
  @Test
  public void malformedJsonBodyIsRejectedByTheSharedHandlerWith400() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post(VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"viewId\": ")
                .accept(MediaType.APPLICATION_JSON)
                .header(SESSION_ID_HEADER, SESSION_ID)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", Matchers.startsWith("Unacceptable JSON")))
        .andExpect(jsonPath("$.errorCode").doesNotExist());

    assertSingleAuditEventWithoutPayload(HttpMethod.POST, VIEWS_PATH, 400);
  }

  /**
   * View routes are mounted under {@code /v1}; the same resource path under {@code /v2} must not
   * reach the view handler.
   */
  @Test
  public void theSamePathUnderV2DoesNotResolve() throws Exception {
    mvcThrowingOnUnmappedPath
        .perform(
            MockMvcRequestBuilders.get("/v2/databases/d200/views/my_view")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(jsonPath("$.message", Matchers.containsString("cannot be resolved by server")))
        // Proves the request never reached the handler, which would have answered 200 with a
        // pointer body.
        .andExpect(jsonPath("$.viewId").doesNotExist());
  }

  // ---------------------------------------------------------------------------------------------
  // Published contract: declared response codes
  // ---------------------------------------------------------------------------------------------

  /**
   * The published status set includes service failures and gateway-originated 502/504 responses.
   */
  private static Stream<Arguments> declaredResponseCodes() {
    return Stream.of(
        Arguments.of(
            "getView", codes("200", "400", "401", "403", "404", "500", "502", "503", "504")),
        Arguments.of(
            "getAllViews", codes("200", "400", "401", "403", "404", "500", "502", "503", "504")),
        Arguments.of(
            "createView",
            codes("201", "400", "401", "403", "404", "409", "422", "500", "502", "503", "504")),
        Arguments.of(
            "updateView",
            codes(
                "200", "201", "400", "401", "403", "404", "409", "422", "500", "502", "503",
                "504")),
        Arguments.of(
            "deleteView", codes("204", "400", "401", "403", "404", "500", "502", "503", "504")));
  }

  private static Set<String> codes(String... responseCodes) {
    return new TreeSet<>(Arrays.asList(responseCodes));
  }

  private static Set<String> declaredResponseCodesOf(String methodName) {
    Method method =
        Arrays.stream(ViewsController.class.getDeclaredMethods())
            .filter(candidate -> candidate.getName().equals(methodName))
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "ViewsController has no method named " + methodName + " any more"));

    ApiResponses apiResponses = method.getAnnotation(ApiResponses.class);
    Assertions.assertNotNull(
        apiResponses, methodName + " must declare its responses for the generated spec");

    return Arrays.stream(apiResponses.value())
        .map(io.swagger.v3.oas.annotations.responses.ApiResponse::responseCode)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  /**
   * Pins the status annotations used to generate the OpenAPI document without starting a server.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("declaredResponseCodes")
  public void eachOperationPublishesExactlyTheStatusesItCanReturn(
      String methodName, Set<String> expectedCodes) {
    Assertions.assertEquals(
        expectedCodes,
        declaredResponseCodesOf(methodName),
        "The published status set for "
            + methodName
            + " must include the documented service and gateway outcomes.");
  }

  /**
   * Ties the internal taxonomy to the published contract: a write route can surface any {@link
   * ViewErrorCode}, so every status those codes map to must be declared on POST and PUT. Adding a
   * code with a new status now fails here instead of silently producing an undeclared response.
   */
  @Test
  public void everyInternalErrorCodeStatusIsPublishedOnTheWriteRoutes() {
    Set<String> codeStatuses =
        Arrays.stream(ViewErrorCode.values())
            .map(errorCode -> String.valueOf(errorCode.getHttpStatus().value()))
            .collect(Collectors.toCollection(TreeSet::new));

    Assertions.assertTrue(
        declaredResponseCodesOf("createView").containsAll(codeStatuses),
        "POST does not publish every status its internal codes can select: " + codeStatuses);
    Assertions.assertTrue(
        declaredResponseCodesOf("updateView").containsAll(codeStatuses),
        "PUT does not publish every status its internal codes can select: " + codeStatuses);
  }
}
