package com.linkedin.openhouse.tables.mock.controller;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.CachingRequestBodyFilter;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.controller.ViewsController;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.mock.MockViewsApiHandler;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.codehaus.jettison.json.JSONException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * MockMvc coverage of the five /v2 view routes: success statuses plus every failure status the
 * routes can report.
 *
 * <p>Error statuses are driven through {@link MockViewsApiHandler}'s database-id switch, so this
 * class exercises controller wiring and the shared exception handler rather than validation. The
 * validator's own rejections are covered by {@code ViewsValidatorTest}.
 *
 * <p><b>No test here asserts an error code in the response JSON.</b> View error codes are internal
 * status selectors: they choose the HTTP status and are never serialized. The assertions are
 * therefore status plus the fixed message, and one explicit assertion that no code field leaked.
 *
 * <p>Paths are written as literal {@code /v2} strings rather than reusing {@code
 * ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX}, which is {@code /v1} and describes the table
 * routes. Hard-coding {@code /v2} here is what pins views to their own major version.
 */
@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
public class ViewsControllerTest {

  private static final String VIEWS_PATH = "/v2/databases/d200/views";

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

  @MockBean private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

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
            MockMvcRequestBuilders.post(VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(ViewModelConstants.createRequestWithoutBaseVersion().toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(ViewModelConstants.pointerResponse().toJson()));
  }

  @Test
  public void updateViewReplacingExistingViewReturns200() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put(VIEWS_PATH + "/my_view")
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
            MockMvcRequestBuilders.put(VIEWS_PATH + "/" + MockViewsApiHandler.PUT_CREATES_VIEW_ID)
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

  /**
   * The list body is asserted with JSON paths rather than a whole-document comparison: the Gson
   * {@code toJson()} helper on {@link
   * com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody} serializes the
   * Spring {@code PageImpl} by its internal fields, whereas the response goes out through Jackson
   * and its getters. Only the Jackson shape is the wire contract, so it is what this asserts.
   */
  @Test
  public void getAllViewsReturns200WithSparsePaginatedBody() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.pageResults.content", Matchers.hasSize(2)))
        .andExpect(jsonPath("$.pageResults.content[0].viewId", Matchers.is("my_view")))
        .andExpect(
            jsonPath(
                "$.pageResults.content[0].databaseId", Matchers.is(ViewModelConstants.DATABASE_ID)))
        .andExpect(jsonPath("$.pageResults.content[1].viewId", Matchers.is("my_other_view")))
        // Sparse by design: list elements populate identifiers only.
        .andExpect(jsonPath("$.pageResults.content[0].metadataLocation").doesNotExist())
        .andExpect(jsonPath("$.pageResults.totalElements", Matchers.is(2)))
        .andExpect(jsonPath("$.pageResults.size", Matchers.is(50)))
        .andExpect(jsonPath("$.pageResults.number", Matchers.is(0)));
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
  // Negative paths
  // ---------------------------------------------------------------------------------------------

  /** Builds a request against one of the five routes for a given database id. */
  @FunctionalInterface
  interface ViewRoute {
    MockHttpServletRequestBuilder request(String databaseId);
  }

  private static String viewsPath(String databaseId) {
    return "/v2/databases/" + databaseId + "/views";
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
   * Exercises exception mapping only. {@code AuthorizationInterceptor.check()} unconditionally
   * returns an allow decision today, so nothing in this PR can deny a request on privilege grounds;
   * what this pins is that when the handler layer eventually does deny one, the shared handler
   * turns it into a 403 rather than a 500.
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
   * the shared Jackson path and carry no view vocabulary at all.
   */
  @Test
  public void malformedJsonBodyIsRejectedByTheSharedHandlerWith400() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post(VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"viewId\": ")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", Matchers.startsWith("Unacceptable JSON")))
        .andExpect(jsonPath("$.errorCode").doesNotExist());
  }

  /**
   * Views are a new resource mounted under {@code /v2}; the same path under {@code /v1} must not
   * resolve. {@link ViewsController} is the only class that could ever map a {@code /v1} view path,
   * so this fails exactly when someone adds one.
   */
  @Test
  public void theSamePathUnderV1DoesNotResolve() throws Exception {
    mvcThrowingOnUnmappedPath
        .perform(
            MockMvcRequestBuilders.get("/v1/databases/d200/views/my_view")
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
   * The status set each operation publishes, taken from the status matrix. {@code
   * client/tableclient} is generated from the OpenAPI document these annotations produce, so a
   * status the routes can return but do not declare is invisible to every generated client.
   */
  private static Stream<Arguments> declaredResponseCodes() {
    return Stream.of(
        Arguments.of("getView", codes("200", "400", "401", "403", "404", "503")),
        Arguments.of("getAllViews", codes("200", "400", "401", "403", "404", "503")),
        Arguments.of("createView", codes("201", "400", "401", "403", "404", "409", "422", "503")),
        Arguments.of(
            "updateView", codes("200", "201", "400", "401", "403", "404", "409", "422", "503")),
        Arguments.of("deleteView", codes("204", "400", "401", "403", "404", "503")));
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
   * Pins the exact published status set per operation.
   *
   * <p>Asserted off the annotations rather than off a generated document on purpose: booting the
   * app to produce the spec needs a free port, and the default spec-generation port is occupied by
   * an unrelated stale service in some environments, which silently yields a views-free document.
   * The annotations are the sole input to that document, so pinning them pins the contract without
   * that failure mode.
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
            + " drifted from the statuses the route can actually return. Every status asserted by"
            + " the error-code, 401, 403 and 503 tests in this class must also be declared here,"
            + " because the generated client is built from these annotations.");
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
