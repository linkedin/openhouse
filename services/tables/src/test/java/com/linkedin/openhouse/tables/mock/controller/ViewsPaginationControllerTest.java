package com.linkedin.openhouse.tables.mock.controller;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseViewsApiHandler;
import com.linkedin.openhouse.tables.api.validator.ViewsApiValidator;
import com.linkedin.openhouse.tables.controller.ViewsController;
import com.linkedin.openhouse.tables.dto.mapper.ViewsMapper;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.ViewListResult;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.services.ViewsDisabledService;
import com.linkedin.openhouse.tables.services.ViewsService;
import java.net.URI;
import java.util.Collections;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * End-to-end plumbing for the continuation-token list route: the real {@link ViewsController}, the
 * real {@link OpenHouseViewsApiHandler}, the real validator and the real mapper, with only the
 * {@link ViewsService} replaced by a declarative mock.
 *
 * <p>{@code ViewsControllerTest} cannot prove any of this. The application context registers a
 * {@code @Primary} mock handler that bypasses validation entirely, so a green result there says
 * nothing about whether a token reached the service or a blank one was rejected. Rather than
 * replace that global bean, this class autowires the real collaborators out of the same context and
 * wires test-local handler and controller instances by field injection, which keeps every other
 * view test untouched.
 *
 * <p><b>What the mocked service proves and what it does not.</b> The mock returns fixed, stubbed
 * results per argument set. That demonstrates forwarding, envelope shape and continuation
 * behaviour. It is not a cursor engine: no token is generated, parsed, signed or validated
 * anywhere, no listing is performed, and a successful response here is not evidence that views can
 * be listed. The registered production service still answers 404, which the last test pins.
 */
@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
public class ViewsPaginationControllerTest {

  private static final String VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";

  /** The principal {@link DummyTokenInterceptor} establishes for these requests. */
  private static final String ACTING_PRINCIPAL = "DUMMY_ANONYMOUS_USER";

  private static final int DEFAULT_SIZE = 50;

  /** Fixed, value-free messages the API reports for a service that broke its output contract. */
  private static final String MISSING_RESULT_MESSAGE = "viewsService returned no result";

  private static final String INVALID_RESULTS_MESSAGE =
      "viewsService returned an invalid results list";

  private static final String BLANK_TOKEN_MESSAGE =
      "viewsService returned a blank continuation token";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired private ViewsApiValidator viewsApiValidator;

  @Autowired private ViewsMapper viewsMapper;

  @Autowired private ClusterProperties clusterProperties;

  @Autowired private OpenHouseExceptionHandler openHouseExceptionHandler;

  /** The registered production service: still disabled, and exercised as such below. */
  @Autowired private ViewsDisabledService viewsDisabledService;

  private ViewsService viewsService;

  private MockMvc mvc;

  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws Exception {
    viewsService = Mockito.mock(ViewsService.class);
    mvc = standaloneMvcBackedBy(viewsService);
    jwtAccessToken = new DummyTokenInterceptor.DummySecurityJWT(ACTING_PRINCIPAL).buildNoopJWT();
  }

  /**
   * Builds the real controller/handler/validator/mapper chain over {@code service}. Field injection
   * mirrors how Spring wires these beans; constructing them here keeps the application's own beans
   * and its {@code @Primary} mock handler untouched.
   */
  private MockMvc standaloneMvcBackedBy(ViewsService service) {
    OpenHouseViewsApiHandler handler = new OpenHouseViewsApiHandler();
    ReflectionTestUtils.setField(handler, "viewsApiValidator", viewsApiValidator);
    ReflectionTestUtils.setField(handler, "viewsService", service);
    ReflectionTestUtils.setField(handler, "viewsMapper", viewsMapper);
    ReflectionTestUtils.setField(handler, "clusterProperties", clusterProperties);

    ViewsController controller = new ViewsController();
    ReflectionTestUtils.setField(controller, "viewsApiHandler", handler);

    return MockMvcBuilders.standaloneSetup(controller)
        .setControllerAdvice(openHouseExceptionHandler)
        .addInterceptors(new DummyTokenInterceptor())
        .build();
  }

  private MockHttpServletRequestBuilder listRequest() {
    return authorize(MockMvcRequestBuilders.get(VIEWS_PATH));
  }

  private MockHttpServletRequestBuilder authorize(MockHttpServletRequestBuilder request) {
    return request
        .accept(MediaType.APPLICATION_JSON)
        .header("Authorization", "Bearer " + jwtAccessToken);
  }

  /** Performs a request expected to succeed and returns its parsed body. */
  private JsonNode okBody(MockHttpServletRequestBuilder request) throws Exception {
    String body =
        mvc.perform(request)
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    return MAPPER.readTree(body);
  }

  private static ViewListResult terminalResult() {
    return ViewModelConstants.viewListResult();
  }

  // -------------------------------------------------------------------------------------------
  // Continuation
  // -------------------------------------------------------------------------------------------

  /**
   * Three requests where each one replays the token from the <b>previous response</b>. The middle
   * response is empty yet non-terminal and the last is full yet terminal, so a client that stopped
   * on an empty array, or continued because an array was full, would fail here.
   */
  @Test
  public void aTokenWalkContinuesUntilTheResponseOmitsTheToken() throws Exception {
    String tokenA = "token-A";
    String tokenB = "token-B";
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, null, 2, "viewId", ACTING_PRINCIPAL))
        .thenReturn(
            ViewListResult.builder()
                .results(Collections.singletonList(ViewModelConstants.sparseListDto("my_view")))
                .nextPageToken(tokenA)
                .build());
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, tokenA, 2, "viewId", ACTING_PRINCIPAL))
        .thenReturn(
            ViewListResult.builder()
                .results(Collections.emptyList())
                .nextPageToken(tokenB)
                .build());
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, tokenB, 2, "viewId", ACTING_PRINCIPAL))
        .thenReturn(terminalResult());

    JsonNode first = okBody(listRequest().param("size", "2").param("sortBy", "viewId"));
    Assertions.assertEquals(
        1, first.get("results").size(), "A short page is still a continuing page.");
    Assertions.assertEquals("my_view", first.get("results").get(0).get("viewId").asText());
    Assertions.assertEquals(tokenA, first.get("nextPageToken").asText());

    JsonNode second =
        okBody(
            listRequest()
                .param("pageToken", first.get("nextPageToken").asText())
                .param("size", "2")
                .param("sortBy", "viewId"));
    Assertions.assertEquals(
        0,
        second.get("results").size(),
        "An empty page is legal mid-traversal and must not be turned into a terminal response.");
    Assertions.assertEquals(tokenB, second.get("nextPageToken").asText());

    JsonNode third =
        okBody(
            listRequest()
                .param("pageToken", second.get("nextPageToken").asText())
                .param("size", "2")
                .param("sortBy", "viewId"));
    Assertions.assertEquals(
        2,
        third.get("results").size(),
        "The terminal page is deliberately full, so exhaustion cannot be inferred from the count.");
    Assertions.assertFalse(
        third.has("nextPageToken"),
        "The client stops because the token is absent, not because the array was short.");
    Assertions.assertEquals("my_view", third.get("results").get(0).get("viewId").asText());
    Assertions.assertEquals("my_other_view", third.get("results").get(1).get("viewId").asText());

    InOrder inOrder = Mockito.inOrder(viewsService);
    inOrder
        .verify(viewsService)
        .getAllViews(ViewModelConstants.DATABASE_ID, null, 2, "viewId", ACTING_PRINCIPAL);
    inOrder
        .verify(viewsService)
        .getAllViews(ViewModelConstants.DATABASE_ID, tokenA, 2, "viewId", ACTING_PRINCIPAL);
    inOrder
        .verify(viewsService)
        .getAllViews(ViewModelConstants.DATABASE_ID, tokenB, 2, "viewId", ACTING_PRINCIPAL);
    Mockito.verifyNoMoreInteractions(viewsService);
  }

  @Test
  public void anEmptyTerminalPageIsAnEmptyArrayWithoutAToken() throws Exception {
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, null, 50, null, ACTING_PRINCIPAL))
        .thenReturn(ViewListResult.builder().results(Collections.emptyList()).build());

    mvc.perform(listRequest())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results", Matchers.hasSize(0)))
        .andExpect(jsonPath("$.nextPageToken").doesNotExist());
  }

  /** The outgoing token comes from the service. Echoing the request's token would loop forever. */
  @Test
  public void theReturnedTokenIsTheServicesRatherThanTheRequestsEcho() throws Exception {
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID,
                ViewModelConstants.REQUEST_PAGE_TOKEN,
                50,
                null,
                ACTING_PRINCIPAL))
        .thenReturn(ViewModelConstants.viewListResultWithNextPageToken());

    mvc.perform(listRequest().param("pageToken", ViewModelConstants.REQUEST_PAGE_TOKEN))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nextPageToken", Matchers.is(ViewModelConstants.NEXT_PAGE_TOKEN)))
        .andExpect(
            jsonPath(
                "$.nextPageToken",
                Matchers.not(Matchers.is(ViewModelConstants.REQUEST_PAGE_TOKEN))));
  }

  /**
   * A replayed token arrives percent-encoded. Spring's query decoding is the only decoding step:
   * the application must not decode again, trim, or otherwise normalise the value.
   */
  @Test
  public void aPercentEncodedTokenIsDecodedExactlyOnce() throws Exception {
    String decodedToken = "opaque+/=%";
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, decodedToken, 50, null, ACTING_PRINCIPAL))
        .thenReturn(terminalResult());

    mvc.perform(
            authorize(
                MockMvcRequestBuilders.get(
                    URI.create(VIEWS_PATH + "?pageToken=opaque%2B%2F%3D%25"))))
        .andExpect(status().isOk());

    Mockito.verify(viewsService)
        .getAllViews(ViewModelConstants.DATABASE_ID, decodedToken, 50, null, ACTING_PRINCIPAL);
  }

  /**
   * The API has no token grammar. Anything non-blank is structurally legal and reaches the service
   * unchanged, including text that looks like a page number, a null literal or a URL. Whether such
   * a token means anything is the future engine's problem, not a validation rule.
   */
  @ParameterizedTest(name = "pageToken={0}")
  @ValueSource(strings = {"null", "42", "  padded  ", "a,b", "a:b", "https://example.com/x?y=1"})
  public void arbitraryNonBlankTokensAreForwardedVerbatim(String token) throws Exception {
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, token, 50, null, ACTING_PRINCIPAL))
        .thenReturn(terminalResult());

    mvc.perform(listRequest().param("pageToken", token)).andExpect(status().isOk());

    Mockito.verify(viewsService)
        .getAllViews(ViewModelConstants.DATABASE_ID, token, 50, null, ACTING_PRINCIPAL);
  }

  /**
   * The service's token reaches the client unchanged over the wire too, not merely inside the
   * mapper: padding is kept and reserved characters are not re-encoded, so replaying the value
   * round-trips.
   */
  @Test
  public void aPaddedReservedCharacterTokenIsReturnedVerbatim() throws Exception {
    String outgoingToken = "  a+b/c=%  ";
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, null, 50, null, ACTING_PRINCIPAL))
        .thenReturn(
            ViewListResult.builder()
                .results(ViewModelConstants.sparseListDtos())
                .nextPageToken(outgoingToken)
                .build());

    mvc.perform(listRequest())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nextPageToken", Matchers.is(outgoingToken)));
  }

  /**
   * Sort acceptance is unchanged by the token migration: anything without a comma or colon is
   * structurally legal and reaches the service exactly as sent. Field allowlisting and the meaning
   * of a blank or unknown field stay service concerns, so a new allowlist, a blank-sort rejection
   * or a trimming step would fail here.
   */
  @ParameterizedTest(name = "sortBy={0}")
  @ValueSource(strings = {"", "   ", "unknownField", "  viewId  ", "VIEWID"})
  public void acceptedSortValuesReachTheServiceUnchanged(String sortBy) throws Exception {
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, null, 50, sortBy, ACTING_PRINCIPAL))
        .thenReturn(terminalResult());

    mvc.perform(listRequest().param("sortBy", sortBy)).andExpect(status().isOk());

    Mockito.verify(viewsService)
        .getAllViews(ViewModelConstants.DATABASE_ID, null, 50, sortBy, ACTING_PRINCIPAL);
  }

  // -------------------------------------------------------------------------------------------
  // Per-request count
  // -------------------------------------------------------------------------------------------

  private static Stream<Arguments> countsReachingTheService() {
    return Stream.of(
        Arguments.of("omitted", null, 50),
        Arguments.of("explicitly empty", "", 50),
        Arguments.of("one", "1", 1),
        Arguments.of("no upper cap", String.valueOf(Integer.MAX_VALUE), Integer.MAX_VALUE));
  }

  /**
   * The count is a per-request maximum, not a page index. A very large value is structurally legal
   * and is forwarded as such; the mocked service returns two elements, so nothing is allocated to
   * prove it.
   */
  @ParameterizedTest(name = "size {0} reaches the service as {2}")
  @MethodSource("countsReachingTheService")
  public void eachRequestChoosesItsOwnCount(String name, String sizeParam, int expectedSize)
      throws Exception {
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, null, expectedSize, null, ACTING_PRINCIPAL))
        .thenReturn(terminalResult());

    MockHttpServletRequestBuilder request = listRequest();
    if (sizeParam != null) {
      request = request.param("size", sizeParam);
    }
    mvc.perform(request).andExpect(status().isOk());

    Mockito.verify(viewsService)
        .getAllViews(ViewModelConstants.DATABASE_ID, null, expectedSize, null, ACTING_PRINCIPAL);
  }

  /**
   * A continuation is free to ask for a different count, and omitting the count again means the
   * documented default rather than the previously used value. The token carries no count.
   */
  @Test
  public void aContinuationChoosesItsOwnCountIndependently() throws Exception {
    Mockito.when(
            viewsService.getAllViews(
                Mockito.eq(ViewModelConstants.DATABASE_ID),
                Mockito.eq(ViewModelConstants.REQUEST_PAGE_TOKEN),
                Mockito.anyInt(),
                Mockito.isNull(),
                Mockito.eq(ACTING_PRINCIPAL)))
        .thenReturn(terminalResult());

    mvc.perform(
            listRequest()
                .param("pageToken", ViewModelConstants.REQUEST_PAGE_TOKEN)
                .param("size", "5"))
        .andExpect(status().isOk());
    mvc.perform(listRequest().param("pageToken", ViewModelConstants.REQUEST_PAGE_TOKEN))
        .andExpect(status().isOk());

    Mockito.verify(viewsService)
        .getAllViews(
            ViewModelConstants.DATABASE_ID,
            ViewModelConstants.REQUEST_PAGE_TOKEN,
            5,
            null,
            ACTING_PRINCIPAL);
    Mockito.verify(viewsService)
        .getAllViews(
            ViewModelConstants.DATABASE_ID,
            ViewModelConstants.REQUEST_PAGE_TOKEN,
            DEFAULT_SIZE,
            null,
            ACTING_PRINCIPAL);
  }

  // -------------------------------------------------------------------------------------------
  // Structural rejections, through the real validator
  // -------------------------------------------------------------------------------------------

  private static Stream<Arguments> structurallyInvalidRequests() {
    return Stream.of(
        Arguments.of(
            "whitespace token", "pageToken", "   ", "pageToken : cannot be blank when provided"),
        Arguments.of("empty token", "pageToken", "", "pageToken : cannot be blank when provided"),
        Arguments.of("zero count", "size", "0", "size : must be greater than 0"),
        Arguments.of("negative count", "size", "-1", "size : must be greater than 0"),
        Arguments.of(
            "multiple sort fields",
            "sortBy",
            "viewId,databaseId",
            "sortBy : does not support multiple sort fields or directions"),
        Arguments.of(
            "sort direction",
            "sortBy",
            "viewId:asc",
            "sortBy : does not support multiple sort fields or directions"));
  }

  /**
   * The three view-local structural rules, exercised over HTTP through the real validator. Each
   * message is fixed and echoes nothing the caller sent, because it is copied into the error body
   * and into the service audit event.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("structurallyInvalidRequests")
  public void structurallyInvalidInputsAreRejectedBeforeTheService(
      String name, String parameter, String value, String expectedMessage) throws Exception {
    mvc.perform(listRequest().param(parameter, value))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", Matchers.is(expectedMessage)))
        .andExpect(jsonPath("$.status", Matchers.is("BAD_REQUEST")))
        .andExpect(jsonPath("$.results").doesNotExist());

    Mockito.verifyNoInteractions(viewsService);
  }

  /** Structural failures accumulate so a client sees every problem in one response. */
  @Test
  public void everyStructuralFailureIsReportedTogether() throws Exception {
    mvc.perform(
            listRequest().param("pageToken", " ").param("size", "0").param("sortBy", "viewId:asc"))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(
                    "pageToken : cannot be blank when provided; size : must be greater than 0;"
                        + " sortBy : does not support multiple sort fields or directions")));

    Mockito.verifyNoInteractions(viewsService);
  }

  /**
   * The legacy guard runs before the accumulating validator, so a valueless {@code page} key beats
   * even structurally invalid list inputs and the service is never consulted. This is the same
   * guard {@code ViewsControllerTest} pins against the mock handler, asserted here against the real
   * validator so its precedence over validation is unambiguous.
   */
  @Test
  public void aBareLegacyPageKeyIsRejectedBeforeTheValidator() throws Exception {
    mvc.perform(
            authorize(
                MockMvcRequestBuilders.get(
                    URI.create(VIEWS_PATH + "?page&pageToken=%20%20&size=0"))))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is("page : is no longer supported; use pageToken for continuation")));

    Mockito.verifyNoInteractions(viewsService);
  }

  /**
   * A count that cannot bind to an {@code int} fails in Spring's own conversion, before any view
   * code runs. That path is left exactly as it is rather than reproduced by a handler check.
   */
  @ParameterizedTest(name = "size={0}")
  @ValueSource(strings = {"abc", "2147483648", "1.5"})
  public void unbindableCountsFailAsBindingErrors(String size) throws Exception {
    mvc.perform(listRequest().param("size", size)).andExpect(status().isBadRequest());

    Mockito.verifyNoInteractions(viewsService);
  }

  // -------------------------------------------------------------------------------------------
  // Server-side output contract
  // -------------------------------------------------------------------------------------------

  private static Stream<Arguments> invalidServiceResults() {
    return Stream.of(
        Arguments.of("no result at all", null, MISSING_RESULT_MESSAGE),
        Arguments.of(
            "null results list",
            ViewModelConstants.invalidResultWithNullResults(),
            INVALID_RESULTS_MESSAGE),
        Arguments.of(
            "null element",
            ViewModelConstants.invalidResultWithNullElement(),
            INVALID_RESULTS_MESSAGE),
        Arguments.of(
            "whitespace-only continuation token",
            ViewModelConstants.invalidResultWithBlankNextPageToken(),
            BLANK_TOKEN_MESSAGE),
        Arguments.of(
            "empty continuation token",
            ViewModelConstants.invalidResultWithEmptyNextPageToken(),
            BLANK_TOKEN_MESSAGE));
  }

  /**
   * A service that breaks its output contract is a server defect. It must surface as 500, never as
   * a 400 blamed on the caller and never as a successful empty or terminal page, which would tell a
   * client the traversal had finished when it had not.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidServiceResults")
  public void invalidServiceOutputIsAServerErrorRatherThanATerminalPage(
      String name, ViewListResult result, String expectedMessage) throws Exception {
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, null, 50, null, ACTING_PRINCIPAL))
        .thenReturn(result);

    mvc.perform(listRequest())
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.message", Matchers.is(expectedMessage)))
        .andExpect(jsonPath("$.results").doesNotExist())
        .andExpect(jsonPath("$.nextPageToken").doesNotExist());
  }

  /** A failing service keeps its own status; the API adds no empty-page fallback. */
  @Test
  public void serviceFailuresKeepTheirStatusAndProduceNoResults() throws Exception {
    Mockito.when(
            viewsService.getAllViews(
                ViewModelConstants.DATABASE_ID, null, 50, null, ACTING_PRINCIPAL))
        .thenThrow(new ViewApiException(ViewErrorCode.DATABASE_NOT_FOUND, "Database not found"));

    mvc.perform(listRequest())
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.message", Matchers.is("Database not found")))
        .andExpect(jsonPath("$.results").doesNotExist());
  }

  // -------------------------------------------------------------------------------------------
  // The registered service is still disabled
  // -------------------------------------------------------------------------------------------

  /**
   * The same chain over the real {@link ViewsDisabledService}. Structurally valid first and
   * continuation requests both reach it and both report 404: nothing in this change makes views
   * listable. A structurally invalid request still fails earlier, with 400.
   */
  @Test
  public void theRegisteredServiceStillReportsViewsDisabled() throws Exception {
    MockMvc disabled = standaloneMvcBackedBy(viewsDisabledService);

    disabled
        .perform(listRequest())
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.message", Matchers.is("Views are disabled")));

    disabled
        .perform(listRequest().param("pageToken", ViewModelConstants.REQUEST_PAGE_TOKEN))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.message", Matchers.is("Views are disabled")));

    disabled
        .perform(listRequest().param("pageToken", "   "))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", Matchers.is("pageToken : cannot be blank when provided")));
  }

  /** A token is not a credential: it cannot stand in for an authenticated principal. */
  @Test
  public void aTokenDoesNotBypassAuthentication() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .param("pageToken", ViewModelConstants.REQUEST_PAGE_TOKEN)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isUnauthorized());

    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .param("pageToken", ViewModelConstants.REQUEST_PAGE_TOKEN)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "******"))
        .andExpect(status().isUnauthorized());

    Mockito.verifyNoInteractions(viewsService);
  }
}
