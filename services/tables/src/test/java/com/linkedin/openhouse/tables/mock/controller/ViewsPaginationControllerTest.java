package com.linkedin.openhouse.tables.mock.controller;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseViewsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.validator.ViewsApiValidator;
import com.linkedin.openhouse.tables.controller.ViewsController;
import com.linkedin.openhouse.tables.dto.mapper.ViewsMapper;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.ViewDto;
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
import org.springframework.data.util.Pair;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * Tests token pagination through the real API stack with a mocked service.
 *
 * <p>Local wiring avoids the application's primary mock handler; no token engine is exercised.
 */
@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
public class ViewsPaginationControllerTest {

  private static final String VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";

  /** The principal {@link DummyTokenInterceptor} establishes for these requests. */
  private static final String ACTING_PRINCIPAL = "DUMMY_ANONYMOUS_USER";

  private static final int DEFAULT_SIZE = 50;

  /** Failure messages must not include service output. */
  private static final String MISSING_RESULT_MESSAGE = "viewsService returned no result";

  private static final String INVALID_RESULTS_MESSAGE =
      "viewsService returned an invalid results list";

  private static final String BLANK_TOKEN_MESSAGE =
      "viewsService returned a blank continuation token";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired private ViewsApiValidator viewsApiValidator;

  @Autowired private ViewsMapper viewsMapper;

  @Autowired private OpenHouseExceptionHandler openHouseExceptionHandler;

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

  /** Local instances leave the shared Spring test beans untouched. */
  private MockMvc standaloneMvcBackedBy(ViewsService service) {
    OpenHouseViewsApiHandler handler = new OpenHouseViewsApiHandler();
    ReflectionTestUtils.setField(handler, "viewsApiValidator", viewsApiValidator);
    ReflectionTestUtils.setField(handler, "viewsService", service);
    ReflectionTestUtils.setField(handler, "viewsMapper", viewsMapper);

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

  // Continuation

  /** Replay response tokens across short, empty, and full pages. */
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

  /** The next token comes from the service, not from the request. */
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

  /** Query decoding must happen only once. */
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

  /** Nonblank tokens reach the service without interpretation. */
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

  /** Returned tokens retain padding and reserved characters. */
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

  /** Sort values are forwarded unchanged unless they contain a comma or colon. */
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

  // Per-request count

  private static Stream<Arguments> countsReachingTheService() {
    return Stream.of(
        Arguments.of("omitted", null, 50),
        Arguments.of("explicitly empty", "", 50),
        Arguments.of("one", "1", 1),
        Arguments.of("no upper cap", String.valueOf(Integer.MAX_VALUE), Integer.MAX_VALUE));
  }

  /** Large sizes are forwarded without allocating matching result lists. */
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

  /** Each continuation uses its own size; omission restores the default. */
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

  // Structural rejections, through the real validator

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

  /** Validation errors must not echo caller input. */
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

  /** The page-parameter guard runs before structural validation. */
  @Test
  public void aBareUnsupportedPageKeyIsRejectedBeforeTheValidator() throws Exception {
    mvc.perform(
            authorize(
                MockMvcRequestBuilders.get(
                    URI.create(VIEWS_PATH + "?page&pageToken=%20%20&size=0"))))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is("page : is not supported; use pageToken for continuation")));

    Mockito.verifyNoInteractions(viewsService);
  }

  /** Malformed size values fail during Spring binding. */
  @ParameterizedTest(name = "size={0}")
  @ValueSource(strings = {"abc", "2147483648", "1.5"})
  public void unbindableCountsFailAsBindingErrors(String size) throws Exception {
    mvc.perform(listRequest().param("size", size)).andExpect(status().isBadRequest());

    Mockito.verifyNoInteractions(viewsService);
  }

  // Server-side output contract

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

  /** Invalid service output must not look like terminal success. */
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

  // The registered service is still disabled

  /** Valid read and write requests both reach the disabled service rather than failing earlier. */
  @Test
  public void theRegisteredServiceStillReportsViewsDisabled() throws Exception {
    MockMvc disabled = standaloneMvcBackedBy(viewsDisabledService);

    disabled
        .perform(listRequest())
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.message", Matchers.is("Views are disabled")));

    disabled
        .perform(
            authorize(MockMvcRequestBuilders.post(VIEWS_PATH))
                .contentType(MediaType.APPLICATION_JSON)
                .content(ViewModelConstants.createRequestWithoutBaseVersion().toJson()))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.message", Matchers.is("Views are disabled")));

    disabled
        .perform(
            authorize(MockMvcRequestBuilders.put(VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID))
                .contentType(MediaType.APPLICATION_JSON)
                .content(ViewModelConstants.fullyPopulatedRequest().toJson()))
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

  /** Tokens do not bypass authentication. */
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

  // Cluster identity on the write routes

  // Exercise omitted/forged clusters through real validation; response identity remains
  // service-owned.
  private static Stream<Arguments> requestBodyClusterIds() {
    return Stream.of(
        Arguments.of("omitted", null),
        Arguments.of("forged", "a-cluster-this-server-does-not-serve"));
  }

  @ParameterizedTest(name = "create accepts a {0} clusterId")
  @MethodSource("requestBodyClusterIds")
  public void createIgnoresAnyRequestBodyClusterId(String name, String clusterId) throws Exception {
    Mockito.when(
            viewsService.putView(Mockito.any(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(true)))
        .thenReturn(Pair.of(serviceOwnedPointerDto(), true));

    mvc.perform(
            authorize(MockMvcRequestBuilders.post(VIEWS_PATH))
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    bodyWithClusterId(
                        ViewModelConstants.createRequestWithoutBaseVersion(), clusterId)))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.clusterId", Matchers.is(ViewModelConstants.CLUSTER_ID)));

    Mockito.verify(viewsService)
        .putView(Mockito.any(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(true));
  }

  @ParameterizedTest(name = "replace accepts a {0} clusterId")
  @MethodSource("requestBodyClusterIds")
  public void updateIgnoresAnyRequestBodyClusterId(String name, String clusterId) throws Exception {
    Mockito.when(
            viewsService.putView(Mockito.any(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(false)))
        .thenReturn(Pair.of(serviceOwnedPointerDto(), false));

    mvc.perform(
            authorize(MockMvcRequestBuilders.put(VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID))
                .contentType(MediaType.APPLICATION_JSON)
                .content(bodyWithClusterId(ViewModelConstants.fullyPopulatedRequest(), clusterId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.clusterId", Matchers.is(ViewModelConstants.CLUSTER_ID)));

    Mockito.verify(viewsService)
        .putView(Mockito.any(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(false));
  }

  /** Raw JSON, so the cluster key can be dropped or forged independently of the typed model. */
  private static String bodyWithClusterId(CreateUpdateViewRequestBody request, String clusterId)
      throws Exception {
    ObjectNode body = (ObjectNode) MAPPER.readTree(request.toJson());
    body.remove("clusterId");
    if (clusterId != null) {
      body.put("clusterId", clusterId);
    }
    return MAPPER.writeValueAsString(body);
  }

  private static ViewDto serviceOwnedPointerDto() {
    return ViewDto.builder()
        .viewId(ViewModelConstants.VIEW_ID)
        .databaseId(ViewModelConstants.DATABASE_ID)
        .clusterId(ViewModelConstants.CLUSTER_ID)
        .viewUri(ViewModelConstants.VIEW_URI)
        .metadataLocation(ViewModelConstants.METADATA_LOCATION)
        .viewVersion(ViewModelConstants.VIEW_VERSION)
        .viewCreator(ViewModelConstants.VIEW_CREATOR)
        .creationTime(ViewModelConstants.CREATION_TIME)
        .lastModifiedTime(ViewModelConstants.LAST_MODIFIED_TIME)
        .build();
  }
}
