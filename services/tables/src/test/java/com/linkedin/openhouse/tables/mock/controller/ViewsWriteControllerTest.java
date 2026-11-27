package com.linkedin.openhouse.tables.mock.controller;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseViewsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.validator.ViewsApiValidator;
import com.linkedin.openhouse.tables.controller.ViewsController;
import com.linkedin.openhouse.tables.dto.mapper.ViewsMapper;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.exception.ViewRequestValidationFailureException;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.services.ViewsService;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.util.Pair;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * Tests controller and validator boundaries with a mocked service. Local wiring leaves shared
 * Spring test beans untouched.
 */
@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
public class ViewsWriteControllerTest {

  private static final String VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";

  private static final String VIEW_PATH = VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID;

  private static final String OTHER_DATABASE_ID = "another_database";

  private static final String OTHER_VIEW_ID = "another_view";

  /** The principal {@link DummyTokenInterceptor} establishes for these requests. */
  private static final String ACTING_PRINCIPAL = "DUMMY_ANONYMOUS_USER";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired private ViewsApiValidator viewsApiValidator;

  @Autowired private ViewsMapper viewsMapper;

  @Autowired private OpenHouseExceptionHandler openHouseExceptionHandler;

  private ViewsService viewsService;

  private ViewsController controller;

  private MockMvc mvc;

  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws Exception {
    viewsService = Mockito.mock(ViewsService.class);

    OpenHouseViewsApiHandler handler = new OpenHouseViewsApiHandler();
    ReflectionTestUtils.setField(handler, "viewsApiValidator", viewsApiValidator);
    ReflectionTestUtils.setField(handler, "viewsService", viewsService);
    ReflectionTestUtils.setField(handler, "viewsMapper", viewsMapper);

    controller = new ViewsController();
    ReflectionTestUtils.setField(controller, "viewsApiHandler", handler);

    mvc =
        MockMvcBuilders.standaloneSetup(controller)
            .setControllerAdvice(openHouseExceptionHandler)
            .addInterceptors(new DummyTokenInterceptor())
            .build();

    jwtAccessToken = new DummyTokenInterceptor.DummySecurityJWT(ACTING_PRINCIPAL).buildNoopJWT();
  }

  private MockHttpServletRequestBuilder authorize(MockHttpServletRequestBuilder request) {
    return request
        .accept(MediaType.APPLICATION_JSON)
        .header("Authorization", "Bearer " + jwtAccessToken);
  }

  private MockHttpServletRequestBuilder createRequest(
      String path, CreateUpdateViewRequestBody requestBody) {
    return authorize(MockMvcRequestBuilders.post(path))
        .contentType(MediaType.APPLICATION_JSON)
        .content(requestBody.toJson());
  }

  private MockHttpServletRequestBuilder replaceRequest(
      String path, CreateUpdateViewRequestBody requestBody) {
    return authorize(MockMvcRequestBuilders.put(path))
        .contentType(MediaType.APPLICATION_JSON)
        .content(requestBody.toJson());
  }

  private static String databaseIdMismatch(String pathValue, String bodyValue) {
    return String.format(
        "databaseId : provided %s, doesn't match with the RequestBody %s", pathValue, bodyValue);
  }

  private static String viewIdMismatch(String pathValue, String bodyValue) {
    return String.format(
        "viewId : provided %s, doesn't match with the RequestBody %s", pathValue, bodyValue);
  }

  private static String viewsPath(String databaseId) {
    return "/v1/databases/" + databaseId + "/views";
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

  // Identifier agreement, ahead of the service

  @Test
  public void aCreateNamingADifferentDatabaseNeverReachesTheService() throws Exception {
    mvc.perform(
            createRequest(
                viewsPath(OTHER_DATABASE_ID), ViewModelConstants.createRequestWithoutBaseVersion()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(databaseIdMismatch(OTHER_DATABASE_ID, ViewModelConstants.DATABASE_ID))))
        .andExpect(jsonPath("$.status", Matchers.is("BAD_REQUEST")))
        .andExpect(jsonPath("$.errorCode").doesNotExist());

    Mockito.verifyNoInteractions(viewsService);
  }

  @Test
  public void aReplaceNamingADifferentViewNeverReachesTheService() throws Exception {
    mvc.perform(
            replaceRequest(
                VIEWS_PATH + "/" + OTHER_VIEW_ID, ViewModelConstants.fullyPopulatedRequest()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(viewIdMismatch(OTHER_VIEW_ID, ViewModelConstants.VIEW_ID))));

    Mockito.verifyNoInteractions(viewsService);
  }

  @Test
  public void aReplaceDisagreeingAboutBothIdentifiersReportsBothDatabaseFirst() throws Exception {
    mvc.perform(
            replaceRequest(
                viewsPath(OTHER_DATABASE_ID) + "/" + OTHER_VIEW_ID,
                ViewModelConstants.fullyPopulatedRequest()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(
                    databaseIdMismatch(OTHER_DATABASE_ID, ViewModelConstants.DATABASE_ID)
                        + "; "
                        + viewIdMismatch(OTHER_VIEW_ID, ViewModelConstants.VIEW_ID))));

    Mockito.verifyNoInteractions(viewsService);
  }

  /** Identifier mismatches take precedence over structural errors. */
  @Test
  public void anIdentifierMismatchStopsTheRequestBeforeAnyStructuralRuleRuns() throws Exception {
    MvcResult result =
        mvc.perform(
                createRequest(
                    viewsPath(OTHER_DATABASE_ID),
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
                    Matchers.is(
                        databaseIdMismatch(OTHER_DATABASE_ID, ViewModelConstants.DATABASE_ID))))
            .andExpect(jsonPath("$.errorCode").doesNotExist())
            .andReturn();

    identifierRejectionOf(result);

    Mockito.verifyNoInteractions(viewsService);
  }

  @Test
  public void aReplaceIdentifierMismatchStopsTheRequestBeforeAnyStructuralRuleRuns()
      throws Exception {
    MvcResult result =
        mvc.perform(
                replaceRequest(
                    VIEWS_PATH + "/" + OTHER_VIEW_ID,
                    ViewModelConstants.fullyPopulatedRequest()
                        .toBuilder()
                        .schema(ViewModelConstants.MALFORMED_SCHEMA_LITERAL)
                        .defaultCatalog("   ")
                        .build()))
            .andExpect(status().isBadRequest())
            .andExpect(
                jsonPath(
                    "$.message",
                    Matchers.is(viewIdMismatch(OTHER_VIEW_ID, ViewModelConstants.VIEW_ID))))
            .andExpect(jsonPath("$.errorCode").doesNotExist())
            .andReturn();

    identifierRejectionOf(result);

    Mockito.verifyNoInteractions(viewsService);
  }

  /** Error codes are internal and cannot be checked in the response JSON. */
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
  public void agreeingIdentifiersStillReportEveryStructuralFailureTogether() throws Exception {
    mvc.perform(
            createRequest(
                VIEWS_PATH,
                ViewModelConstants.createRequestWithoutBaseVersion()
                    .toBuilder()
                    .defaultCatalog("   ")
                    .baseMetadataLocation("not-the-initial-token")
                    .build()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is(
                    "defaultCatalog : cannot be blank when provided; baseMetadataLocation : must"
                        + " be omitted or INITIAL_VERSION on POST create")))
        .andExpect(jsonPath("$.errorCode").doesNotExist());

    Mockito.verifyNoInteractions(viewsService);
  }

  @Test
  public void aCreateWithoutADatabaseIdIsReportedAsAMissingFieldRatherThanAMismatch()
      throws Exception {
    mvc.perform(
            createRequest(
                VIEWS_PATH,
                ViewModelConstants.createRequestWithoutBaseVersion()
                    .toBuilder()
                    .databaseId(null)
                    .build()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is("CreateUpdateViewRequestBody.databaseId : databaseId cannot be empty")))
        .andExpect(jsonPath("$.message", Matchers.not(Matchers.containsString("doesn't match"))));

    Mockito.verifyNoInteractions(viewsService);
  }

  @Test
  public void aReplaceWithoutAViewIdIsReportedAsAMissingFieldRatherThanAMismatch()
      throws Exception {
    mvc.perform(
            replaceRequest(
                VIEW_PATH,
                ViewModelConstants.fullyPopulatedRequest().toBuilder().viewId(null).build()))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath(
                "$.message",
                Matchers.is("CreateUpdateViewRequestBody.viewId : viewId cannot be empty")))
        .andExpect(jsonPath("$.message", Matchers.not(Matchers.containsString("doesn't match"))));

    Mockito.verifyNoInteractions(viewsService);
  }

  // The body is required by the route itself

  /** Required-body conversion rejects these inputs before controller validation. */
  @ParameterizedTest(name = "body={0}")
  @ValueSource(strings = {"", "null", "{\"viewId\": "})
  public void anUnusableRequestBodyIsRejectedDuringConversion(String body) throws Exception {
    mvc.perform(
            authorize(MockMvcRequestBuilders.post(VIEWS_PATH))
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest());

    mvc.perform(
            authorize(MockMvcRequestBuilders.put(VIEW_PATH))
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest());

    Mockito.verifyNoInteractions(viewsService);
  }

  /** Direct calls can pass null even though HTTP request bodies are required. */
  @Test
  public void aDirectlySuppliedNullBodyIsLeftForTheValidatorRatherThanDereferenced() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> controller.createView(ViewModelConstants.DATABASE_ID, null),
        "A null body must reach the validator, which rejects it, rather than fail the comparison"
            + " with a NullPointerException.");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            controller.updateView(
                ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, null));

    Mockito.verifyNoInteractions(viewsService);
  }

  // Accepted writes

  @Test
  public void anAgreeingCreateForwardsTheCallersBodyToTheService() throws Exception {
    CreateUpdateViewRequestBody request = ViewModelConstants.createRequestWithoutBaseVersion();
    Mockito.when(
            viewsService.putView(Mockito.any(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(true)))
        .thenReturn(Pair.of(serviceOwnedPointerDto(), true));

    mvc.perform(createRequest(VIEWS_PATH, request))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.viewCreator", Matchers.is(ViewModelConstants.VIEW_CREATOR)))
        .andExpect(
            jsonPath("$.lastModifiedTime", Matchers.is(ViewModelConstants.LAST_MODIFIED_TIME)))
        .andExpect(jsonPath("$.creationTime", Matchers.is(ViewModelConstants.CREATION_TIME)));

    ArgumentCaptor<CreateUpdateViewRequestBody> forwarded =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    Mockito.verify(viewsService)
        .putView(forwarded.capture(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(true));
    Assertions.assertEquals(request, forwarded.getValue());
  }

  @Test
  public void anAgreeingReplaceForwardsTheCallersBodyToTheService() throws Exception {
    CreateUpdateViewRequestBody request = ViewModelConstants.fullyPopulatedRequest();
    Mockito.when(
            viewsService.putView(Mockito.any(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(false)))
        .thenReturn(Pair.of(serviceOwnedPointerDto(), false));

    mvc.perform(replaceRequest(VIEW_PATH, request))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.viewCreator", Matchers.is(ViewModelConstants.VIEW_CREATOR)))
        .andExpect(
            jsonPath("$.lastModifiedTime", Matchers.is(ViewModelConstants.LAST_MODIFIED_TIME)));

    ArgumentCaptor<CreateUpdateViewRequestBody> forwarded =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    Mockito.verify(viewsService)
        .putView(forwarded.capture(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(false));
    Assertions.assertEquals(request, forwarded.getValue());
  }

  /** Client metadata must not reach the service or replace service-owned response values. */
  @ParameterizedTest(name = "{0} ignores forged server-owned metadata")
  @ValueSource(strings = {"create", "replace"})
  public void aWriteIgnoresForgedServerOwnedMetadata(String route) throws Exception {
    boolean create = "create".equals(route);
    CreateUpdateViewRequestBody request =
        create
            ? ViewModelConstants.createRequestWithoutBaseVersion()
            : ViewModelConstants.fullyPopulatedRequest();
    Mockito.when(
            viewsService.putView(Mockito.any(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(create)))
        .thenReturn(Pair.of(serviceOwnedPointerDto(), create));

    MockHttpServletRequestBuilder forged =
        authorize(
                create
                    ? MockMvcRequestBuilders.post(VIEWS_PATH)
                    : MockMvcRequestBuilders.put(VIEW_PATH))
            .contentType(MediaType.APPLICATION_JSON)
            .content(bodyWithForgedMetadata(request));

    mvc.perform(forged)
        .andExpect(status().is(create ? 201 : 200))
        .andExpect(jsonPath("$.viewCreator", Matchers.is(ViewModelConstants.VIEW_CREATOR)))
        .andExpect(
            jsonPath("$.lastModifiedTime", Matchers.is(ViewModelConstants.LAST_MODIFIED_TIME)));

    ArgumentCaptor<CreateUpdateViewRequestBody> forwarded =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    Mockito.verify(viewsService)
        .putView(forwarded.capture(), Mockito.eq(ACTING_PRINCIPAL), Mockito.eq(create));
    Assertions.assertEquals(
        request,
        forwarded.getValue(),
        "The forged keys have no request counterpart, so the service sees the caller's body"
            + " unchanged.");
  }

  /** Add fields the typed request cannot represent. */
  private static String bodyWithForgedMetadata(CreateUpdateViewRequestBody request)
      throws Exception {
    ObjectNode body = (ObjectNode) MAPPER.readTree(request.toJson());
    body.put("viewCreator", "mallory");
    body.put("lastModifiedTime", 1L);
    return MAPPER.writeValueAsString(body);
  }
}
