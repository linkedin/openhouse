package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.tables.authorization.OpaHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalViewRepository;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import com.linkedin.openhouse.tables.repository.impl.PreparedViewOperation;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import org.springframework.web.util.ServletRequestPathUtils;

/**
 * Shared fixture for the four authentication configurations: token interceptor (axis A) on/off
 * crossed with method security (axis B) on/off. Every concrete context below is its own top-level
 * class so JUnit discovers it independently.
 */
abstract class ViewsManagedAuthMatrixBase {

  static final String PRINCIPAL = "matrix-user";
  static final String VIEWS_PATH = "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";
  static final String VIEW_PATH = VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID;

  @Autowired MockMvc mvc;

  @Autowired WebApplicationContext webApplicationContext;

  @Autowired
  @Qualifier("requestMappingHandlerMapping")
  RequestMappingHandlerMapping handlerMapping;

  @MockBean OpaHandler opaHandler;
  @MockBean ViewsFeatureGate viewsFeatureGate;
  @MockBean OpenHouseInternalViewRepository viewRepository;
  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  String jwtAccessToken;

  @BeforeEach
  void setupBase() throws Exception {
    SecurityContextHolder.clearContext();
    jwtAccessToken = new DummyTokenInterceptor.DummySecurityJWT(PRINCIPAL).buildNoopJWT();
    ensureDatabaseExists();
    when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(viewRepository.findById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(listedView());
    when(viewRepository.searchViews(eq(ViewModelConstants.DATABASE_ID), any(Pageable.class)))
        .thenAnswer(
            invocation ->
                new PageImpl<>(
                    Collections.singletonList(listedView()),
                    invocation.<Pageable>getArgument(1),
                    1));
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.observedAbsence());
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.view(viewRow()));
    when(viewRepository.commitCreate(any(), any(), any())).thenReturn(createdOutcome());
    when(viewRepository.commitReplace(any(), any(), any())).thenReturn(replacedOutcome());
  }

  @AfterEach
  void clearAuth() {
    SecurityContextHolder.clearContext();
  }

  /** Inspects the real MVC chain the managed context resolves for a view route. */
  boolean tokenInterceptorInstalled() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest("GET", VIEW_PATH);
    ServletRequestPathUtils.parseAndCache(request);
    HandlerExecutionChain chain = handlerMapping.getHandler(request);
    assertTrue(chain != null, "View route must resolve to a handler");
    return chain.getInterceptorList().stream()
        .anyMatch(interceptor -> interceptor instanceof DummyTokenInterceptor);
  }

  void installAuthenticatedContext() {
    User user = new User(PRINCIPAL, "unused", AuthorityUtils.NO_AUTHORITIES);
    SecurityContextHolder.getContext()
        .setAuthentication(
            new UsernamePasswordAuthenticationToken(user, "unused", user.getAuthorities()));
  }

  /**
   * The scanned {@code MockMvcBuilderConfig} adds a valid default Authorization header to every
   * request of the auto-configured {@link #mvc}. This client is built from the same web context
   * (same handler mappings and interceptors) without that customizer, so a request can carry no
   * Authorization header at all.
   */
  MockMvc mvcWithoutDefaultCredentials() {
    return MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
  }

  /** Overrides the inherited default token with an empty Authorization header. */
  static MockHttpServletRequestBuilder withoutCredentials(MockHttpServletRequestBuilder builder) {
    return builder.header("Authorization", "");
  }

  /**
   * Absent and empty credentials are both rejected by the token interceptor for reads and writes.
   */
  void assertMissingCredentialsAreUnauthorized() throws Exception {
    MockMvc withoutDefault = mvcWithoutDefaultCredentials();
    withoutDefault.perform(getView()).andExpect(status().isUnauthorized());
    withoutDefault.perform(listViews()).andExpect(status().isUnauthorized());
    withoutDefault.perform(postView()).andExpect(status().isUnauthorized());
    mvc.perform(withoutCredentials(getView())).andExpect(status().isUnauthorized());
    mvc.perform(withoutCredentials(listViews())).andExpect(status().isUnauthorized());
    mvc.perform(withoutCredentials(postView())).andExpect(status().isUnauthorized());
  }

  MockHttpServletRequestBuilder withToken(MockHttpServletRequestBuilder builder) {
    return builder.header("Authorization", "Bearer " + jwtAccessToken);
  }

  static MockHttpServletRequestBuilder getView() {
    return MockMvcRequestBuilders.get(VIEW_PATH).accept(MediaType.APPLICATION_JSON);
  }

  static MockHttpServletRequestBuilder listViews() {
    return MockMvcRequestBuilders.get(VIEWS_PATH).accept(MediaType.APPLICATION_JSON);
  }

  static MockHttpServletRequestBuilder postView() {
    return MockMvcRequestBuilders.post(VIEWS_PATH)
        .contentType(MediaType.APPLICATION_JSON)
        .content(ViewModelConstants.createRequestWithoutBaseVersion().toJson())
        .accept(MediaType.APPLICATION_JSON);
  }

  static MockHttpServletRequestBuilder deleteView() {
    return MockMvcRequestBuilders.delete(VIEW_PATH).accept(MediaType.APPLICATION_JSON);
  }

  /** Successful item read and terminal list read, served from the stubbed pointer source. */
  void assertReadsSucceed(MockHttpServletRequestBuilder get, MockHttpServletRequestBuilder list)
      throws Exception {
    mvc.perform(get)
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.viewId").value(ViewModelConstants.VIEW_ID));
    mvc.perform(list)
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results[0].viewId").value(ViewModelConstants.VIEW_ID))
        .andExpect(jsonPath("$.nextPageToken").doesNotExist());
  }

  void assertNoWriteEffects() {
    verify(viewRepository, never()).commitCreate(any(), any(), any());
    verify(viewRepository, never()).commitReplace(any(), any(), any());
    verify(viewRepository, never()).deleteById(any(), any());
  }

  void assertCreateTableDatabaseAuthorization() {
    assertDatabaseAuthorization(Privileges.CREATE_TABLE);
  }

  void assertDatabaseAuthorization(Privileges expectedPrivilege) {
    ArgumentCaptor<String> principal = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<DatabaseDto> database = ArgumentCaptor.forClass(DatabaseDto.class);
    ArgumentCaptor<Privileges> privilege = ArgumentCaptor.forClass(Privileges.class);
    verify(opaHandler)
        .checkAccessDecision(principal.capture(), database.capture(), privilege.capture());
    verify(opaHandler, never()).checkAccessDecision(any(), any(TableDto.class), any());
    assertEquals(PRINCIPAL, principal.getValue());
    assertEquals(ViewModelConstants.DATABASE_ID, database.getValue().getDatabaseId());
    assertEquals(expectedPrivilege, privilege.getValue());
  }

  /** Reads and rejected requests must not consult OPA through any overload. */
  void assertNoOpaInteractions() {
    verifyNoInteractions(opaHandler);
  }

  static ViewDto listedView() {
    return ViewDto.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .viewId(ViewModelConstants.VIEW_ID)
        .build();
  }

  static ViewDto createdView() {
    return ViewDto.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .viewId(ViewModelConstants.VIEW_ID)
        .metadataLocation(ViewModelConstants.METADATA_LOCATION)
        .viewVersion(ViewModelConstants.METADATA_LOCATION)
        .build();
  }

  static ViewDto replacedView() {
    return createdView()
        .toBuilder()
        .metadataLocation("file:/warehouse/replaced.metadata.json")
        .viewVersion("file:/warehouse/replaced.metadata.json")
        .build();
  }

  static final String CREATED_VIEW_UUID = "committed-create-uuid";

  static ViewCommitOutcome createdOutcome() {
    return ViewCommitOutcome.builder()
        .dto(createdView())
        .committedViewUuid(CREATED_VIEW_UUID)
        .created(true)
        .build();
  }

  static ViewCommitOutcome replacedOutcome() {
    return ViewCommitOutcome.builder()
        .dto(replacedView())
        .committedViewUuid("view-uuid")
        .created(false)
        .build();
  }

  static HouseTable viewRow() {
    return HouseTable.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .tableId(ViewModelConstants.VIEW_ID)
        .tableUUID("view-uuid")
        .tableLocation(ViewModelConstants.METADATA_LOCATION)
        .storageType("local")
        .entityType("VIEW")
        .build();
  }

  void ensureDatabaseExists() {
    TableDto anchor =
        TableModelConstants.TABLE_DTO
            .toBuilder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .tableId("views_auth_matrix_anchor")
            .tableVersion(ValidatorConstants.INITIAL_TABLE_VERSION)
            .build();
    TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder()
            .databaseId(anchor.getDatabaseId())
            .tableId(anchor.getTableId())
            .build();
    if (!openHouseInternalRepository.existsById(key)) {
      openHouseInternalRepository.save(anchor);
    }
  }
}

/** A1B1: token interceptor configured and method security enabled. */
@SpringBootTest(classes = SpringH2Application.class)
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@TestPropertySource(
    properties = {
      "cluster.security.token.interceptor.classname=com.linkedin.openhouse.common.security.DummyTokenInterceptor",
      "cluster.security.tables.authorization.enabled=true",
      "cluster.security.tables.authorization.opa.base-uri=http://opa.test"
    })
class ViewsManagedAuthMatrixTokenAndMethodSecurityTest extends ViewsManagedAuthMatrixBase {

  @Test
  void missingTokenIsRejectedWith401ForReadAndWriteBeforeOpaOrService() throws Exception {
    assertTrue(tokenInterceptorInstalled(), "A1 context must install the token interceptor");

    assertMissingCredentialsAreUnauthorized();

    assertNoOpaInteractions();
    verify(viewRepository, never()).prepareWrite(any(), any());
    assertNoWriteEffects();
  }

  @Test
  void authenticatedGetAndListSkipOpaAndAuthenticatedWriteUsesDatabasePrivilege() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);

    assertReadsSucceed(withToken(getView()), withToken(listViews()));
    assertNoOpaInteractions();

    mvc.perform(withToken(postView())).andExpect(status().isCreated());
    assertCreateTableDatabaseAuthorization();
  }

  @Test
  void deniedWritesAreForbiddenWithoutMutation() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(false);

    mvc.perform(withToken(postView())).andExpect(status().isForbidden());
    assertCreateTableDatabaseAuthorization();
    clearInvocations(opaHandler);

    mvc.perform(withToken(deleteView())).andExpect(status().isForbidden());
    assertDatabaseAuthorization(Privileges.DELETE_TABLE);
    verify(viewRepository, never()).prepareDelete(any(), any());
    assertNoWriteEffects();
  }

  @Test
  void authenticatedWritesUseMappedDatabasePrivilegesForPutCreateReplaceAndDelete()
      throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);

    mvc.perform(
            withToken(
                MockMvcRequestBuilders.put(VIEW_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(ViewModelConstants.createRequestWithInitialBaseVersion().toJson())
                    .accept(MediaType.APPLICATION_JSON)))
        .andExpect(status().isCreated());
    assertDatabaseAuthorization(Privileges.CREATE_TABLE);
    clearInvocations(opaHandler);

    HouseTable captured = viewRow();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.view(captured));
    mvc.perform(
            withToken(
                MockMvcRequestBuilders.put(VIEW_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(ViewModelConstants.fullyPopulatedRequest().toJson())
                    .accept(MediaType.APPLICATION_JSON)))
        .andExpect(status().isOk());
    assertDatabaseAuthorization(Privileges.UPDATE_TABLE_METADATA);
    ArgumentCaptor<PreparedViewOperation> replaced =
        ArgumentCaptor.forClass(PreparedViewOperation.class);
    verify(viewRepository).commitReplace(any(), replaced.capture(), eq(PRINCIPAL));
    assertSame(captured, replaced.getValue().getViewBaseRow().get());
    clearInvocations(opaHandler);

    mvc.perform(withToken(deleteView())).andExpect(status().isNoContent());
    assertDatabaseAuthorization(Privileges.DELETE_TABLE);
    verify(viewRepository).deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
  }
}

/** A1B0: token interceptor configured, method security disabled. */
@SpringBootTest(classes = SpringH2Application.class)
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@TestPropertySource(
    properties = {
      "cluster.security.token.interceptor.classname=com.linkedin.openhouse.common.security.DummyTokenInterceptor",
      "cluster.security.tables.authorization.enabled=false",
      "cluster.security.tables.authorization.opa.base-uri=http://opa.test"
    })
class ViewsManagedAuthMatrixTokenOnlyTest extends ViewsManagedAuthMatrixBase {

  @Test
  void tokenInterceptorRejectsMissingAndInvalidCredentialsWhenMethodSecurityIsDisabled()
      throws Exception {
    assertTrue(tokenInterceptorInstalled(), "A1 context must install the token interceptor");

    assertMissingCredentialsAreUnauthorized();
    mvc.perform(getView().header("Authorization", "Bearer not-a-jwt"))
        .andExpect(status().isUnauthorized());
    mvc.perform(postView().header("Authorization", "Bearer not-a-jwt"))
        .andExpect(status().isUnauthorized());

    assertNoOpaInteractions();
    verify(viewRepository, never()).prepareWrite(any(), any());
    assertNoWriteEffects();
  }

  @Test
  void tokenOnlyAuthenticatedGetAndListReachServiceWithoutOpa() throws Exception {
    assertReadsSucceed(withToken(getView()), withToken(listViews()));
    assertNoOpaInteractions();
  }

  @Test
  void tokenOnlyAuthenticatedWriteStillUsesDatabasePolicy() throws Exception {
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);

    mvc.perform(withToken(postView())).andExpect(status().isCreated());

    assertCreateTableDatabaseAuthorization();
  }
}

/** A0B1: no token interceptor, method security enabled. */
@SpringBootTest(classes = SpringH2Application.class)
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@TestPropertySource(
    properties = {
      "cluster.security.token.interceptor.classname=",
      "cluster.security.tables.authorization.enabled=true",
      "cluster.security.tables.authorization.opa.base-uri=http://opa.test"
    })
class ViewsManagedAuthMatrixMethodSecurityOnlyTest extends ViewsManagedAuthMatrixBase {

  @Test
  void unauthenticatedReadAndWriteAreForbiddenWithoutOpaOrMutation() throws Exception {
    assertFalse(tokenInterceptorInstalled(), "A0 context must not install a token interceptor");

    mvc.perform(withoutCredentials(getView())).andExpect(status().isForbidden());
    mvc.perform(withoutCredentials(listViews())).andExpect(status().isForbidden());
    mvc.perform(withoutCredentials(postView())).andExpect(status().isForbidden());

    assertNoOpaInteractions();
    verify(viewRepository, never()).prepareWrite(any(), any());
    assertNoWriteEffects();
  }

  @Test
  void authenticatedMethodSecurityOnlyReadReachesServiceWithoutOpa() throws Exception {
    installAuthenticatedContext();

    assertReadsSucceed(getView(), listViews());
    assertNoOpaInteractions();
  }

  @Test
  void authenticatedMethodSecurityOnlyWriteUsesDatabasePolicy() throws Exception {
    installAuthenticatedContext();
    when(opaHandler.checkAccessDecision(any(), any(DatabaseDto.class), any())).thenReturn(true);

    mvc.perform(postView()).andExpect(status().isCreated());

    assertCreateTableDatabaseAuthorization();
  }
}

/** A0B0: development mode, neither axis configured. */
@SpringBootTest(classes = SpringH2Application.class)
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@TestPropertySource(
    properties = {
      "cluster.security.token.interceptor.classname=",
      "cluster.security.tables.authorization.enabled=false"
    })
class ViewsManagedAuthMatrixBothOffTest extends ViewsManagedAuthMatrixBase {

  @Test
  void developmentModeHasNoAuthenticationGateForReads() throws Exception {
    assertFalse(tokenInterceptorInstalled(), "A0 context must not install a token interceptor");

    assertReadsSucceed(withoutCredentials(getView()), withoutCredentials(listViews()));
    assertNoOpaInteractions();
  }

  @Test
  void developmentModeWriteReachesServiceWithoutOpaWhenOpaIsNotConfigured() throws Exception {
    mvc.perform(withoutCredentials(postView())).andExpect(status().isCreated());

    assertNoOpaInteractions();
    verify(viewRepository).commitCreate(any(), any(), any());
  }
}
