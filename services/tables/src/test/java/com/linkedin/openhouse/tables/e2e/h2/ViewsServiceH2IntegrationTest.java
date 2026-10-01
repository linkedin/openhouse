package com.linkedin.openhouse.tables.e2e.h2;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.services.ViewAdmissionService;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@SpringBootTest(classes = SpringH2Application.class)
@AutoConfigureMockMvc
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ViewsServiceH2IntegrationTest {

  private static final String VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";
  private static final String VIEW_PATH = VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID;

  @Autowired private MockMvc mvc;
  @Autowired private WebApplicationContext webApplicationContext;
  @Autowired private ClusterProperties clusterProperties;
  @Autowired private StorageManager storageManager;
  @Autowired private OpenHouseInternalRepository openHouseInternalRepository;
  @Autowired private HouseTableRepository houseTableRepository;
  @MockBean private ViewsFeatureGate viewsFeatureGate;
  @MockBean private AuditHandler<ViewAuditEvent> viewAuditHandler;
  // Mock admission is a no-op pass-through unless a test scripts a rejection.
  @MockBean private ViewAdmissionService admissionService;
  @SpyBean private StorageSelector storageSelector;

  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws Exception {
    jwtAccessToken =
        new DummyTokenInterceptor.DummySecurityJWT("DUMMY_ANONYMOUS_USER").buildNoopJWT();
    org.mockito.Mockito.when(viewsFeatureGate.isEnabled(ViewModelConstants.DATABASE_ID))
        .thenReturn(true);
    ensureDatabaseExists();
  }

  @Test
  public void enabledViewRoutesCreateReadReplaceListAndDeleteUsingServerOwnedState()
      throws Exception {
    MvcResult create =
        mvc.perform(
                MockMvcRequestBuilders.post(VIEWS_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(ViewModelConstants.createRequestWithoutBaseVersion().toJson())
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.clusterId").value(clusterProperties.getClusterName()))
            .andExpect(
                jsonPath("$.metadataLocation").value(containsString(ViewModelConstants.VIEW_ID)))
            .andExpect(jsonPath("$.metadataLocation").value(containsString("metadata")))
            .andExpect(
                jsonPath("$.metadataLocation")
                    .value(
                        containsString(
                            storageManager.getDefaultStorage().getClient().getRootPrefix())))
            .andReturn();
    String baseMetadataLocation =
        com.jayway.jsonpath.JsonPath.read(
            create.getResponse().getContentAsString(), "$.metadataLocation");
    HouseTable createdRow = findViewRow();
    org.junit.jupiter.api.Assertions.assertEquals("local", createdRow.getStorageType());
    org.junit.jupiter.api.Assertions.assertTrue(
        createdRow.getTableLocation().contains(createdRow.getTableUUID()));

    mvc.perform(
            MockMvcRequestBuilders.get(VIEW_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.schema").doesNotExist())
        .andExpect(jsonPath("$.representations").doesNotExist())
        .andExpect(jsonPath("$.metadataLocation").value(baseMetadataLocation));

    MvcResult replace =
        mvc.perform(
                MockMvcRequestBuilders.put(VIEW_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        ViewModelConstants.fullyPopulatedRequest()
                            .toBuilder()
                            .baseMetadataLocation(baseMetadataLocation)
                            .representations(
                                java.util.Collections.singletonList(
                                    com.linkedin.openhouse.tables.api.spec.v0.request.components
                                        .ViewRepresentation.builder()
                                        .type("sql")
                                        .dialect(ViewModelConstants.SOURCE_DIALECT)
                                        .sql("SELECT id FROM my_database.my_table")
                                        .build()))
                            .build()
                            .toJson())
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.clusterId").value(clusterProperties.getClusterName()))
            .andReturn();
    String replacementMetadataLocation =
        com.jayway.jsonpath.JsonPath.read(
            replace.getResponse().getContentAsString(), "$.metadataLocation");
    org.junit.jupiter.api.Assertions.assertNotEquals(
        baseMetadataLocation,
        replacementMetadataLocation,
        "Changed replacement must publish a new metadata pointer.");
    HouseTable replacedRow = findViewRow();
    org.junit.jupiter.api.Assertions.assertEquals(
        createdRow.getTableUUID(), replacedRow.getTableUUID());
    org.junit.jupiter.api.Assertions.assertEquals(
        createdRow.getStorageType(), replacedRow.getStorageType());
    org.junit.jupiter.api.Assertions.assertTrue(
        replacedRow.getTableLocation().contains(createdRow.getTableUUID()));

    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .param("size", "1")
                .param("sortBy", "viewId")
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results[0].viewId").value(ViewModelConstants.VIEW_ID))
        .andExpect(jsonPath("$.results[0].metadataLocation").doesNotExist());

    mvc.perform(
            MockMvcRequestBuilders.delete(VIEW_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNoContent());

    ArgumentCaptor<ViewAuditEvent> events = ArgumentCaptor.forClass(ViewAuditEvent.class);
    Mockito.verify(viewAuditHandler, Mockito.times(3)).audit(events.capture());
    List<ViewAuditEvent> audited = events.getAllValues();
    String viewUuid = createdRow.getTableUUID();
    assertOperationAudit(audited.get(0), viewUuid, null, baseMetadataLocation);
    assertOperationAudit(
        audited.get(1), viewUuid, baseMetadataLocation, replacementMetadataLocation);
    assertOperationAudit(audited.get(2), viewUuid, replacementMetadataLocation, null);
  }

  @Test
  public void admissionRejectionAllocatesNothingAndWritesNoMetadataForCreateOrReplace()
      throws Exception {
    String existingViewId = "admission_existing_view";
    String rejectedViewId = "admission_rejected_view";
    createView(existingViewId);
    try {
      HouseTable existingBefore = findViewRow(existingViewId);
      long filesBefore = metadataFileCount();
      Mockito.doThrow(
              new ViewApiException(ViewErrorCode.VIEW_ADMISSION_FAILED, "View admission rejected"))
          .when(admissionService)
          .admit(ArgumentMatchers.any());
      // The successful seed above already passed admission and storage selection; count only the
      // two rejected requests below. Stubbing is retained.
      Mockito.clearInvocations(admissionService, storageSelector);

      mvc.perform(
              MockMvcRequestBuilders.post(VIEWS_PATH)
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(
                      ViewModelConstants.createRequestWithoutBaseVersion()
                          .toBuilder()
                          .viewId(rejectedViewId)
                          .build()
                          .toJson())
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().isUnprocessableEntity());
      mvc.perform(
              MockMvcRequestBuilders.put(VIEWS_PATH + "/" + existingViewId)
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(
                      ViewModelConstants.fullyPopulatedRequest()
                          .toBuilder()
                          .viewId(existingViewId)
                          .baseMetadataLocation(existingBefore.getTableLocation())
                          .representations(
                              java.util.Collections.singletonList(
                                  com.linkedin.openhouse.tables.api.spec.v0.request.components
                                      .ViewRepresentation.builder()
                                      .type("sql")
                                      .dialect(ViewModelConstants.SOURCE_DIALECT)
                                      .sql("SELECT id FROM my_database.my_table")
                                      .build()))
                          .build()
                          .toJson())
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().isUnprocessableEntity());

      Mockito.verify(admissionService, Mockito.times(2)).admit(ArgumentMatchers.any());
      Mockito.verify(storageSelector, Mockito.never())
          .selectStorage(ArgumentMatchers.any(), ArgumentMatchers.any());
      org.junit.jupiter.api.Assertions.assertEquals(filesBefore, metadataFileCount());
      org.junit.jupiter.api.Assertions.assertFalse(
          houseTableRepository
              .findEntityById(
                  HouseTablePrimaryKey.builder()
                      .databaseId(ViewModelConstants.DATABASE_ID)
                      .tableId(rejectedViewId)
                      .build())
              .isPresent());
      HouseTable existingAfter = findViewRow(existingViewId);
      org.junit.jupiter.api.Assertions.assertEquals(
          existingBefore.getTableLocation(), existingAfter.getTableLocation());
      org.junit.jupiter.api.Assertions.assertEquals(
          existingBefore.getTableUUID(), existingAfter.getTableUUID());
    } finally {
      // This class shares one context and my_database across methods; remove this case's rows so
      // sorted listings in other methods are unaffected regardless of execution order.
      deleteViewIfPresent(existingViewId);
      deleteViewIfPresent(rejectedViewId);
    }
  }

  /**
   * HTS database identity is case-insensitive, so a mixed-case alias of the seeded {@code
   * my_database} route database must be recognized as existing.
   */
  @Test
  public void mixedCaseRouteAliasOfASeededDatabaseIsRecognizedAsExisting() throws Exception {
    String routeAlias = "My_Database";
    org.mockito.Mockito.when(viewsFeatureGate.isEnabled(routeAlias)).thenReturn(true);

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/databases/" + routeAlias + "/views")
                .accept(MediaType.APPLICATION_JSON)
                .param("sortBy", "viewId")
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk());
  }

  /** A default namespace naming the seeded database in another case also names an existing one. */
  @Test
  public void mixedCaseDefaultNamespaceAliasOfASeededDatabaseIsRecognizedAsExisting()
      throws Exception {
    String viewId = "case_alias_namespace_view";
    try {
      mvc.perform(
              MockMvcRequestBuilders.post(VIEWS_PATH)
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(
                      ViewModelConstants.createRequestWithoutBaseVersion()
                          .toBuilder()
                          .viewId(viewId)
                          .defaultNamespace(java.util.Collections.singletonList("MY_DATABASE"))
                          .build()
                          .toJson())
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().isCreated());
    } finally {
      deleteViewIfPresent(viewId);
    }
  }

  /**
   * POST/PUT item responses carry the same creator and creation time a later GET returns: the
   * creator is the creating principal and survives replacement by another principal; the creation
   * time is set at create and preserved by changed and no-op replacements.
   */
  @Test
  public void writeResponsesCarryCreatorAndCreationTimeMatchingSubsequentGet() throws Exception {
    String viewId = "creator_time_view";
    String creator = "DUMMY_ANONYMOUS_USER";
    String replacer = "second-view-writer";
    String replacerToken = new DummyTokenInterceptor.DummySecurityJWT(replacer).buildNoopJWT();
    String viewPath = VIEWS_PATH + "/" + viewId;
    try {
      MvcResult create =
          mvc.perform(
                  MockMvcRequestBuilders.post(VIEWS_PATH)
                      .contentType(MediaType.APPLICATION_JSON)
                      .content(
                          ViewModelConstants.createRequestWithoutBaseVersion()
                              .toBuilder()
                              .viewId(viewId)
                              .build()
                              .toJson())
                      .accept(MediaType.APPLICATION_JSON)
                      .header("Authorization", "Bearer " + jwtAccessToken))
              .andExpect(status().isCreated())
              .andExpect(jsonPath("$.viewCreator").value(creator))
              .andReturn();
      long createdAt = readLong(create, "$.creationTime");
      org.junit.jupiter.api.Assertions.assertTrue(createdAt > 0, "creationTime must be set");
      assertGetMatches(viewPath, creator, createdAt);

      String base = com.jayway.jsonpath.JsonPath.read(responseBody(create), "$.metadataLocation");
      com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody changed =
          ViewModelConstants.fullyPopulatedRequest()
              .toBuilder()
              .viewId(viewId)
              .representations(
                  java.util.Collections.singletonList(
                      com.linkedin.openhouse.tables.api.spec.v0.request.components
                          .ViewRepresentation.builder()
                          .type("sql")
                          .dialect(ViewModelConstants.SOURCE_DIALECT)
                          .sql("SELECT id FROM my_database.my_table")
                          .build()))
              .build();
      MvcResult replace =
          mvc.perform(
                  MockMvcRequestBuilders.put(viewPath)
                      .contentType(MediaType.APPLICATION_JSON)
                      .content(changed.toBuilder().baseMetadataLocation(base).build().toJson())
                      .accept(MediaType.APPLICATION_JSON)
                      .header("Authorization", "Bearer " + replacerToken))
              .andExpect(status().isOk())
              .andExpect(jsonPath("$.viewCreator").value(creator))
              .andReturn();
      String replaced =
          com.jayway.jsonpath.JsonPath.read(responseBody(replace), "$.metadataLocation");
      org.junit.jupiter.api.Assertions.assertNotEquals(base, replaced);
      org.junit.jupiter.api.Assertions.assertEquals(createdAt, readLong(replace, "$.creationTime"));
      assertGetMatches(viewPath, creator, createdAt);

      MvcResult noOp =
          mvc.perform(
                  MockMvcRequestBuilders.put(viewPath)
                      .contentType(MediaType.APPLICATION_JSON)
                      .content(changed.toBuilder().baseMetadataLocation(replaced).build().toJson())
                      .accept(MediaType.APPLICATION_JSON)
                      .header("Authorization", "Bearer " + replacerToken))
              .andExpect(status().isOk())
              .andExpect(jsonPath("$.metadataLocation").value(replaced))
              .andExpect(jsonPath("$.viewCreator").value(creator))
              .andReturn();
      org.junit.jupiter.api.Assertions.assertEquals(createdAt, readLong(noOp, "$.creationTime"));
      assertGetMatches(viewPath, creator, createdAt);
    } finally {
      deleteViewIfPresent(viewId);
    }
  }

  private void assertGetMatches(String viewPath, String creator, long creationTime)
      throws Exception {
    MvcResult get =
        mvc.perform(
                MockMvcRequestBuilders.get(viewPath)
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.viewCreator").value(creator))
            .andReturn();
    org.junit.jupiter.api.Assertions.assertEquals(creationTime, readLong(get, "$.creationTime"));
  }

  private static String responseBody(MvcResult result) throws Exception {
    return result.getResponse().getContentAsString();
  }

  private static long readLong(MvcResult result, String path) throws Exception {
    Number value = com.jayway.jsonpath.JsonPath.read(responseBody(result), path);
    return value.longValue();
  }

  @Test
  public void configuredTokenInterceptorRejectsMissingCredentialsBeforeViewService()
      throws Exception {
    // The auto-configured MockMvc inherits a valid default token from MockMvcBuilderConfig, so
    // prove both a truly absent header (client without that default) and an empty override.
    MockMvcBuilders.webAppContextSetup(webApplicationContext)
        .build()
        .perform(MockMvcRequestBuilders.get(VIEW_PATH).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isUnauthorized());
    mvc.perform(
            MockMvcRequestBuilders.get(VIEW_PATH)
                .header("Authorization", "")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isUnauthorized());
  }

  @Test
  public void absentPutWithStaleTokenDoesNotRecreateDeletedView() throws Exception {
    MvcResult create =
        mvc.perform(
                MockMvcRequestBuilders.post(VIEWS_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(ViewModelConstants.createRequestWithoutBaseVersion().toJson())
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isCreated())
            .andReturn();
    String deletedBase =
        com.jayway.jsonpath.JsonPath.read(
            create.getResponse().getContentAsString(), "$.metadataLocation");

    mvc.perform(
            MockMvcRequestBuilders.delete(VIEW_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNoContent());
    long filesBeforeStalePut = metadataFileCount();

    mvc.perform(
            MockMvcRequestBuilders.put(VIEW_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    ViewModelConstants.fullyPopulatedRequest()
                        .toBuilder()
                        .baseMetadataLocation(deletedBase)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.message").value(not(containsString(deletedBase))));
    org.junit.jupiter.api.Assertions.assertEquals(filesBeforeStalePut, metadataFileCount());

    mvc.perform(
            MockMvcRequestBuilders.get(VIEW_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNotFound());
  }

  @Test
  public void listContinuationSupportsChangingClientSizesWithoutSkipOrRepeat() throws Exception {
    createView("view_a");
    createView("view_b");
    createView("view_c");
    createView("view_d");
    createView("view_e");

    MvcResult first =
        mvc.perform(
                MockMvcRequestBuilders.get(VIEWS_PATH)
                    .accept(MediaType.APPLICATION_JSON)
                    .param("size", "2")
                    .param("sortBy", "viewId")
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.results[0].viewId").value("view_a"))
            .andExpect(jsonPath("$.results[1].viewId").value("view_b"))
            .andReturn();
    String token =
        com.jayway.jsonpath.JsonPath.read(
            first.getResponse().getContentAsString(), "$.nextPageToken");

    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .param("size", "3")
                .param("sortBy", "viewId")
                .param("pageToken", token)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results[0].viewId").value("view_c"))
        .andExpect(jsonPath("$.results[1].viewId").value("view_d"))
        .andExpect(jsonPath("$.results[2].viewId").value("view_e"));
  }

  private void ensureDatabaseExists() {
    TableDto anchor =
        TableModelConstants.TABLE_DTO
            .toBuilder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .tableId("views_anchor_table")
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

  private void createView(String viewId) throws Exception {
    CreateUpdateViewRequestBody body =
        ViewModelConstants.createRequestWithoutBaseVersion().toBuilder().viewId(viewId).build();
    mvc.perform(
            MockMvcRequestBuilders.post(VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body.toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isCreated());
  }

  private void deleteViewIfPresent(String viewId) throws Exception {
    boolean present =
        houseTableRepository
            .findEntityById(
                HouseTablePrimaryKey.builder()
                    .databaseId(ViewModelConstants.DATABASE_ID)
                    .tableId(viewId)
                    .build())
            .isPresent();
    if (present) {
      mvc.perform(
              MockMvcRequestBuilders.delete(VIEWS_PATH + "/" + viewId)
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().isNoContent());
    }
  }

  private HouseTable findViewRow() {
    return findViewRow(ViewModelConstants.VIEW_ID);
  }

  private HouseTable findViewRow(String viewId) {
    return houseTableRepository
        .findViewById(
            HouseTablePrimaryKey.builder()
                .databaseId(ViewModelConstants.DATABASE_ID)
                .tableId(viewId)
                .build())
        .orElseThrow(() -> new AssertionError("Expected view row to exist"));
  }

  private static void assertOperationAudit(
      ViewAuditEvent event, String viewUuid, String oldPointer, String newPointer) {
    org.junit.jupiter.api.Assertions.assertEquals(
        OperationStatus.SUCCESS, event.getOperationStatus());
    org.junit.jupiter.api.Assertions.assertEquals(
        ViewModelConstants.DATABASE_ID, event.getDatabaseName());
    org.junit.jupiter.api.Assertions.assertEquals(ViewModelConstants.VIEW_ID, event.getViewName());
    org.junit.jupiter.api.Assertions.assertEquals("DUMMY_ANONYMOUS_USER", event.getUser());
    org.junit.jupiter.api.Assertions.assertEquals(viewUuid, event.getViewUUID());
    org.junit.jupiter.api.Assertions.assertEquals(oldPointer, event.getOldMetadataLocation());
    org.junit.jupiter.api.Assertions.assertEquals(newPointer, event.getNewMetadataLocation());
  }

  private long metadataFileCount() throws Exception {
    java.nio.file.Path rootPath =
        Paths.get(storageManager.getDefaultStorage().getClient().getRootPrefix());
    if (!Files.exists(rootPath)) {
      return 0;
    }
    try (java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(rootPath)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".metadata.json"))
          .count();
    }
  }
}
