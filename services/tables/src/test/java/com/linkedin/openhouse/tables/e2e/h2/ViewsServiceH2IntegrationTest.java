package com.linkedin.openhouse.tables.e2e.h2;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.exception.ViewExceptionHandler;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest(
    classes = SpringH2Application.class,
    properties = "cluster.tables.views.supported-dialects=spark,trino")
@AutoConfigureMockMvc
@Import(ViewExceptionHandler.class)
@ContextConfiguration(
    initializers = {
      ViewsServiceH2IntegrationTest.StorageInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ViewsServiceH2IntegrationTest {

  private static final String VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views";
  private static final String VIEW_PATH = VIEWS_PATH + "/" + ViewModelConstants.VIEW_ID;

  @Autowired private MockMvc mvc;
  @Autowired private ClusterProperties clusterProperties;
  @Autowired private StorageManager storageManager;
  @Autowired private OpenHouseInternalRepository openHouseInternalRepository;
  @Autowired private HouseTableRepository houseTableRepository;
  @MockBean private ViewsFeatureGate viewsFeatureGate;
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
  public void missingDatabaseGetAndDeleteAreNotFoundAndListIsEmpty() throws Exception {
    String databaseId = "missing_views_database";
    String viewsPath = "/v1/databases/" + databaseId + "/views";
    Mockito.when(viewsFeatureGate.isEnabled(databaseId)).thenReturn(true);

    mvc.perform(
            MockMvcRequestBuilders.get(viewsPath + "/missing_view")
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNotFound());
    mvc.perform(
            MockMvcRequestBuilders.get(viewsPath)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results").isEmpty())
        .andExpect(jsonPath("$.nextPageToken").doesNotExist());
    mvc.perform(
            MockMvcRequestBuilders.delete(viewsPath + "/missing_view")
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNotFound());
  }

  /**
   * A database exists once it holds a table or a view, so a write into a database holding only
   * views proceeds through the view probe, while a write into neither is a 404 with no row.
   */
  @Test
  public void writesProbeTablesThenViewsAndNeverIntroduceADatabase() throws Exception {
    String viewsOnlyDatabase = "views_only_write_database";
    String missingDatabase = "missing_write_database";
    Mockito.when(viewsFeatureGate.isEnabled(viewsOnlyDatabase)).thenReturn(true);
    Mockito.when(viewsFeatureGate.isEnabled(missingDatabase)).thenReturn(true);
    HouseTablePrimaryKey seededKey =
        HouseTablePrimaryKey.builder().databaseId(viewsOnlyDatabase).tableId("seed_view").build();
    HouseTablePrimaryKey createdKey =
        HouseTablePrimaryKey.builder().databaseId(viewsOnlyDatabase).tableId("new_view").build();
    houseTableRepository.saveView(
        HouseTable.builder()
            .databaseId(viewsOnlyDatabase)
            .tableId(seededKey.getTableId())
            .tableUUID("seed-view-uuid")
            .tableLocation("file:/warehouse/" + viewsOnlyDatabase + "/seed_view/metadata.json")
            .storageType("local")
            .build());
    try {
      mvc.perform(postViewInto(missingDatabase, "new_view")).andExpect(status().isNotFound());
      org.junit.jupiter.api.Assertions.assertFalse(
          houseTableRepository
              .findEntityById(
                  HouseTablePrimaryKey.builder()
                      .databaseId(missingDatabase)
                      .tableId("new_view")
                      .build())
              .isPresent());

      mvc.perform(postViewInto(viewsOnlyDatabase, createdKey.getTableId()))
          .andExpect(status().isCreated());
    } finally {
      houseTableRepository.deleteViewById(createdKey);
      houseTableRepository.deleteViewById(seededKey);
    }
  }

  private MockHttpServletRequestBuilder postViewInto(String databaseId, String viewId) {
    return MockMvcRequestBuilders.post("/v1/databases/" + databaseId + "/views")
        .contentType(MediaType.APPLICATION_JSON)
        .content(
            ViewModelConstants.createRequestWithoutBaseVersion()
                .toBuilder()
                .databaseId(databaseId)
                .viewId(viewId)
                .build()
                .toJson())
        .accept(MediaType.APPLICATION_JSON)
        .header("Authorization", "Bearer " + jwtAccessToken);
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
  }

  @Test
  public void changedSourceDialectReturns400WithoutPublicationAndSameDialectStillReplaces()
      throws Exception {
    String viewId = "immutable_source_view";
    createView(viewId);
    try {
      HouseTable before = findViewRow(viewId);
      String pointer = before.getTableLocation();
      String privateSql = "SELECT private_dialect_sql FROM private_table";
      CreateUpdateViewRequestBody changed =
          ViewModelConstants.createRequestWithoutBaseVersion()
              .toBuilder()
              .viewId(viewId)
              .baseMetadataLocation(pointer)
              .viewProperties(ViewModelConstants.viewPropertiesWithSourceDialect("trino"))
              .representations(
                  java.util.Collections.singletonList(
                      ViewRepresentation.builder()
                          .type("sql")
                          .dialect("trino")
                          .sql(privateSql)
                          .build()))
              .build();
      Mockito.clearInvocations(storageSelector);

      MvcResult rejected =
          mvc.perform(
                  MockMvcRequestBuilders.put(VIEWS_PATH + "/" + viewId)
                      .contentType(MediaType.APPLICATION_JSON)
                      .content(changed.toJson())
                      .accept(MediaType.APPLICATION_JSON)
                      .header("Authorization", "Bearer " + jwtAccessToken))
              .andExpect(status().isBadRequest())
              .andExpect(jsonPath("$.status").value("BAD_REQUEST"))
              .andExpect(jsonPath("$.error").value("Bad Request"))
              .andExpect(jsonPath("$.message").value("sourceDialect must match the current view"))
              .andExpect(jsonPath("$.cause").doesNotExist())
              .andExpect(jsonPath("$.stacktrace").doesNotExist())
              .andReturn();
      String response = responseBody(rejected);
      org.junit.jupiter.api.Assertions.assertFalse(response.contains(privateSql));
      org.junit.jupiter.api.Assertions.assertFalse(response.contains(changed.getSchema()));
      org.junit.jupiter.api.Assertions.assertFalse(response.contains("DUMMY_ANONYMOUS_USER"));
      org.junit.jupiter.api.Assertions.assertFalse(response.contains(pointer));
      org.junit.jupiter.api.Assertions.assertFalse(response.contains("Exception"));
      org.junit.jupiter.api.Assertions.assertEquals(
          pointer, findViewRow(viewId).getTableLocation());
      Mockito.verifyNoInteractions(storageSelector);

      mvc.perform(
              MockMvcRequestBuilders.put(VIEWS_PATH + "/" + viewId)
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(
                      changed
                          .toBuilder()
                          .viewProperties(ViewModelConstants.VIEW_PROPERTIES)
                          .representations(
                              java.util.Collections.singletonList(
                                  ViewRepresentation.builder()
                                      .type("sql")
                                      .dialect(ViewModelConstants.SOURCE_DIALECT)
                                      .sql(privateSql)
                                      .build()))
                          .build()
                          .toJson())
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().isOk());
      org.junit.jupiter.api.Assertions.assertNotEquals(
          pointer, findViewRow(viewId).getTableLocation());
    } finally {
      deleteViewIfPresent(viewId);
    }
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"   ", "unsupported", "SPARK"})
  public void invalidCreateSourceDialectReturns400BeforeAllocation(String dialect)
      throws Exception {
    String viewId = "invalid_source_view";
    Mockito.clearInvocations(storageSelector);
    mvc.perform(
            MockMvcRequestBuilders.post(VIEWS_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    ViewModelConstants.createRequestWithoutBaseVersion()
                        .toBuilder()
                        .viewId(viewId)
                        .viewProperties(ViewModelConstants.viewPropertiesWithSourceDialect(dialect))
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isBadRequest());
    Mockito.verifyNoInteractions(storageSelector);
    org.junit.jupiter.api.Assertions.assertFalse(
        houseTableRepository
            .findEntityById(
                HouseTablePrimaryKey.builder()
                    .databaseId(ViewModelConstants.DATABASE_ID)
                    .tableId(viewId)
                    .build())
            .isPresent());
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

  private long metadataFileCount() throws Exception {
    java.nio.file.Path rootPath =
        Paths.get(storageManager.getDefaultStorage().getClient().getRootPrefix());
    try (java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(rootPath)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".metadata.json"))
          .count();
    }
  }

  public static final class StorageInitializer extends PropertyOverrideContextInitializer {
    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
      super.initialize(applicationContext);
      String root =
          applicationContext.getEnvironment().getRequiredProperty("cluster.storage.root-path");
      TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
          applicationContext,
          "cluster.storages.default-type=local",
          "cluster.storages.types.local.endpoint=file:",
          "cluster.storages.types.local.root-path=" + root);
    }
  }
}
