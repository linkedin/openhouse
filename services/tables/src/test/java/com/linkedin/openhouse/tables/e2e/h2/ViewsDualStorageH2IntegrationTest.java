package com.linkedin.openhouse.tables.e2e.h2;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.mock.properties.CustomClusterPropertiesInitializer;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * Uses the cluster-test-properties storage layout: default storage is HDFS and the regex selector
 * routes {@code local_db.*} to LOCAL. A view in {@code local_db} therefore proves the bridge honors
 * the selected storage rather than stamping the default.
 */
@SpringBootTest(classes = SpringH2Application.class)
@AutoConfigureMockMvc
@ContextConfiguration(
    initializers = {
      CustomClusterPropertiesInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
public class ViewsDualStorageH2IntegrationTest {

  private static final String DEFAULT_STORAGE_DATABASE_ID = "db";
  private static final String SELECTED_LOCAL_DATABASE_ID = "local_db";
  private static final String VIEW_ID = "dual_storage_view";

  @Autowired private MockMvc mvc;
  @Autowired private OpenHouseInternalRepository openHouseInternalRepository;
  @Autowired private HouseTableRepository houseTableRepository;
  @Autowired private StorageManager storageManager;
  @MockBean private ViewsFeatureGate viewsFeatureGate;

  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws Exception {
    jwtAccessToken =
        new DummyTokenInterceptor.DummySecurityJWT("DUMMY_ANONYMOUS_USER").buildNoopJWT();
    org.mockito.Mockito.when(viewsFeatureGate.isEnabled(ArgumentMatchers.anyString()))
        .thenReturn(true);
    ensureDatabaseExists(DEFAULT_STORAGE_DATABASE_ID);
    ensureDatabaseExists(SELECTED_LOCAL_DATABASE_ID);
  }

  @AfterAll
  static void unsetSysProp() {
    System.clearProperty("OPENHOUSE_CLUSTER_CONFIG_PATH");
  }

  @Test
  public void layoutPreconditionDefaultStorageIsHdfs() {
    Assertions.assertEquals(
        StorageType.HDFS.getValue(), storageManager.getDefaultStorage().getType().getValue());
  }

  @Test
  public void defaultHdfsStoragePersistsStorageTypeAndStableUuidRootAcrossChangedReplace()
      throws Exception {
    assertCreateAndChangedReplaceKeepIdentity(
        DEFAULT_STORAGE_DATABASE_ID, StorageType.HDFS.getValue());
  }

  @Test
  public void selectedNonDefaultLocalStorageIsPersistedInsteadOfTheDefault() throws Exception {
    assertCreateAndChangedReplaceKeepIdentity(
        SELECTED_LOCAL_DATABASE_ID, StorageType.LOCAL.getValue());
  }

  private void assertCreateAndChangedReplaceKeepIdentity(
      String databaseId, String expectedStorageType) throws Exception {
    String viewsPath = "/v1/databases/" + databaseId + "/views";
    MvcResult create =
        mvc.perform(
                MockMvcRequestBuilders.post(viewsPath)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(request(databaseId, null, ViewModelConstants.VIEW_SQL).toJson())
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.viewId").value(VIEW_ID))
            .andReturn();
    String base =
        com.jayway.jsonpath.JsonPath.read(
            create.getResponse().getContentAsString(), "$.metadataLocation");
    HouseTable created = findViewRow(databaseId);
    UUID.fromString(created.getTableUUID());
    Assertions.assertEquals(expectedStorageType, created.getStorageType());
    Assertions.assertEquals(base, created.getTableLocation());
    String createdRoot = viewRoot(created.getTableLocation());
    Assertions.assertTrue(createdRoot.contains(created.getTableUUID()), createdRoot);

    MvcResult replace =
        mvc.perform(
                MockMvcRequestBuilders.put(viewsPath + "/" + VIEW_ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        request(databaseId, base, "SELECT id FROM my_database.my_table").toJson())
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isOk())
            .andReturn();
    String replacement =
        com.jayway.jsonpath.JsonPath.read(
            replace.getResponse().getContentAsString(), "$.metadataLocation");
    Assertions.assertNotEquals(
        base, replacement, "A changed definition must publish a different pointer.");
    HouseTable replaced = findViewRow(databaseId);
    Assertions.assertEquals(replacement, replaced.getTableLocation());
    Assertions.assertEquals(created.getTableUUID(), replaced.getTableUUID());
    Assertions.assertEquals(created.getStorageType(), replaced.getStorageType());
    Assertions.assertEquals(createdRoot, viewRoot(replaced.getTableLocation()));
  }

  private static String viewRoot(String metadataLocation) {
    int metadataDirectory = metadataLocation.lastIndexOf("/metadata/");
    Assertions.assertTrue(metadataDirectory > 0, metadataLocation);
    return metadataLocation.substring(0, metadataDirectory);
  }

  private void ensureDatabaseExists(String databaseId) {
    TableDto anchor =
        TableModelConstants.TABLE_DTO
            .toBuilder()
            .databaseId(databaseId)
            .tableId("views_dual_storage_anchor")
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

  private static CreateUpdateViewRequestBody request(
      String databaseId, String baseMetadataLocation, String sql) {
    return ViewModelConstants.createRequestWithoutBaseVersion()
        .toBuilder()
        .databaseId(databaseId)
        .viewId(VIEW_ID)
        .defaultNamespace(Collections.singletonList(databaseId))
        .representations(
            Collections.singletonList(
                ViewRepresentation.builder()
                    .type(ViewModelConstants.SQL_REPRESENTATION_TYPE)
                    .dialect(ViewModelConstants.SOURCE_DIALECT)
                    .sql(sql)
                    .build()))
        .baseMetadataLocation(baseMetadataLocation)
        .build();
  }

  private HouseTable findViewRow(String databaseId) {
    return houseTableRepository
        .findViewById(
            HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(VIEW_ID).build())
        .orElseThrow(() -> new AssertionError("Expected view row to exist"));
  }
}
