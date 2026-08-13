package com.linkedin.openhouse.internal.catalog.mapper;

import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.local.LocalStorage;
import com.linkedin.openhouse.housetables.client.api.ToggleStatusApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.housetables.client.model.UserTable;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepositoryImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@SpringBootTest
public class HouseTableMapperTest {

  /**
   * Tests that doesn't care on HTS server should import this test configuration as
   *
   * @import(classes = MockConfiguration.class)
   */
  @TestConfiguration
  public static class MockConfiguration {
    @Bean
    public UserTableApi provideMockHtsApiInstance() {
      // Routing the client to access port from Mock server so that Mock server can respond with
      // stub response.
      ApiClient apiClient = new ApiClient();
      return new UserTableApi(apiClient);
    }

    @Bean
    public ToggleStatusApi provideMockHtsApiInstanceForToggle() {
      // Routing the client to access port from Mock server so that Mock server can respond with
      // stub response.
      ApiClient apiClient = new ApiClient();
      return new ToggleStatusApi(apiClient);
    }

    @Bean
    public HouseTableRepository provideRealHtsRepository() {
      return new HouseTableRepositoryImpl();
    }
  }

  @Autowired protected HouseTableMapper houseTableMapper;

  @Autowired FileIOManager fileIOManager;

  @Test
  public void simpleMapperTest() {
    HadoopFileIO fileIO = new HadoopFileIO(new Configuration());
    LocalStorage localStorage = mock(LocalStorage.class);
    when(fileIOManager.getStorage(fileIO)).thenReturn(localStorage);
    when(localStorage.getType()).thenReturn(StorageType.LOCAL);
    HouseTable houseTable =
        houseTableMapper.toHouseTable(
            ImmutableMap.of("databaseId", "openhouse.database", "tableId", "table"), fileIO);
    Assertions.assertEquals("database", houseTable.getDatabaseId());
    Assertions.assertEquals("table", houseTable.getTableId());
    Assertions.assertEquals("local", houseTable.getStorageType());
  }

  /**
   * The discriminator must survive the generated HTS client model in both directions, otherwise the
   * pointer would be written or read back without its type.
   */
  @Test
  public void houseTableGeneratedUserTableRoundTripPreservesEntityType() {
    HouseTable viewHouseTable =
        HouseTable.builder()
            .databaseId("d1")
            .tableId("v1")
            .tableLocation("/base/d1/v1-uuid/00001-x.metadata.json")
            .tableVersion("INITIAL_VERSION")
            .entityType("VIEW")
            .build();

    UserTable viewUserTable = houseTableMapper.toUserTable(viewHouseTable);
    Assertions.assertEquals("VIEW", viewUserTable.getEntityType());
    Assertions.assertEquals(
        "/base/d1/v1-uuid/00001-x.metadata.json", viewUserTable.getMetadataLocation());

    HouseTable viewBack = houseTableMapper.toHouseTable(viewUserTable);
    Assertions.assertEquals("VIEW", viewBack.getEntityType());
    Assertions.assertEquals("/base/d1/v1-uuid/00001-x.metadata.json", viewBack.getTableLocation());

    HouseTable tableHouseTable =
        HouseTable.builder()
            .databaseId("d1")
            .tableId("t1")
            .tableLocation("/base/d1/t1-uuid/00001-y.metadata.json")
            .entityType("TABLE")
            .build();
    UserTable tableUserTable = houseTableMapper.toUserTable(tableHouseTable);
    Assertions.assertEquals("TABLE", tableUserTable.getEntityType());
    Assertions.assertEquals("TABLE", houseTableMapper.toHouseTable(tableUserTable).getEntityType());
  }

  /** Backward compatibility: an absent discriminator maps to null, never "TABLE". */
  @Test
  public void missingEntityTypeMapsToLegacyTableNull() {
    HadoopFileIO fileIO = new HadoopFileIO(new Configuration());
    LocalStorage localStorage = mock(LocalStorage.class);
    when(fileIOManager.getStorage(fileIO)).thenReturn(localStorage);
    when(localStorage.getType()).thenReturn(StorageType.LOCAL);

    HouseTable fromProperties =
        houseTableMapper.toHouseTable(ImmutableMap.of("databaseId", "d1", "tableId", "t1"), fileIO);
    Assertions.assertNull(fromProperties.getEntityType());

    UserTable legacyUserTable =
        houseTableMapper.toUserTable(
            HouseTable.builder().databaseId("d1").tableId("t1").tableLocation("loc").build());
    Assertions.assertNull(legacyUserTable.getEntityType());
    Assertions.assertNull(houseTableMapper.toHouseTable(legacyUserTable).getEntityType());
  }
}
