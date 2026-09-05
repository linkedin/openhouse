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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types;
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

  /**
   * The shared outgoing mapping is the table write path, so it declares TABLE; the view write path
   * gets its own mapping so neither caller can inherit the wrong discriminator by omission.
   */
  @Test
  public void outgoingMappingsDeclareTheirOwnEntityType() {
    HouseTable pointer =
        HouseTable.builder()
            .databaseId("d1")
            .tableId("t1")
            .tableLocation("/openhouse/d1/t1/v0.metadata.json")
            .tableVersion("INITIAL_VERSION")
            .storageType("local")
            .build();

    Assertions.assertEquals("TABLE", houseTableMapper.toUserTable(pointer).getEntityType());
    Assertions.assertEquals("VIEW", houseTableMapper.toUserView(pointer).getEntityType());
  }

  /**
   * The stamp is authoritative, not a default: a pointer that already carries the wrong type must
   * still leave through its route's own discriminator, or a mis-typed caller could reach the wrong
   * table.
   */
  @Test
  public void outgoingStampsOverrideAConflictingIncomingEntityType() {
    HouseTable mislabelledAsView =
        HouseTable.builder()
            .databaseId("d1")
            .tableId("t1")
            .tableLocation("/openhouse/d1/t1/v0.metadata.json")
            .entityType("VIEW")
            .build();
    HouseTable mislabelledAsTable = mislabelledAsView.toBuilder().entityType("TABLE").build();
    HouseTable nonsense = mislabelledAsView.toBuilder().entityType("MATERIALIZED_VIEW").build();

    Assertions.assertEquals(
        "TABLE",
        houseTableMapper.toUserTable(mislabelledAsView).getEntityType(),
        "the table route always declares TABLE, whatever the pointer claims");
    Assertions.assertEquals(
        "VIEW",
        houseTableMapper.toUserView(mislabelledAsTable).getEntityType(),
        "the view route always declares VIEW, whatever the pointer claims");
    Assertions.assertEquals("TABLE", houseTableMapper.toUserTable(nonsense).getEntityType());
    Assertions.assertEquals("VIEW", houseTableMapper.toUserView(nonsense).getEntityType());
  }

  /** The two write mappings differ in the discriminator and in nothing else. */
  @Test
  public void theViewWriteMappingCarriesTheSamePointerShapeAsTheTableOne() {
    HouseTable pointer =
        HouseTable.builder()
            .databaseId("d1")
            .tableId("t1")
            .tableLocation("/openhouse/d1/t1/v0.metadata.json")
            .tableVersion("INITIAL_VERSION")
            .storageType("local")
            .creationTime(1700000000000L)
            .build();

    UserTable asView = houseTableMapper.toUserView(pointer);
    UserTable asTable = houseTableMapper.toUserTable(pointer);

    Assertions.assertEquals("/openhouse/d1/t1/v0.metadata.json", asView.getMetadataLocation());
    Assertions.assertEquals("INITIAL_VERSION", asView.getTableVersion());
    Assertions.assertEquals("d1", asView.getDatabaseId());
    Assertions.assertEquals("t1", asView.getTableId());
    Assertions.assertEquals("local", asView.getStorageType());
    Assertions.assertEquals(1700000000000L, asView.getCreationTime());

    asView.setEntityType(asTable.getEntityType());
    Assertions.assertEquals(
        asTable, asView, "the view write body must differ from the table body only by its type");
  }

  /** A hydrated row states its type, and the client keeps that value verbatim. */
  @Test
  public void incomingMappingCarriesTheServerResolvedDiscriminator() {
    for (String entityType : new String[] {"TABLE", "VIEW"}) {
      UserTable userTable = new UserTable();
      userTable.setDatabaseId("d1");
      userTable.setTableId("t1");
      userTable.setMetadataLocation("/openhouse/d1/t1/v0.metadata.json");
      userTable.setEntityType(entityType);

      HouseTable houseTable = houseTableMapper.toHouseTable(userTable);

      Assertions.assertEquals(entityType, houseTable.getEntityType());
      Assertions.assertEquals("/openhouse/d1/t1/v0.metadata.json", houseTable.getTableLocation());
    }
  }

  /**
   * A wire value is carried through unaltered rather than normalized: classifying it belongs to the
   * layer that knows which endpoint answered, and silently repairing it would hide corruption.
   */
  @Test
  public void incomingMappingDoesNotInventOrNormalizeADiscriminator() {
    UserTable missing = new UserTable();
    missing.setDatabaseId("d1");
    missing.setTableId("t1");
    Assertions.assertNull(houseTableMapper.toHouseTable(missing).getEntityType());

    UserTable nonCanonical = new UserTable();
    nonCanonical.setDatabaseId("d1");
    nonCanonical.setTableId("t1");
    nonCanonical.setEntityType("view");
    Assertions.assertEquals("view", houseTableMapper.toHouseTable(nonCanonical).getEntityType());
  }

  private HadoopFileIO localFileIO() {
    HadoopFileIO fileIO = new HadoopFileIO(new Configuration());
    LocalStorage localStorage = mock(LocalStorage.class);
    when(fileIOManager.getStorage(fileIO)).thenReturn(localStorage);
    when(localStorage.getType()).thenReturn(StorageType.LOCAL);
    return fileIO;
  }

  /**
   * This overload takes fields already stripped to their bare names, so it copies values verbatim.
   * The namespace belongs to the key, and a value that merely looks like one is still just a value.
   */
  @Test
  public void simpleMapperTest() {
    HadoopFileIO fileIO = localFileIO();

    HouseTable houseTable =
        houseTableMapper.toHouseTable(
            ImmutableMap.of("databaseId", "openhouse.database", "tableId", "table"), fileIO);

    Assertions.assertEquals("openhouse.database", houseTable.getDatabaseId());
    Assertions.assertEquals("table", houseTable.getTableId());
    Assertions.assertEquals("local", houseTable.getStorageType());
  }

  /**
   * Stripping happens on the way in from table metadata, where the server-owned fields are
   * namespaced keys among the caller's own properties, and everything unrecognized is left behind.
   */
  @Test
  public void toHouseTableStripsTheNamespaceFromMetadataKeysAndIgnoresForeignOnes() {
    HadoopFileIO fileIO = localFileIO();
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            PartitionSpec.unpartitioned(),
            "/tmp/openhouse/db/tbl",
            ImmutableMap.of(
                "openhouse.databaseId", "db",
                "openhouse.tableId", "tbl",
                "openhouse.tableVersion", "INITIAL_VERSION",
                "user.owner", "team-a"));

    HouseTable houseTable = houseTableMapper.toHouseTable(metadata, fileIO);

    Assertions.assertEquals("db", houseTable.getDatabaseId());
    Assertions.assertEquals("tbl", houseTable.getTableId());
    Assertions.assertEquals("INITIAL_VERSION", houseTable.getTableVersion());
    Assertions.assertEquals("local", houseTable.getStorageType());
    Assertions.assertNull(
        houseTable.getTableUUID(), "an un-namespaced property is not an HTS field");
    Assertions.assertNull(
        houseTable.getEntityType(),
        "entity type belongs to the House Table row and its route, never to table metadata");
  }
}
