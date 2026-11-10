package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.exception.ViewExceptionHandler;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import com.linkedin.openhouse.tablestest.SpringH2TestApplication;
import com.linkedin.openhouse.tablestest.TestSparkSessionUtil;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OpenHouseViewEnabledTestSpark3_5 {
  private static final String DATABASE = "viewenabled_db";
  private static final Namespace NAMESPACE = Namespace.of(DATABASE);
  private static final TableIdentifier VIEW_ID = TableIdentifier.of(NAMESPACE, "v_roundtrip");
  private static final String TABLE = "openhouse." + DATABASE + ".base_table";
  private static final String VIEW = "openhouse." + VIEW_ID;
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));

  private ConfigurableApplicationContext server;
  private SparkSession spark;
  private Catalog catalog;
  private ViewCatalog views;
  private HouseTableRepository repository;

  @BeforeAll
  void start() {
    org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.disable();
    server =
        new SpringApplication(SpringH2TestApplication.class, ViewExceptionHandler.class)
            .run("--server.port=0");
    repository = server.getBean(HouseTableRepository.class);
    server
        .getBean(ToggleStatusesRepository.class)
        .save(
            TableToggleStatus.builder()
                .databaseId(DATABASE)
                .tableId(ViewsFeatureGate.GATE_PROBE_TABLE_ID)
                .featureId("views")
                .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
                .build());

    URI serverUri =
        URI.create(
            "http://localhost:" + ((WebServerApplicationContext) server).getWebServer().getPort());
    SparkSession.Builder builder = TestSparkSessionUtil.getBaseBuilder(URI.create("file:///"));
    TestSparkSessionUtil.configureCatalogs(builder, "openhouse", serverUri);
    spark =
        builder
            .config("spark.sql.catalog.openhouse.iceberg-views-enabled", true)
            .config("spark.sql.catalog.openhouse.cache-enabled", false)
            .config("spark.sql.iceberg.rewriteViewCommands.enabled", true)
            .getOrCreate();
    catalog =
        ((HasIcebergCatalog) spark.sessionState().catalogManager().catalog("openhouse"))
            .icebergCatalog();
    views = (ViewCatalog) catalog;
  }

  @BeforeEach
  void createBaseTable() {
    spark.sql("CREATE TABLE " + TABLE + " (id INT)");
    spark.sql("INSERT INTO " + TABLE + " VALUES (1), (2)");
  }

  @AfterEach
  void dropRelations() {
    try {
      views.dropView(VIEW_ID);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + TABLE);
    }
  }

  @AfterAll
  void stop() {
    try {
      if (spark != null) {
        spark.close();
      }
    } finally {
      if (server != null) {
        server.close();
      }
    }
  }

  @Test
  void testCatalogViewRoundTrip() {
    views
        .buildView(VIEW_ID)
        .withSchema(SCHEMA)
        .withQuery("spark", "SELECT id FROM base_table")
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog("openhouse")
        .withProperty("owner", "catalog_test")
        .create();

    String initialMetadata = persistedView().getTableLocation();
    assertTrue(Files.isRegularFile(Paths.get(initialMetadata)));
    View loaded = views.loadView(VIEW_ID);
    assertTrue(SCHEMA.sameSchema(loaded.schema()));
    assertEquals("SELECT id FROM base_table", loaded.sqlFor("spark").sql());
    assertEquals(NAMESPACE, loaded.currentVersion().defaultNamespace());
    assertEquals("openhouse", loaded.currentVersion().defaultCatalog());
    assertEquals("catalog_test", loaded.properties().get("owner"));
    assertEquals(Collections.singletonList(VIEW_ID), views.listViews(NAMESPACE));
    assertEquals(
        Collections.singletonList(TableIdentifier.of(NAMESPACE, "base_table")),
        catalog.listTables(NAMESPACE));
    assertRows(1, 2);
    assertThrows(
        AlreadyExistsException.class,
        () ->
            views
                .buildView(VIEW_ID)
                .withSchema(SCHEMA)
                .withQuery("spark", "SELECT id FROM base_table")
                .withDefaultNamespace(NAMESPACE)
                .create());

    views
        .buildView(VIEW_ID)
        .withSchema(SCHEMA)
        .withQuery("spark", "SELECT id + 10 AS id FROM base_table")
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog("openhouse")
        .replace();

    View replaced = views.loadView(VIEW_ID);
    assertEquals(loaded.uuid(), replaced.uuid());
    assertNotEquals(initialMetadata, persistedView().getTableLocation());
    assertEquals("SELECT id + 10 AS id FROM base_table", replaced.sqlFor("spark").sql());
    assertRows(11, 12);

    replaced.updateProperties().set("owner", "updated_owner").commit();
    assertEquals("updated_owner", views.loadView(VIEW_ID).properties().get("owner"));
    assertThrows(
        BadRequestException.class,
        () ->
            views
                .loadView(VIEW_ID)
                .updateProperties()
                .set("openhouse.tableUUID", "changed")
                .commit());
    assertEquals(loaded.uuid(), views.loadView(VIEW_ID).uuid());

    assertTrue(views.dropView(VIEW_ID));
    assertFalse(views.viewExists(VIEW_ID));
    assertTrue(views.listViews(NAMESPACE).isEmpty());
    assertThrows(NoSuchViewException.class, () -> views.loadView(VIEW_ID));
    assertFalse(views.dropView(VIEW_ID));
    assertFalse(repository.findViewById(viewKey()).isPresent());
  }

  @ParameterizedTest
  @ValueSource(strings = {"CREATE VIEW", "CREATE OR REPLACE VIEW"})
  void testSqlViewRoundTrip(String createCommand) {
    spark.sql(createCommand + " " + VIEW + " AS SELECT id FROM " + TABLE);
    assertRows(1, 2);
    assertEquals(
        Collections.singletonList(RowFactory.create(DATABASE, VIEW_ID.name(), false)),
        spark.sql("SHOW VIEWS IN openhouse." + DATABASE).collectAsList());
    String initialMetadata = persistedView().getTableLocation();

    spark.sql("CREATE OR REPLACE VIEW " + VIEW + " AS SELECT id + 10 AS id FROM " + TABLE);
    assertRows(11, 12);
    assertNotEquals(initialMetadata, persistedView().getTableLocation());

    spark.sql("DROP VIEW " + VIEW);
    assertTrue(spark.sql("SHOW VIEWS IN openhouse." + DATABASE).collectAsList().isEmpty());
    assertThrows(AnalysisException.class, () -> spark.sql("SELECT * FROM " + VIEW).collectAsList());
    assertFalse(repository.findViewById(viewKey()).isPresent());
    spark.sql("DROP VIEW IF EXISTS " + VIEW);
    assertEquals(2L, spark.sql("SELECT * FROM " + TABLE).count());
  }

  private void assertRows(int first, int second) {
    assertEquals(
        Arrays.asList(RowFactory.create(first), RowFactory.create(second)),
        spark.sql("SELECT id FROM " + VIEW + " ORDER BY id").collectAsList());
  }

  private HouseTable persistedView() {
    return repository.findViewById(viewKey()).orElseThrow(AssertionError::new);
  }

  private HouseTablePrimaryKey viewKey() {
    return HouseTablePrimaryKey.builder().databaseId(DATABASE).tableId(VIEW_ID.name()).build();
  }
}
