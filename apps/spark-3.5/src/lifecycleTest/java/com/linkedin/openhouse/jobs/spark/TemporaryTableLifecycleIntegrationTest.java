package com.linkedin.openhouse.jobs.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.internal.catalog.AbstractCommitStatsPublishOperation;
import com.linkedin.openhouse.internal.catalog.CommitStats;
import com.linkedin.openhouse.internal.catalog.PostCommitOperation;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.optimizer.analyzer.AnalyzeRequest;
import com.linkedin.openhouse.optimizer.analyzer.AnalyzerRunner;
import com.linkedin.openhouse.optimizer.analyzer.ExpiredTableDeletionAnalyzer;
import com.linkedin.openhouse.optimizer.api.controller.TableOperationsController;
import com.linkedin.openhouse.optimizer.api.controller.TableStatsController;
import com.linkedin.openhouse.optimizer.client.api.TableStatsControllerApi;
import com.linkedin.openhouse.optimizer.client.invoker.ApiClient;
import com.linkedin.openhouse.optimizer.client.model.CommitDeltaDto;
import com.linkedin.openhouse.optimizer.client.model.SnapshotMetricsDto;
import com.linkedin.openhouse.optimizer.client.model.TableStatsPayload;
import com.linkedin.openhouse.optimizer.client.model.UpsertTableStatsRequest;
import com.linkedin.openhouse.optimizer.db.HistoryStatus;
import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.optimizer.scheduler.SchedulerRunner;
import com.linkedin.openhouse.optimizer.scheduler.account.DummyAccountResolver;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import com.linkedin.openhouse.optimizer.scheduler.config.SchedulerConfig;
import com.linkedin.openhouse.optimizer.service.OptimizerDataServiceImpl;
import com.linkedin.openhouse.tablestest.SpringH2TestApplication;
import com.linkedin.openhouse.tablestest.TestSparkSessionUtil;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Runs local Spark against the embedded tables service and the Optimizer's HTTP API and MySQL. Only
 * remote job submission and the LinkedIn-specific stats transport are replaced: the local adapter
 * records the scheduler's HTTP request and executes the real app with its unchanged args.
 *
 * <p>Run with {@code ./gradlew :apps:openhouse-spark-apps-1.5_2.12:lifecycleTest}. Docker must be
 * available for the test-owned MySQL container.
 */
class TemporaryTableLifecycleIntegrationTest {
  private static final String DATABASE = "temporary_lifecycle";
  private static final List<String> EXPIRED = Arrays.asList("expired_one", "expired_two");
  private static final List<String> RETAINED = Arrays.asList("unexpired", "ordinary", "converted");
  private static final List<String> ALL_TABLES =
      Arrays.asList("expired_one", "expired_two", "unexpired", "ordinary", "converted");
  private static final OperationTypeDto DELETION = OperationTypeDto.EXPIRED_TABLE_DELETION;

  @Configuration
  @EnableAutoConfiguration(
      exclude = {SecurityAutoConfiguration.class, ManagementWebSecurityAutoConfiguration.class})
  @EntityScan(basePackageClasses = TableOperationsRow.class)
  @EnableJpaRepositories(basePackageClasses = TableOperationsRepository.class)
  @Import({
    TableStatsController.class,
    TableOperationsController.class,
    OptimizerDataServiceImpl.class,
    AnalyzerRunner.class,
    ExpiredTableDeletionAnalyzer.class,
    LocalJobsAdapter.class
  })
  static class OptimizerApplication {}

  @Configuration
  static class PublisherConfiguration {
    @Bean
    PostCommitOperation commitStatsPublisher(@Value("${lifecycle.optimizer-uri}") String baseUri) {
      TableStatsControllerApi api =
          new TableStatsControllerApi(new ApiClient().setBasePath(baseUri));
      return new AbstractCommitStatsPublishOperation() {
        @Override
        protected void publish(CommitStats stats) {
          SnapshotMetricsDto snapshot =
              new SnapshotMetricsDto()
                  .tableVersion(stats.getTableVersion())
                  .tableLocation(stats.getTableLocation())
                  .tableSizeBytes(stats.getTableSizeBytes())
                  .numCurrentFiles(stats.getNumCurrentFiles());
          CommitDeltaDto delta =
              Optional.ofNullable(stats.getDelta())
                  .map(
                      value ->
                          new CommitDeltaDto()
                              .numFilesAdded(value.getNumFilesAdded())
                              .numFilesDeleted(value.getNumFilesDeleted())
                              .addedSizeBytes(value.getAddedSizeBytes())
                              .deletedSizeBytes(value.getDeletedSizeBytes()))
                  .orElse(null);
          api.upsertTableStats(
                  stats.getTableUuid(),
                  new UpsertTableStatsRequest()
                      .databaseName(stats.getDatabaseName())
                      .tableName(stats.getTableName())
                      .tableProperties(stats.getTableProperties())
                      .stats(new TableStatsPayload().snapshot(snapshot).delta(delta)))
              .block(Duration.ofSeconds(10));
        }
      };
    }
  }

  @RestController
  static class LocalJobsAdapter {
    private final List<JsonNode> submissions = new CopyOnWriteArrayList<>();

    @PostMapping("/jobs")
    Map<String, String> submit(@RequestBody JsonNode request) {
      submissions.add(request);
      return Collections.singletonMap("jobId", "local-job-" + submissions.size());
    }

    void execute(Operations operations, String optimizerUri) {
      submissions.forEach(
          submission -> {
            JsonNode configuration = submission.path("jobConf");
            assertThat(configuration.path("jobType").asText())
                .isEqualTo("EXPIRED_TABLE_DELETION_BATCH");
            List<String> arguments =
                StreamSupport.stream(configuration.path("args").spliterator(), false)
                    .map(JsonNode::asText)
                    .collect(Collectors.toCollection(ArrayList::new));
            assertThat(arguments).hasSize(6);
            assertThat(arguments.get(1)).doesNotContain(",");
            assertThat(arguments.get(3)).doesNotContain(",");
            assertThat(arguments.get(5)).isEqualTo(optimizerUri);
            arguments.addAll(Arrays.asList("--storageURL", optimizerUri, "--jobId", "local-job"));
            ExpiredTableDeletionSparkApp.createApp(
                    arguments.toArray(new String[0]),
                    new AppsOtelEmitter(
                        Collections.singletonList(DefaultOtelConfig.getOpenTelemetry())))
                .runInner(operations);
          });
    }
  }

  @Test
  @Timeout(240)
  void committedTablesExpireAndArePurgedThroughTheScheduledJob() throws Exception {
    Path root =
        Files.createDirectories(
            Paths.get("build", "temporary-table-lifecycle", UUID.randomUUID().toString())
                .toAbsolutePath());
    org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.disable();
    try (ConfigurableApplicationContext optimizer = startOptimizer(root)) {
      String optimizerUri = serviceUri(optimizer);
      try (ConfigurableApplicationContext tables =
          new SpringApplication(SpringH2TestApplication.class, PublisherConfiguration.class)
              .run(
                  "--server.port=0",
                  "--spring.flyway.enabled=false",
                  "--spring.jpa.hibernate.ddl-auto=create-drop",
                  "--spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
                  "--spring.datasource.url=jdbc:h2:mem:lifecycle;MODE=MYSQL;DB_CLOSE_DELAY=-1",
                  "--cluster.housetables.database.url=jdbc:h2:mem:"
                      + root.getFileName()
                      + ";MODE=MYSQL;DB_CLOSE_DELAY=-1",
                  "--cluster.storage.root-path=" + root.resolve("tables"),
                  "--cluster.storages.default-type=local",
                  "--cluster.storages.types.local.rootpath=" + root.resolve("tables"),
                  "--cluster.storages.types.local.endpoint=file:///",
                  "--server.tomcat.basedir=" + root.resolve("tables-tomcat"),
                  "--cluster.tables.postcommit.enabled=true",
                  "--cluster.tables.postcommit.max-threads=1",
                  "--lifecycle.optimizer-uri=" + optimizerUri)) {
        SparkSession.Builder builder =
            TestSparkSessionUtil.getBaseBuilder(URI.create("file:///"))
                .config("spark.local.dir", root.resolve("spark").toString())
                .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString())
                .config("spark.sql.catalog.openhouse.cache-enabled", false);
        TestSparkSessionUtil.configureCatalogs(
            builder, "openhouse", URI.create(serviceUri(tables)));
        try (SparkSession spark = builder.getOrCreate();
            Operations operations =
                Operations.withCatalog(
                    spark,
                    new AppsOtelEmitter(
                        Collections.singletonList(DefaultOtelConfig.getOpenTelemetry())))) {
          runLifecycle(spark, operations, optimizer, optimizerUri, root.resolve("tables"));
        }
      }
    }
  }

  private static ConfigurableApplicationContext startOptimizer(Path root) {
    return new SpringApplication(OptimizerApplication.class)
        .run(
            "--spring.config.name=temporary-lifecycle-optimizer",
            "--server.port=0",
            "--server.tomcat.basedir=" + root.resolve("optimizer-tomcat"),
            "--spring.datasource.url=jdbc:tc:mysql:"
                + System.getProperty("openhouse.test.mysql.version")
                + ":///optimizer?socketTimeout=60000&connectionAttributes=context:"
                + UUID.randomUUID(),
            "--spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
            "--spring.jpa.hibernate.ddl-auto=validate",
            "--spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.MySQL8Dialect",
            "--spring.jpa.hibernate.naming.physical-strategy=org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl",
            "--spring.flyway.enabled=true",
            "--analyzer.expired-table-deletion.enabled=true");
  }

  private static void runLifecycle(
      SparkSession spark,
      Operations operations,
      ConfigurableApplicationContext optimizer,
      String optimizerUri,
      Path storageRoot)
      throws Exception {
    Catalog catalog = Spark3Util.loadIcebergCatalog(spark, "openhouse");
    Map<String, List<Path>> dataFiles = new LinkedHashMap<>();
    for (String name : RETAINED) {
      String properties =
          "unexpired".equals(name) ? " TBLPROPERTIES ('openhouse.temporary'='true')" : "";
      spark.sql("CREATE TABLE openhouse." + DATABASE + "." + name + " (id BIGINT)" + properties);
      spark.sql("INSERT INTO openhouse." + DATABASE + "." + name + " VALUES (1), (2)");
    }
    Instant expiration = Instant.now().plusSeconds(30).truncatedTo(ChronoUnit.SECONDS);
    for (String name : EXPIRED) {
      spark.sql(
          "CREATE TABLE openhouse."
              + DATABASE
              + "."
              + name
              + " (id BIGINT)"
              + " TBLPROPERTIES ('openhouse.temporary'='true', 'openhouse.expirationTime'='"
              + expiration
              + "')");
      spark.sql("INSERT INTO openhouse." + DATABASE + "." + name + " VALUES (1), (2)");
    }
    makeTemporary(catalog.loadTable(identifier("converted")), expiration);
    catalog
        .loadTable(identifier("ordinary"))
        .updateProperties()
        .set("optimizer.commitStatsCollectionEnabled", "true")
        .commit();
    catalog
        .loadTable(identifier("converted"))
        .updateProperties()
        .remove("openhouse.temporary")
        .commit();

    for (String name : Arrays.asList("expired_one", "expired_two", "unexpired", "ordinary")) {
      spark.sql("INSERT INTO openhouse." + DATABASE + "." + name + " VALUES (3)");
    }
    for (String name : ALL_TABLES) {
      assertReadableData(spark, name);
      try (CloseableIterable<FileScanTask> files =
          catalog.loadTable(identifier(name)).newScan().planFiles()) {
        dataFiles.put(
            name,
            StreamSupport.stream(files.spliterator(), false)
                .map(
                    file ->
                        Paths.get(
                            new org.apache.hadoop.fs.Path(file.file().path().toString())
                                .toUri()
                                .getPath()))
                .collect(Collectors.toList()));
      }
      assertThat(dataFiles.get(name))
          .isNotEmpty()
          .allSatisfy(path -> assertThat(path).startsWith(storageRoot).exists());
    }

    TableStatsRepository stats = optimizer.getBean(TableStatsRepository.class);
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              List<TableStatsRow> rows = stats.findAll();
              assertThat(rows).hasSize(5);
              assertThat(rows)
                  .filteredOn(row -> EXPIRED.contains(row.getTableName()))
                  .allSatisfy(
                      row -> {
                        assertThat(row.getTableProperties())
                            .containsEntry("openhouse.expirationTime", expiration.toString());
                        assertThat(row.getSnapshot().getNumCurrentFiles()).isPositive();
                      });
              assertThat(rows)
                  .filteredOn(row -> row.getTableName().equals("converted"))
                  .allSatisfy(
                      row ->
                          assertThat(row.getTableProperties())
                              .doesNotContainKeys(
                                  "openhouse.temporary", "openhouse.expirationTime"));
            });

    AnalyzerRunner analyzer = optimizer.getBean(AnalyzerRunner.class);
    TableOperationsRepository operationsRepository =
        optimizer.getBean(TableOperationsRepository.class);
    SchedulerRunner scheduler =
        new SchedulerConfig()
            .schedulerRunner(
                operationsRepository,
                stats,
                new JobsServiceClient(
                    WebClient.builder().baseUrl(optimizerUri).build(), "local-cluster"),
                new DummyAccountResolver(),
                optimizerUri,
                1000000,
                50,
                1000000,
                25);
    LocalJobsAdapter jobs = optimizer.getBean(LocalJobsAdapter.class);
    AnalyzeRequest scan = AnalyzeRequest.builder().operationTypes(Set.of(DELETION)).build();
    analyzer.analyze(scan);
    assertThat(operationsRepository.findAll()).isEmpty();
    await().atMost(Duration.ofSeconds(40)).until(() -> !Instant.now().isBefore(expiration));
    EXPIRED.forEach(
        name -> {
          assertThat(catalog.loadTable(identifier(name)).properties())
              .containsEntry("openhouse.temporary", "true");
          assertReadableData(spark, name);
          assertThat(dataFiles.get(name)).allSatisfy(path -> assertThat(path).exists());
        });
    assertThat(catalog.listTables(Namespace.of(DATABASE))).hasSize(5);
    analyzer.analyze(scan);
    assertThat(operationsRepository.findAll())
        .extracting(TableOperationsRow::getTableName)
        .containsExactlyInAnyOrderElementsOf(EXPIRED);
    scheduler.schedule(DELETION);
    assertThat(operationsRepository.findAll())
        .allSatisfy(row -> assertThat(row.getStatus()).isEqualTo(OperationStatus.SCHEDULED));
    assertThat(jobs.submissions).hasSize(2);
    jobs.execute(operations, optimizerUri);

    assertThat(optimizer.getBean(TableOperationsHistoryRepository.class).findAll())
        .hasSize(2)
        .allSatisfy(row -> assertThat(row.getStatus()).isEqualTo(HistoryStatus.SUCCESS));
    assertThat(catalog.listTables(Namespace.of(DATABASE)))
        .extracting(TableIdentifier::name)
        .containsExactlyInAnyOrderElementsOf(RETAINED);
    EXPIRED.forEach(
        name ->
            assertThat(dataFiles.get(name)).allSatisfy(path -> assertThat(path).doesNotExist()));
    RETAINED.forEach(
        name -> {
          assertThat(dataFiles.get(name)).allSatisfy(path -> assertThat(path).exists());
          assertReadableData(spark, name);
        });
    analyzer.analyze(scan);
    scheduler.schedule(DELETION);
    assertThat(jobs.submissions).hasSize(2);
    RETAINED.forEach(name -> catalog.dropTable(identifier(name), true));
  }

  private static void makeTemporary(Table table, Instant expiration) {
    table
        .updateProperties()
        .set("openhouse.temporary", "true")
        .set("openhouse.expirationTime", expiration.toString())
        .commit();
  }

  private static void assertReadableData(SparkSession spark, String table) {
    assertThat(
            spark
                .sql("SELECT id FROM openhouse." + DATABASE + "." + table + " ORDER BY id")
                .collectAsList())
        .extracting(row -> row.getLong(0))
        .containsExactlyElementsOf(
            "converted".equals(table) ? Arrays.asList(1L, 2L) : Arrays.asList(1L, 2L, 3L));
  }

  private static TableIdentifier identifier(String table) {
    return TableIdentifier.of(DATABASE, table);
  }

  private static String serviceUri(ConfigurableApplicationContext context) {
    return "http://localhost:" + ((WebServerApplicationContext) context).getWebServer().getPort();
  }
}
