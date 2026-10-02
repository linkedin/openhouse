package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/** Each Spring test context owns a real, disposable HTS process and database in Docker mode. */
public class TableE2eContextInitializer extends PropertyOverrideContextInitializer {
  @Override
  public void initialize(ConfigurableApplicationContext context) {
    super.initialize(context);
    String storageRoot = context.getEnvironment().getProperty("cluster.storage.root-path");
    TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
        context,
        "cluster.storages.default-type="
            + context.getEnvironment().getProperty("cluster.storages.default-type", "local"),
        "cluster.storages.types.local.rootpath=" + storageRoot,
        "cluster.storages.types.local.endpoint=file:///",
        "cluster.storages.types.hdfs.rootpath=" + storageRoot,
        "cluster.storages.types.hdfs.endpoint=file:///");
    String backend = System.getProperty("tableE2eBackend", "docker");
    if (!"docker".equals(backend) && !"h2".equals(backend)) {
      throw new IllegalArgumentException("tableE2eBackend must be docker or h2");
    }
    TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
        context, "tableE2eBackend=" + backend);
    if ("h2".equals(backend)) {
      TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
          context, "spring.jpa.mapping-resources=table-e2e-orm.xml");
      HouseTablesH2Repository.softDeletedTables.clear();
      context
          .getBeanFactory()
          .registerSingleton("tableE2eFixtures", new TableE2eFixtures(null, context));
      return;
    }
    Network network = Network.newNetwork();
    MySQLContainer<?> mysql =
        new MySQLContainer<>("mysql:8.4.11")
            .withNetwork(network)
            .withNetworkAliases("mysql")
            .withDatabaseName("oh_db")
            .withUsername("oh_user")
            .withPassword("oh_password");
    GenericContainer<?> hts =
        new GenericContainer<>("eclipse-temurin:17-jre")
            .withNetwork(network)
            .withExposedPorts(8080)
            .withCopyFileToContainer(
                MountableFile.forHostPath(System.getProperty("tableE2eHtsJar")), "/app/hts.jar")
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("cluster-test-properties.yaml"),
                "/var/config/cluster.yaml")
            .withEnv("HTS_DB_USER", mysql.getUsername())
            .withEnv("HTS_DB_PASSWORD", mysql.getPassword())
            .withCommand(
                "java",
                "-Xmx384m",
                "-jar",
                "/app/hts.jar",
                "--cluster.housetables.database.type=MYSQL",
                "--cluster.housetables.database.url=jdbc:mysql://mysql:3306/oh_db?allowPublicKeyRetrieval=true&useSSL=false")
            .waitingFor(Wait.forHttp("/hts/tables/query").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(3));
    Runnable close =
        () -> {
          try {
            hts.stop();
          } finally {
            try {
              mysql.stop();
            } finally {
              network.close();
            }
          }
        };
    try {
      mysql.start();
      try (Connection connection = mysql.createConnection("");
          Statement statement = connection.createStatement()) {
        String[] ddlFiles = {"0000__baseline.sql", "0001__add_entity_type_to_user_table_row.sql"};
        for (String ddl : ddlFiles) {
          String sql =
              new String(
                  java.nio.file.Files.readAllBytes(
                      Paths.get(System.getProperty("tableE2eDdl"), ddl)),
                  java.nio.charset.StandardCharsets.UTF_8);
          for (String command : sql.replaceAll("(?m)--.*$", "").split(";")) {
            if (!command.trim().isEmpty()) {
              statement.execute(command);
            }
          }
        }
        // Keep filesystem fixtures inside the checkout even when its absolute path is long.
        statement.execute(
            "ALTER TABLE user_table_row MODIFY metadata_location VARCHAR(1024), "
                + "MODIFY table_version VARCHAR(1024)");
      }
      hts.start();
      TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
          context,
          "cluster.housetables.base-uri=http://" + hts.getHost() + ":" + hts.getMappedPort(8080));
      context
          .getBeanFactory()
          .registerSingleton("tableE2eFixtures", new TableE2eFixtures(mysql, context));
      ((DefaultListableBeanFactory) context.getBeanFactory())
          .registerDisposableBean("tableE2eContainers", close::run);
    } catch (Throwable exception) {
      close.run();
      throw new IllegalStateException(
          "Docker HTS e2e startup failed; Docker is required (or explicitly select -PtableE2eBackend=h2)",
          exception);
    }
  }
}
