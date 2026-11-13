package com.linkedin.openhouse.testinfra.mysql;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/** One isolated HTS and MySQL server shared by the embedded Tables contexts in a test JVM. */
final class MySqlHtsCluster implements AutoCloseable {
  private static MySqlHtsCluster instance;

  private final Network network;
  private final MySQLContainer<?> mysql;
  private final GenericContainer<?> hts;

  private MySqlHtsCluster(Path htsJar) {
    network = Network.newNetwork();
    mysql =
        new MySQLContainer<>(DockerImageName.parse("mysql:8.0.46"))
            .withDatabaseName("openhouse_test")
            .withUrlParam("socketTimeout", "60000")
            .withNetwork(network)
            .withNetworkAliases("mysql");
    hts =
        new GenericContainer<>(DockerImageName.parse("eclipse-temurin:17-jre"))
            .withNetwork(network)
            .withCopyFileToContainer(MountableFile.forHostPath(htsJar), "/hts.jar")
            .withEnv("HTS_DB_USER", mysql.getUsername())
            .withEnv("HTS_DB_PASSWORD", mysql.getPassword())
            .withCommand(
                "java",
                "-Xmx512m",
                "-jar",
                "/hts.jar",
                "--cluster.housetables.database.type=MYSQL",
                "--cluster.housetables.database.url=jdbc:mysql://mysql:3306/openhouse_test"
                    + "?useSSL=false&allowPublicKeyRetrieval=true&socketTimeout=60000",
                "--server.port=8080")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/actuator/health").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(2));
  }

  static synchronized MySqlHtsCluster getOrStart() {
    if (instance == null) {
      String jarProperty = System.getProperty("openhouse.test.hts.jar");
      if (jarProperty == null || !Files.isRegularFile(Paths.get(jarProperty))) {
        throw new IllegalStateException(
            "openhouse.test.hts.jar must point to :services:housetables:bootJar; "
                + "run the test through Gradle's openhouse.mysql-test-backend convention");
      }
      MySqlHtsCluster cluster = new MySqlHtsCluster(Paths.get(jarProperty));
      try {
        cluster.mysql.start();
        cluster.hts.start();
        Runtime.getRuntime()
            .addShutdownHook(new Thread(cluster::close, "openhouse-mysql-test-backend"));
        instance = cluster;
      } catch (RuntimeException | Error failure) {
        cluster.close();
        throw failure;
      }
    }
    return instance;
  }

  String getHtsUri() {
    return "http://" + hts.getHost() + ":" + hts.getMappedPort(8080);
  }

  @Override
  public void close() {
    try {
      hts.stop();
    } finally {
      try {
        mysql.stop();
      } finally {
        network.close();
      }
    }
  }
}
