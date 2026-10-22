package com.linkedin.openhouse.optimizer.testing;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs a Spring test context on a new MySQL server in Docker: starts the container before the
 * context refreshes, points the optimizer's datasource at it, and stops it when the context closes.
 * Testcontainers removes the container if the JVM dies first. Add it to a {@code @SpringBootTest}
 * with {@code @ContextConfiguration(initializers = MySqlContainerInitializer.class)}; each distinct
 * test context gets a server of its own.
 */
public class MySqlContainerInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  /** The MySQL image the tests run on: MySQL 8.0, the release line production runs. */
  public static final DockerImageName MYSQL_IMAGE = DockerImageName.parse("mysql:8.0.46");

  /**
   * How long a test waits for MySQL to answer a request before failing. Connector/J has no read
   * timeout by default, so a server that accepts the connection and never answers would hang the
   * build.
   */
  public static final int SOCKET_TIMEOUT_MILLIS = 60_000;

  /**
   * A container for {@link #MYSQL_IMAGE} whose JDBC URL sets {@link #SOCKET_TIMEOUT_MILLIS}, so the
   * startup check and the connections made from {@code getJdbcUrl()} fail instead of hanging.
   */
  public static MySQLContainer<?> newContainer() {
    return new MySQLContainer<>(MYSQL_IMAGE)
        .withUrlParam("socketTimeout", String.valueOf(SOCKET_TIMEOUT_MILLIS));
  }

  @Override
  public void initialize(ConfigurableApplicationContext context) {
    MySQLContainer<?> mysql = newContainer();
    mysql.start();
    context.addApplicationListener(new StopOnClose(mysql));
    TestPropertyValues.of(
            "cluster.optimizer.database.url=" + mysql.getJdbcUrl(),
            "spring.datasource.username=" + mysql.getUsername(),
            "spring.datasource.password=" + mysql.getPassword())
        .applyTo(context);
  }

  private static final class StopOnClose implements ApplicationListener<ContextClosedEvent> {

    private final MySQLContainer<?> mysql;

    private StopOnClose(MySQLContainer<?> mysql) {
      this.mysql = mysql;
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
      mysql.stop();
    }
  }
}
