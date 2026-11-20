package com.linkedin.openhouse.testinfra.mysql;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/** Selects the real HTS backend only in repository test runtimes that include this module. */
public final class MySqlHtsEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {
  private static final String FIXTURE_APPLICATION =
      "com.linkedin.openhouse.tablestest.SpringH2TestApplication";
  private static final String DATASOURCE_AUTO_CONFIGURATION =
      "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration";
  private static final String CLIENT_HTTP_CONNECTOR_AUTO_CONFIGURATION =
      "org.springframework.boot.autoconfigure.web.reactive.function.client.ClientHttpConnectorAutoConfiguration";

  @Override
  public void postProcessEnvironment(
      ConfigurableEnvironment environment, SpringApplication application) {
    boolean fixtureApplication =
        application.getAllSources().stream()
            .anyMatch(
                source ->
                    source instanceof Class
                        && FIXTURE_APPLICATION.equals(((Class<?>) source).getName()));
    if (!fixtureApplication) {
      return;
    }

    Map<String, Object> properties = new LinkedHashMap<>();
    properties.put("cluster.housetables.base-uri", MySqlHtsCluster.getOrStart().getHtsUri());
    // Without a DataSource, Spring Data does not register the fixture's @Primary H2 repositories.
    // The existing production HouseTableRepositoryImpl and ToggleStatusesRepositoryImpl use HTS.
    // The test JVM shares HTTP clients across contexts. A context-local ReactorResourceFactory
    // must not dispose their JVM-global Netty resources when a sibling context closes.
    String backendExclusions =
        DATASOURCE_AUTO_CONFIGURATION + "," + CLIENT_HTTP_CONNECTOR_AUTO_CONFIGURATION;
    String exclusions = environment.getProperty("spring.autoconfigure.exclude", "");
    properties.put(
        "spring.autoconfigure.exclude",
        exclusions.isEmpty() ? backendExclusions : exclusions + "," + backendExclusions);
    environment
        .getPropertySources()
        .addFirst(new MapPropertySource("openhouse-mysql-test-backend", properties));
  }

  @Override
  public int getOrder() {
    // Read configured exclusions after Spring Boot has loaded application properties.
    return Ordered.LOWEST_PRECEDENCE;
  }
}
