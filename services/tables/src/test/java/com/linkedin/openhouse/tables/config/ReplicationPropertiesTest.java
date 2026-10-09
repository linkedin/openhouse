package com.linkedin.openhouse.tables.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

public class ReplicationPropertiesTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(TestConfiguration.class);

  @Test
  public void bindsDynamicPeerIdsFromConfiguration() {
    contextRunner
        .withPropertyValues(
            "cluster.replication.peers.clusterB.tables-api-base-uri=http://tables-b:8080",
            "cluster.replication.peers.clusterC.tables-api-base-uri=https://tables-c.example:8443")
        .run(
            context -> {
              ReplicationProperties properties = context.getBean(ReplicationProperties.class);
              Assertions.assertEquals(
                  "http://tables-b:8080",
                  properties.getPeers().get("clusterB").getTablesApiBaseUri());
              Assertions.assertEquals(
                  "https://tables-c.example:8443",
                  properties.getPeers().get("clusterC").getTablesApiBaseUri());
            });
  }

  @Test
  public void rejectsPeerWithoutTablesApiBaseUri() {
    contextRunner
        .withPropertyValues("cluster.replication.peers.clusterB.tables-api-base-uri=")
        .run(context -> Assertions.assertTrue(context.getStartupFailure() != null));
  }

  @Test
  public void rejectsNonHttpPeerUri() {
    contextRunner
        .withPropertyValues(
            "cluster.replication.peers.clusterB.tables-api-base-uri=file:///tmp/tables")
        .run(context -> Assertions.assertTrue(context.getStartupFailure() != null));
  }

  @Configuration
  @EnableConfigurationProperties(ReplicationProperties.class)
  static class TestConfiguration {}
}
