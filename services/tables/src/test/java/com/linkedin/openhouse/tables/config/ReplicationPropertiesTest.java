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
            "cluster.replication.peers.clusterB.cascade-signing-key=01234567890123456789012345678901",
            "cluster.replication.peers.clusterC.tables-api-base-uri=https://tables-c.example:8443",
            "cluster.replication.peers.clusterC.cascade-signing-key=abcdefghijklmnopqrstuvwxyz123456")
        .run(
            context -> {
              ReplicationProperties properties = context.getBean(ReplicationProperties.class);
              Assertions.assertEquals(
                  ReplicationProperties.CascadeMode.SPARK, properties.getCascadeMode());
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
        .withPropertyValues(
            "cluster.replication.peers.clusterB.tables-api-base-uri=",
            "cluster.replication.peers.clusterB.cascade-signing-key=01234567890123456789012345678901")
        .run(context -> Assertions.assertTrue(context.getStartupFailure() != null));
  }

  @Test
  public void bindsServiceCascadeMode() {
    contextRunner
        .withPropertyValues("cluster.replication.cascade-mode=service")
        .run(
            context ->
                Assertions.assertEquals(
                    ReplicationProperties.CascadeMode.SERVICE,
                    context.getBean(ReplicationProperties.class).getCascadeMode()));
  }

  @Test
  public void missingReplicationConfigurationDefaultsToSparkMode() {
    contextRunner.run(
        context -> {
          Assertions.assertNull(context.getStartupFailure());
          ReplicationProperties properties = context.getBean(ReplicationProperties.class);
          Assertions.assertEquals(
              ReplicationProperties.CascadeMode.SPARK, properties.getCascadeMode());
          Assertions.assertTrue(properties.getPeers().isEmpty());
        });
  }

  @Test
  public void rejectsNonHttpPeerUri() {
    contextRunner
        .withPropertyValues(
            "cluster.replication.peers.clusterB.tables-api-base-uri=file:///tmp/tables",
            "cluster.replication.peers.clusterB.cascade-signing-key=01234567890123456789012345678901")
        .run(context -> Assertions.assertTrue(context.getStartupFailure() != null));
  }

  @Test
  public void rejectsShortCascadeSigningKey() {
    contextRunner
        .withPropertyValues(
            "cluster.replication.peers.clusterB.tables-api-base-uri=http://tables-b:8080",
            "cluster.replication.peers.clusterB.cascade-signing-key=short")
        .run(context -> Assertions.assertTrue(context.getStartupFailure() != null));
  }

  @Configuration
  @EnableConfigurationProperties(ReplicationProperties.class)
  static class TestConfiguration {}
}
