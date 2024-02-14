package com.linkedin.openhouse.tables.mock.properties;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = DefaultClusterPropertiesInitializer.class)
public class DefaultClusterPropertiesTest {

  @Autowired private ClusterProperties clusterProperties;

  @Test
  public void testClusterProperties() {
    Assertions.assertEquals("local-cluster", clusterProperties.getClusterName());
  }

  @AfterAll
  static void unsetSysProp() {
    System.clearProperty("OPENHOUSE_CLUSTER_CONFIG_PATH");
  }
}
