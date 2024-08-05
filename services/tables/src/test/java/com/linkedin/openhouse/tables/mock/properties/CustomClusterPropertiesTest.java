package com.linkedin.openhouse.tables.mock.properties;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import java.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
public class CustomClusterPropertiesTest {

  @Autowired private ClusterProperties clusterProperties;

  /**
   * StorageManager validates storage properties, the 'cluster-test-properties.yaml' contains
   * invalid storage type called "objectstore" for testing.
   */
  @MockBean private StorageManager storageManager;

  @Test
  public void testClusterProperties() {
    Assertions.assertEquals("TestCluster", clusterProperties.getClusterName());
  }

  @Test
  public void testClusterPropertiesTableAllowedClientNameValues() {
    Assertions.assertEquals(
        Arrays.asList("trino", "spark"), clusterProperties.getAllowedClientNameValues());
  }

  @AfterAll
  static void unsetSysProp() {
    System.clearProperty("OPENHOUSE_CLUSTER_CONFIG_PATH");
  }
}
