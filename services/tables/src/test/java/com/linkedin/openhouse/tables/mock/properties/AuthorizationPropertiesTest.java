package com.linkedin.openhouse.tables.mock.properties;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
public class AuthorizationPropertiesTest {

  @Autowired private ClusterProperties clusterProperties;

  @Test
  public void testAuthPropertyPresent() {
    Assertions.assertEquals(
        "com.linkedin.openhouse.common.security.DummyTokenInterceptor",
        clusterProperties.getClusterSecurityTokenInterceptorClassname());
  }
}
