package com.linkedin.openhouse.tables.mock.properties;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.configs.ClusterPropertiesUtil;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.servlet.HandlerInterceptor;

@SpringBootTest
public class ClusterPropertiesTest {

  @Autowired private ClusterProperties clusterProperties;

  private ClusterProperties spiedClusterProperties;

  @BeforeEach
  void setup() {
    spiedClusterProperties = Mockito.spy(clusterProperties);
  }

  @Test
  public void testValidAuthProperties() {
    Mockito.when(spiedClusterProperties.getClusterSecurityTokenInterceptorClassname())
        .thenReturn("com.linkedin.openhouse.common.security.DummyTokenInterceptor");
    Optional<HandlerInterceptor> handlerInterceptor =
        ClusterPropertiesUtil.getClusterSecurityTokenInterceptor(spiedClusterProperties);
    Assertions.assertTrue(handlerInterceptor.isPresent());
  }

  @Test
  public void testInvalidMissingClassAuthProperties() {
    Mockito.when(spiedClusterProperties.getClusterSecurityTokenInterceptorClassname())
        .thenReturn("this.doesnt.exist");
    Optional<HandlerInterceptor> handlerInterceptor =
        ClusterPropertiesUtil.getClusterSecurityTokenInterceptor(spiedClusterProperties);
    Assertions.assertFalse(handlerInterceptor.isPresent());
  }

  @Test
  public void testInvalidConstructorAuthProperties() {
    Mockito.when(spiedClusterProperties.getClusterSecurityTokenInterceptorClassname())
        .thenReturn(
            "com.linkedin.openhouse.tables.mock.properties.InvalidHandlerInterceptorResource");
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          ClusterPropertiesUtil.getClusterSecurityTokenInterceptor(spiedClusterProperties);
        });
  }
}
