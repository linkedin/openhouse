package com.linkedin.openhouse.cluster.configs;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.springframework.util.ClassUtils;
import org.springframework.web.servlet.HandlerInterceptor;

public class ClusterPropertiesUtil {
  public static Optional<HandlerInterceptor> getClusterSecurityTokenInterceptor(
      ClusterProperties clusterProperties) {
    Optional<Constructor<?>> optionalCons = Optional.empty();

    try {
      Class<?> coordinatorClass =
          ClassUtils.resolveClassName(
              clusterProperties.getClusterSecurityTokenInterceptorClassname(), null);
      optionalCons = Optional.ofNullable(ClassUtils.getConstructorIfAvailable(coordinatorClass));
    } catch (IllegalArgumentException ignored) {
    }
    return optionalCons.map(
        cons -> {
          try {
            return (HandlerInterceptor) cons.newInstance();
          } catch (InstantiationException
              | IllegalAccessException
              | IllegalArgumentException
              | InvocationTargetException
              | ClassCastException e) {
            throw new RuntimeException(
                "Unable to install the configured Request Interception Handler", e);
          }
        });
  }
}
