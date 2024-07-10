package com.linkedin.openhouse.cluster.configs;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.springframework.util.ClassUtils;
import org.springframework.web.servlet.HandlerInterceptor;

public class ClusterPropertiesUtil {
  public static Optional<HandlerInterceptor> getClusterSecurityTokenInterceptor(
      ClusterProperties clusterProperties) {
    try {
      Class<?> coordinatorClass =
          ClassUtils.resolveClassName(
              clusterProperties.getClusterSecurityTokenInterceptorClassname(), null);
      Optional<Constructor<?>> cons =
          Optional.ofNullable(ClassUtils.getConstructorIfAvailable(coordinatorClass));
      if (cons.isPresent()) {
        try {
          return Optional.of((HandlerInterceptor) cons.get().newInstance());
        } catch (InstantiationException
            | IllegalAccessException
            | IllegalArgumentException
            | InvocationTargetException e) {
          throw new RuntimeException(
              "Unable to install the configured Request Interception Handler", e);
        }
      }
    } catch (IllegalArgumentException ignored) {
    }
    return Optional.empty();
  }
}
