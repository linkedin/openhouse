package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * This class is used if we want to inject beans for define callback methods to customize the
 * Java-based configuration for Spring MVC enabled via @EnableWebMvc. For example, if we need to
 * intercept requests pre- and post- processing of controller.
 * https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/web/servlet/config/annotation/WebMvcConfigurer.html
 */
@Configuration
public class TablesMvcConfigurer implements WebMvcConfigurer {

  @Autowired private ClusterProperties clusterProperties;

  public void addInterceptors(InterceptorRegistry registry) {
    if (clusterProperties.getClusterSecurityTokenInterceptorClassname() != null) {
      Class<?> coordinatorClass =
          ReflectionUtils.loadIfPresent(
              clusterProperties.getClusterSecurityTokenInterceptorClassname(),
              getClass().getClassLoader());
      if (coordinatorClass != null) {
        Optional<Constructor<?>> cons = ReflectionUtils.findConstructor(coordinatorClass);
        if (cons.isPresent()) {
          try {
            registry
                .addInterceptor((HandlerInterceptor) cons.get().newInstance())
                .addPathPatterns("/**")
                .excludePathPatterns("/actuator/**", "/**/api-docs/**");
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(
                "Unable to install the configured Request Interception Handler", e);
          }
        }
      }
    }
  }
}
