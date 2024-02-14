package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.authorization.AuthorizationInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.Advisor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Role;
import org.springframework.security.authorization.method.AuthorizationInterceptorsOrder;
import org.springframework.security.authorization.method.AuthorizationManagerBeforeMethodInterceptor;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;

/**
 * Class to expose authorization specific beans. Here are the two beans we need 1. {@link
 * AuthorizationManagerBeforeMethodInterceptor} bean that allows OpenHouse Tables Authorization
 * Interceptor to be called in before {@link org.springframework.security.access.annotation.Secured}
 * method. 2. TODO: TablesAuthorizationHandler bean that allows (1) to call a custom Authorization
 * Handler that could be based on AAS, OPA, etc.
 */
@EnableMethodSecurity
@Slf4j
public class AuthorizationConfig {

  @Autowired private ClusterProperties clusterProperties;

  /**
   * Inject a bean that allows for method interception for methods annotated with {@link
   * org.springframework.security.access.annotation.Secured}. Note, we don't enable the default bean
   * for {@link org.springframework.security.access.annotation.Secured}
   *
   * @return {@Advisor} Action to take at the Joinpoint, i.e. method to call at the site of
   *     annotation.
   */
  @Bean
  @ConditionalOnProperty(
      value = "cluster.security.tables.authorization.enabled",
      havingValue = "true")
  @Role(BeanDefinition.ROLE_APPLICATION)
  public Advisor customAuthorizationInterceptor() {
    log.info(
        "Authorization is enabled {}",
        clusterProperties.isClusterSecurityTablesAuthorizationEnabled());
    AuthorizationManagerBeforeMethodInterceptor authorizationManagerBeforeMethodInterceptor =
        new AuthorizationManagerBeforeMethodInterceptor(
            AuthorizationManagerBeforeMethodInterceptor.secured().getPointcut(),
            new AuthorizationInterceptor());
    authorizationManagerBeforeMethodInterceptor.setOrder(
        AuthorizationInterceptorsOrder.SECURED.getOrder());
    return authorizationManagerBeforeMethodInterceptor;
  }
}
