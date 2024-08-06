package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.configs.ClusterPropertiesUtil;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
  OpenAPI openAPI =
      new OpenAPI()
          .info(
              new Info()
                  .title("OpenHouse Tables APIs")
                  .description("API description for OpenHouse Tables API")
                  .termsOfService("http://swagger.io/terms")
                  .version("v0.1")
                  .license(new License().name("Apache 2.0").url("http://springdoc.org")));

  public void addInterceptors(InterceptorRegistry registry) {
    Optional<HandlerInterceptor> handlerInterceptor =
        ClusterPropertiesUtil.getClusterSecurityTokenInterceptor(clusterProperties);
    handlerInterceptor.ifPresent(
        interceptor -> {
          registry
              .addInterceptor(interceptor)
              .addPathPatterns("/**")
              .excludePathPatterns(
                  "/actuator/**", "/**/api-docs/**", "/**/swagger-ui/**", "/favicon.ico", "/error");
          addOpenAPISecurityScheme();
        });
  }

  @Bean
  public OpenAPI houseTablesOpenAPI() {
    return openAPI;
  }

  private void addOpenAPISecurityScheme() {
    // docs:
    // https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#security-requirement-object
    final String securitySchemaName = "BearerAuth";
    final SecurityRequirement securityRequirement =
        new SecurityRequirement().addList(securitySchemaName);
    final SecurityScheme securityScheme =
        new SecurityScheme().type(SecurityScheme.Type.HTTP).bearerFormat("JWT").scheme("bearer");

    openAPI
        .addSecurityItem(securityRequirement)
        .schemaRequirement(securitySchemaName, securityScheme);
  }
}
