package com.linkedin.openhouse;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SuppressWarnings({"PMD", "checkstyle:hideutilityclassconstructor"})
@SpringBootApplication(
    scanBasePackages = {
      "com.linkedin.openhouse.housetables",
      "com.linkedin.openhouse.common.audit",
      "com.linkedin.openhouse.common.exception",
      "com.linkedin.openhouse.cluster.configs",
      "com.linkedin.openhouse.cluster.storage"
    })
public class HouseTablesSpringApplication {

  public static void main(String[] args) {
    SpringApplication.run(HouseTablesSpringApplication.class, args);
  }

  /** Need a separate bean to only surface this API document internally. */
  @Bean
  public OpenAPI houseTablesOpenAPI() {
    return new OpenAPI()
        .info(
            new Info()
                .title("House Tables API")
                .description("API description for House Tables API")
                .termsOfService("http://swagger.io/terms")
                .version("v0.0.1")
                .license(new License().name("Apache 2.0").url("http://springdoc.org")));
  }
}
