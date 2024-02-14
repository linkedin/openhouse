package com.linkedin.openhouse.jobs.controller;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {
  @Bean
  public OpenAPI houseJobsOpenAPI() {
    return new OpenAPI()
        .info(
            new Info()
                .title("OpenHouse APIs")
                .description("API description for OpenHouse API")
                .termsOfService("http://swagger.io/terms")
                .version("v0.0.1")
                .license(new License().name("Apache 2.0").url("http://springdoc.org")));
  }
}
