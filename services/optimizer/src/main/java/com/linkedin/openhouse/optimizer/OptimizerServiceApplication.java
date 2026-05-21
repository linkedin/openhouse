package com.linkedin.openhouse.optimizer;

import com.linkedin.openhouse.optimizer.api.spec.ApiListLimitProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/** Spring Boot entry point for the Optimizer Service. */
@SpringBootApplication
@EnableConfigurationProperties(ApiListLimitProperties.class)
public class OptimizerServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(OptimizerServiceApplication.class, args);
  }
}
