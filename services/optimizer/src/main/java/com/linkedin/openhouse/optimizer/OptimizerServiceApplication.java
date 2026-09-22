package com.linkedin.openhouse.optimizer;

import com.linkedin.openhouse.optimizer.config.OptimizerPersistenceConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/** Spring Boot entry point for the Optimizer Service. */
@SpringBootApplication
@Import(OptimizerPersistenceConfiguration.class)
public class OptimizerServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(OptimizerServiceApplication.class, args);
  }
}
