package com.linkedin.openhouse.analyzer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** Entry point for the Optimizer Analyzer application. */
@SpringBootApplication
@EntityScan(basePackages = "com.linkedin.openhouse.optimizer.entity")
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.optimizer.repository")
public class AnalyzerApplication {

  public static void main(String[] args) {
    SpringApplication.run(AnalyzerApplication.class, args);
  }

  /** Delegates to {@link AnalyzerRunner#analyze()} once per process invocation. */
  @Bean
  public CommandLineRunner run(AnalyzerRunner runner) {
    return args -> runner.analyze();
  }
}
