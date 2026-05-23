package com.linkedin.openhouse.optimizer.analyzer;

import java.util.List;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** Entry point for the Optimizer Analyzer application. */
@SpringBootApplication
@EntityScan(basePackages = "com.linkedin.openhouse.optimizer.db")
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.optimizer.repository")
public class AnalyzerApplication {

  public static void main(String[] args) {
    SpringApplication.run(AnalyzerApplication.class, args);
  }

  /**
   * Runs the analyzer once per registered {@link OperationAnalyzer} per process invocation. Each
   * call is scoped to one operation type; the runner iterates databases internally.
   */
  @Bean
  public CommandLineRunner run(AnalyzerRunner runner, List<OperationAnalyzer> analyzers) {
    return args -> analyzers.forEach(a -> runner.analyze(a.getOperationType()));
  }
}
