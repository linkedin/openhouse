package com.linkedin.openhouse.optimizer.analyzer;

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
   * Runs a full scan across all registered analyzers and databases once per process invocation. An
   * empty {@link AnalyzeRequest} is the "analyze everything enabled" filter; the runner selects the
   * analyzers and iterates databases internally.
   */
  @Bean
  public CommandLineRunner run(AnalyzerRunner runner) {
    return args -> runner.analyze(AnalyzeRequest.builder().build());
  }
}
