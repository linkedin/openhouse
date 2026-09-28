package com.linkedin.openhouse.optimizer.analyzer;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Entry point for the Optimizer Analyzer application.
 *
 * <p>Runs once per process invocation in one of two modes selected by {@code analyzer.mode}
 * (default {@link AnalyzerMode#INCREMENTAL}), then exits. This lets the two paths be scheduled as
 * two independent instances — e.g. two K8s CronJobs, each with {@code concurrencyPolicy: Forbid}:
 *
 * <ul>
 *   <li>{@code analyzer.mode=INCREMENTAL} on a frequent cron (e.g. every 30 min) — only tables
 *       changed since the last run.
 *   <li>{@code analyzer.mode=FULL} on an infrequent cron (e.g. daily) — the whole fleet, so idle
 *       tables and failed-commit tables (orphans, no stats update) are still swept per TTL.
 * </ul>
 */
@Slf4j
@SpringBootApplication
@EntityScan(basePackages = "com.linkedin.openhouse.optimizer.db")
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.optimizer.repository")
public class AnalyzerApplication {

  public static void main(String[] args) {
    SpringApplication.run(AnalyzerApplication.class, args);
  }

  /**
   * Runs every registered {@link OperationAnalyzer} once in the configured mode, then returns so
   * the process exits (this is a batch/CronJob-style app).
   */
  @Bean
  public CommandLineRunner run(
      AnalyzerRunner runner,
      List<OperationAnalyzer> analyzers,
      @Value("${analyzer.mode:INCREMENTAL}") String modeProperty) {
    AnalyzerMode mode = AnalyzerMode.from(modeProperty);
    return args -> {
      long start = System.currentTimeMillis();
      log.info("Analyzer starting in {} mode for {} operation type(s)", mode, analyzers.size());
      for (OperationAnalyzer analyzer : analyzers) {
        if (mode == AnalyzerMode.FULL) {
          runner.analyze(analyzer.getOperationType());
        } else {
          runner.analyzeIncremental(analyzer.getOperationType());
        }
      }
      log.info("Analyzer {} run complete in {} ms", mode, System.currentTimeMillis() - start);
    };
  }
}
