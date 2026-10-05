package com.linkedin.openhouse.optimizer.analyzer;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Entry point for the Optimizer Analyzer application — the scheduled (k8s CronJob) execution mode
 * that runs a full-database scan per registered {@link OperationAnalyzer}. The complementary
 * commit-driven mode runs inside the Optimizer Service (per-table, triggered on stats upsert) and
 * does not go through this app.
 *
 * <p>Spring Batch–style, mirroring {@code SchedulerApplication} implements {@link
 * CommandLineRunner} so the work runs after context startup, and {@link ExitCodeGenerator} so the
 * JVM exit code reflects batch outcome. {@code SpringApplication.exit(...)} closes the context
 * (triggers {@code @PreDestroy} hooks, drains the JPA pool, etc.) so the CronJob pod terminates
 * cleanly with a status reflecting reality.
 *
 * <p>Unlike the scheduler, analyzers are isolated: a failure in one operation type's full scan is
 * logged and the remaining analyzers still run, but the batch still exits non-zero so the CronJob
 * surfaces the failure.
 */
@Slf4j
@SpringBootApplication
@EntityScan(basePackages = "com.linkedin.openhouse.optimizer.db")
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.optimizer.repository")
public class AnalyzerApplication implements CommandLineRunner, ExitCodeGenerator {

  private final AnalyzerRunner runner;
  private final List<OperationAnalyzer> analyzers;
  private int exitCode = 0;

  @Autowired
  public AnalyzerApplication(AnalyzerRunner runner, List<OperationAnalyzer> analyzers) {
    this.runner = runner;
    this.analyzers = analyzers;
  }

  public static void main(String[] args) {
    System.exit(SpringApplication.exit(SpringApplication.run(AnalyzerApplication.class, args)));
  }

  /**
   * Runs the analyzer once per registered {@link OperationAnalyzer} per process invocation. Each
   * call is scoped to one operation type; the runner iterates databases internally. A failure in
   * one analyzer is logged and does not abort the others, but sets a non-zero exit code that
   * surfaces after the context is shut down cleanly via {@link #getExitCode()}.
   */
  @Override
  public void run(String... args) {
    log.info(
        "Analyzer starting; operation types: {}",
        analyzers.stream().map(OperationAnalyzer::getOperationType).toList());
    for (OperationAnalyzer analyzer : analyzers) {
      try {
        runner.analyze(analyzer.getOperationType());
      } catch (Exception e) {
        log.error("Analyzer failed for operation type {}", analyzer.getOperationType(), e);
        exitCode = 1;
      }
    }
    log.info("Analyzer completed with exit code {}", exitCode);
  }

  @Override
  public int getExitCode() {
    return exitCode;
  }
}
