package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.config.OptimizerDatabaseConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Entry point for the Optimizer Analyzer application — the scheduled (k8s CronJob) execution mode
 * that runs a full-database scan across all registered analyzers. The complementary commit-driven
 * mode runs inside the Optimizer Service (per-table, triggered on stats upsert) and does not go
 * through this app.
 *
 * <p>Spring Batch–style, mirroring {@code SchedulerApplication} implements {@link
 * CommandLineRunner} so the work runs after context startup, and {@link ExitCodeGenerator} so the
 * JVM exit code reflects batch outcome. {@code SpringApplication.exit(...)} closes the context
 * (triggers {@code @PreDestroy} hooks, drains the JPA pool, etc.) so the CronJob pod terminates
 * cleanly with a status reflecting reality.
 *
 * <p>The run issues a single empty-filter {@link AnalyzeRequest} ("analyze everything enabled"), so
 * the runner resolves the database set once and iterates all analyzers internally. Any failure
 * surfaces as a non-zero exit code after the context shuts down cleanly.
 */
@Slf4j
@SpringBootApplication
@Import(OptimizerDatabaseConfiguration.class)
@EntityScan(basePackages = "com.linkedin.openhouse.optimizer.db")
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.optimizer.repository")
public class AnalyzerApplication implements CommandLineRunner, ExitCodeGenerator {

  private final AnalyzerRunner runner;
  private int exitCode = 0;

  @Autowired
  public AnalyzerApplication(AnalyzerRunner runner) {
    this.runner = runner;
  }

  public static void main(String[] args) {
    System.exit(SpringApplication.exit(SpringApplication.run(AnalyzerApplication.class, args)));
  }

  /**
   * Runs a full scan across all registered analyzers and databases once per process invocation. An
   * empty {@link AnalyzeRequest} is the "analyze everything enabled" filter; the runner selects the
   * analyzers and iterates databases internally. A failure is logged and sets a non-zero exit code
   * that surfaces after the context is shut down cleanly via {@link #getExitCode()}.
   */
  @Override
  public void run(String... args) {
    log.info("Analyzer starting");
    try {
      runner.analyze(AnalyzeRequest.builder().build());
      log.info("Analyzer completed successfully");
    } catch (Exception e) {
      log.error("Analyzer failed", e);
      exitCode = 1;
    }
  }

  @Override
  public int getExitCode() {
    return exitCode;
  }
}
