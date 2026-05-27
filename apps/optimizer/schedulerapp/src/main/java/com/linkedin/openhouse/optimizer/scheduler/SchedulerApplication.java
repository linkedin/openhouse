package com.linkedin.openhouse.optimizer.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Entry point for the Optimizer Scheduler application.
 *
 * <p>Spring Batch–style: implements {@link CommandLineRunner} so the work runs after context
 * startup, and {@link ExitCodeGenerator} so the JVM exit code reflects batch outcome. {@code
 * SpringApplication.exit(...)} closes the context (triggers {@code @PreDestroy} hooks, drains the
 * JPA pool, etc.) so the k8s CronJob pod terminates cleanly with a status reflecting reality.
 */
@Slf4j
@SpringBootApplication
@EntityScan(basePackages = "com.linkedin.openhouse.optimizer.db")
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.optimizer.repository")
public class SchedulerApplication implements CommandLineRunner, ExitCodeGenerator {

  private final SchedulerRunner runner;
  private final Map<OperationTypeDto, BinPacker> binPackers;
  private int exitCode = 0;

  @Autowired
  public SchedulerApplication(SchedulerRunner runner, Map<OperationTypeDto, BinPacker> binPackers) {
    this.runner = runner;
    this.binPackers = binPackers;
  }

  public static void main(String[] args) {
    System.exit(SpringApplication.exit(SpringApplication.run(SchedulerApplication.class, args)));
  }

  /**
   * Runs the scheduler once per registered {@link BinPacker} per process invocation. Each call is
   * scoped to one operation type. Any thrown exception is logged and surfaces as a non-zero exit
   * code via {@link #getExitCode()} after the context is shut down cleanly.
   */
  @Override
  public void run(String... args) {
    try {
      log.info("Scheduler starting; operation types: {}", binPackers.keySet());
      binPackers.keySet().forEach(runner::schedule);
      log.info("Scheduler completed successfully");
    } catch (Exception e) {
      log.error("Scheduler failed", e);
      exitCode = 1;
    }
  }

  @Override
  public int getExitCode() {
    return exitCode;
  }
}
