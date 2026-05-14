package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationType;
import java.util.Map;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** Entry point for the Optimizer Scheduler application. */
@SpringBootApplication
@EntityScan(basePackages = "com.linkedin.openhouse.optimizer.entity")
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.optimizer.repository")
public class SchedulerApplication {

  public static void main(String[] args) {
    SpringApplication.run(SchedulerApplication.class, args);
  }

  /**
   * Runs the scheduler once per registered {@link BinPacker} per process invocation. Each call is
   * scoped to one operation type.
   */
  @Bean
  public CommandLineRunner run(SchedulerRunner runner, Map<OperationType, BinPacker> binPackers) {
    return args -> binPackers.keySet().forEach(runner::schedule);
  }
}
