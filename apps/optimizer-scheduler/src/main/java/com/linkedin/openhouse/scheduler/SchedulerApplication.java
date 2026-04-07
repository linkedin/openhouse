package com.linkedin.openhouse.scheduler;

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

  @Bean
  public CommandLineRunner run(SchedulerRunner runner) {
    return args -> runner.schedule();
  }
}
