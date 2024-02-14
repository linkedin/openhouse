package com.linkedin.openhouse.jobs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SuppressWarnings({"PMD", "checkstyle:hideutilityclassconstructor"})
@SpringBootApplication(
    scanBasePackages = {
      "com.linkedin.openhouse.jobs",
      "com.linkedin.openhouse.cluster.configs",
      "com.linkedin.openhouse.cluster.storage",
      "com.linkedin.openhouse.common.audit",
      "com.linkedin.openhouse.common.exception"
    })
@EntityScan(basePackages = {"com.linkedin.openhouse.jobs.model"})
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
public class JobsSpringApplication {

  public static void main(String[] args) {
    SpringApplication.run(JobsSpringApplication.class, args);
  }
}
