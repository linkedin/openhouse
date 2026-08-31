package com.linkedin.openhouse.housetables.e2e;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(
    basePackages = {
      "com.linkedin.openhouse.housetables.api",
      "com.linkedin.openhouse.housetables.dto.mapper",
      "com.linkedin.openhouse.housetables.controller",
      "com.linkedin.openhouse.housetables.services",
      "com.linkedin.openhouse.common.exception.handler",
      "com.linkedin.openhouse.common.audit",
      "com.linkedin.openhouse.housetables.repository",
      "com.linkedin.openhouse.housetables.properties",
      "com.linkedin.openhouse.housetables.config",
      "com.linkedin.openhouse.cluster.configs",
      "com.linkedin.openhouse.cluster.storage",
      "com.linkedin.openhouse.internal.catalog.mapper"
    })
@EntityScan(basePackages = {"com.linkedin.openhouse.housetables.model"})
public class SpringH2HtsApplication {
  public static void main(String[] args) {
    SpringApplication.run(SpringH2HtsApplication.class, args);
  }
}
