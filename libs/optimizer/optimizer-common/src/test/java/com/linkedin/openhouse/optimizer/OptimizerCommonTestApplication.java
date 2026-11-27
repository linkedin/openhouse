package com.linkedin.openhouse.optimizer;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Minimal Spring Boot application for {@code :libs:optimizer:optimizer-common} tests. Lets the
 * repository {@code @SpringBootTest}s bootstrap a JPA context (entity scan + Spring Data
 * repositories under {@code com.linkedin.openhouse.optimizer}) on a MySQL container that Flyway
 * migrated, independent of the service app.
 */
@SpringBootApplication
public class OptimizerCommonTestApplication {}
