package com.linkedin.openhouse.optimizer;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Minimal Spring Boot application for {@code :services:optimizer:common} tests. Lets the repository
 * {@code @SpringBootTest}s bootstrap a JPA context (entity scan + Spring Data repositories under
 * {@code com.linkedin.openhouse.optimizer}) against the H2 schema, independent of the service app.
 */
@SpringBootApplication
public class OptimizerCommonTestApplication {}
