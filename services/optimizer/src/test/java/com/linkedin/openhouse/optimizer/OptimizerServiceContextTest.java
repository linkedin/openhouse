package com.linkedin.openhouse.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Validates that the Spring application context loads successfully against the H2 schema. This test
 * exercises schema-SQL-init, JPA entity scanning, and repository wiring.
 */
@SpringBootTest
@ActiveProfiles("test")
class OptimizerServiceContextTest {

  @Autowired ApplicationContext context;

  @Test
  void contextLoads() {
    assertThat(context).isNotNull();
  }
}
