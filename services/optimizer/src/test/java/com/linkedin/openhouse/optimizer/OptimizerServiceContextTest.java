package com.linkedin.openhouse.optimizer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Validates that the Spring application context loads successfully against the H2 schema. This test
 * exercises schema-SQL-init, JPA entity scanning, and repository wiring.
 */
@SpringBootTest
@ActiveProfiles("test")
class OptimizerServiceContextTest {

  @Test
  void contextLoads() {
    // Context load is the assertion — no additional assertions needed.
  }
}
