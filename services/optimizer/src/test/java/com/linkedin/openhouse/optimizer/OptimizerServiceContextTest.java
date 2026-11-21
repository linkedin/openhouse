package com.linkedin.openhouse.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.analyzer.CadenceBasedOrphanFilesDeletionAnalyzer;
import com.linkedin.openhouse.optimizer.analyzer.CadenceBasedStatsCollectionAnalyzer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.TestPropertySource;

/** Validates the service's Spring application context: Flyway migrations and bean wiring. */
@SpringBootTest
@TestPropertySource(locations = "classpath:optimizer-mysql-testcontainers.properties")
class OptimizerServiceContextTest {

  @Autowired ApplicationContext context;

  @Test
  void statsCollectionAnalyzerEnabled_orphanFilesDeletionDisabled_byDefault() {
    // Staged rollout: stats collection ships first (analyzer.stats.enabled default true); OFD is
    // gated off (analyzer.ofd.enabled default false) so its bean is not registered.
    assertThat(context.getBeansOfType(CadenceBasedStatsCollectionAnalyzer.class)).isNotEmpty();
    assertThat(context.getBeansOfType(CadenceBasedOrphanFilesDeletionAnalyzer.class)).isEmpty();
  }
}
