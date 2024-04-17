package com.linkedin.openhouse.housetables.e2e.togglerule;

import static com.linkedin.openhouse.housetables.e2e.togglerule.ToggleStatusesTestConstants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.model.TableToggleRule;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.ToggleStatusHtsJdbcRepository;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class ToggleStatusRepositoryTest {
  @Autowired ToggleStatusHtsJdbcRepository htsRepository;

  @Test
  public void testFindAllByFeature() {
    List<TableToggleRule> toggleRuleList =
        ImmutableList.copyOf(htsRepository.findAllByFeatureId(TEST_RULE_0.getFeature()));
    Assertions.assertEquals(2, toggleRuleList.size());

    // Now there should be 2 rules under dummy2 feature
    htsRepository.save(
        TEST_RULE_0.toBuilder().feature(TEST_RULE_2.getFeature()).databasePattern("dbnew").build());
    toggleRuleList =
        ImmutableList.copyOf(htsRepository.findAllByFeatureId(TEST_RULE_2.getFeature()));
    Assertions.assertEquals(2, toggleRuleList.size());
  }
}
