package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.tables.toggle.BaseTableFeatureToggle;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class BaseTableFeatureToggleTest {
  @Autowired BaseTableFeatureToggle baseFeatureToggle;

  @Autowired ToggleStatusesRepository toggleStatusesRepository;

  @Test
  public void testDummyToggle() {
    TableToggleStatus rule1 =
        TableToggleStatus.builder()
            .featureId("dummy")
            .databaseId("db1")
            .tableId("tbl1")
            .toggleStatusEnum("ACTIVE")
            .build();
    TableToggleStatus rule2 =
        TableToggleStatus.builder()
            .featureId("dummy")
            .databaseId("db2")
            .tableId("tbl2")
            .toggleStatusEnum("ACTIVE")
            .build();
    TableToggleStatus rule3 =
        TableToggleStatus.builder()
            .featureId("random")
            .databaseId("db1")
            .tableId("tbl3")
            .toggleStatusEnum("ACTIVE")
            .build();
    toggleStatusesRepository.save(rule1);
    toggleStatusesRepository.save(rule2);
    toggleStatusesRepository.save(rule3);

    Assertions.assertTrue(baseFeatureToggle.isFeatureActivated("db1", "tbl1", "dummy"));
    Assertions.assertTrue(baseFeatureToggle.isFeatureActivated("db2", "tbl2", "dummy"));
    Assertions.assertTrue(baseFeatureToggle.isFeatureActivated("db1", "tbl3", "random"));
    /* This is a rule of random, using dummyFeatureToggle should see this deactivated. */
    Assertions.assertFalse(baseFeatureToggle.isFeatureActivated("db1", "tbl3", "dummy"));

    toggleStatusesRepository.deleteAll();
  }
}
