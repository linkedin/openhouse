package com.linkedin.openhouse.housetables.e2e.togglerule;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.api.spec.model.ToggleStatusEnum;
import com.linkedin.openhouse.housetables.e2e.SpringH2HtsApplication;
import com.linkedin.openhouse.housetables.model.TableToggleRule;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.ToggleStatusHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.ToggleStatusesService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = SpringH2HtsApplication.class)
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class ToggleStatusesServiceRuleProvisioningE2ETest {

  @Autowired private ToggleStatusHtsJdbcRepository repository;
  @Autowired private ToggleStatusesService service;

  @Test
  public void newlyInsertedViewsRuleIsImmediatelyVisibleAndDatabaseLiteralIsExact() {
    String feature = "views_e2e_" + System.nanoTime();
    String probe = "views_gate_probe";

    assertEquals(
        ToggleStatusEnum.INACTIVE, service.getTableToggleStatus(feature, "dba", probe).getStatus());

    repository.save(
        TableToggleRule.builder()
            .feature(feature)
            .databasePattern("dba")
            .tablePattern("*")
            .creationTimeMs(System.currentTimeMillis())
            .build());

    assertEquals(
        ToggleStatusEnum.ACTIVE, service.getTableToggleStatus(feature, "dba", probe).getStatus());
    assertEquals(
        ToggleStatusEnum.INACTIVE, service.getTableToggleStatus(feature, "db", probe).getStatus());
    assertEquals(
        ToggleStatusEnum.INACTIVE, service.getTableToggleStatus(feature, "db2", probe).getStatus());
    assertEquals(
        ToggleStatusEnum.INACTIVE, service.getTableToggleStatus(feature, "DbA", probe).getStatus());
  }
}
