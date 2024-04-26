package com.linkedin.openhouse.housetables.e2e.togglerule;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.api.spec.model.ToggleStatusEnum;
import com.linkedin.openhouse.housetables.services.ToggleStatusesService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.jdbc.Sql;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@Sql({"/schema.sql", "/data.sql"})
public class ToggleStatusesServiceTest {
  @Autowired ToggleStatusesService toggleStatusesService;

  @Test
  public void testActivatedTableForDummy() {
    Assertions.assertEquals(
        toggleStatusesService.getTableToggleStatus("demo", "demodb", "demotable").getStatus(),
        ToggleStatusEnum.ACTIVE);
    Assertions.assertEquals(
        toggleStatusesService.getTableToggleStatus("dummy1", "db", "tbl").getStatus(),
        ToggleStatusEnum.ACTIVE);
    Assertions.assertEquals(
        toggleStatusesService.getTableToggleStatus("dummy1", "db", "testtbl1").getStatus(),
        ToggleStatusEnum.ACTIVE);
    Assertions.assertEquals(
        toggleStatusesService.getTableToggleStatus("dummy2", "db", "tbl").getStatus(),
        ToggleStatusEnum.ACTIVE);
    Assertions.assertEquals(
        toggleStatusesService.getTableToggleStatus("dummy2", "db", "testtbl1").getStatus(),
        ToggleStatusEnum.INACTIVE);
  }
}
