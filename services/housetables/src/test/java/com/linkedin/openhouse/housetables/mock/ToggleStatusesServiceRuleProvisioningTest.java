package com.linkedin.openhouse.housetables.mock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.housetables.api.spec.model.ToggleStatusEnum;
import com.linkedin.openhouse.housetables.model.TableToggleRule;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.ToggleStatusHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.ToggleStatusesServiceImpl;
import com.linkedin.openhouse.housetables.services.WildcardTableToggleRuleMatcher;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

public class ToggleStatusesServiceRuleProvisioningTest {

  @Test
  public void missingViewsRuleDefaultsInactive() {
    ToggleStatusHtsJdbcRepository repository = Mockito.mock(ToggleStatusHtsJdbcRepository.class);
    when(repository.findAllByFeature("views")).thenReturn(Collections.emptyList());

    assertEquals(
        ToggleStatusEnum.INACTIVE,
        service(repository).getTableToggleStatus("views", "dba", "views_gate_probe").getStatus());
  }

  @Test
  public void lowerCaseLiteralDatabaseRuleIsExactAndImmediatelyVisible() {
    ToggleStatusHtsJdbcRepository repository = Mockito.mock(ToggleStatusHtsJdbcRepository.class);
    when(repository.findAllByFeature("views"))
        .thenReturn(
            Collections.singletonList(
                TableToggleRule.builder()
                    .feature("views")
                    .databasePattern("db")
                    .tablePattern("*")
                    .build()));

    ToggleStatusesServiceImpl service = service(repository);

    assertEquals(
        ToggleStatusEnum.ACTIVE,
        service.getTableToggleStatus("views", "db", "views_gate_probe").getStatus());
    assertEquals(
        ToggleStatusEnum.INACTIVE,
        service.getTableToggleStatus("views", "db2", "views_gate_probe").getStatus());
  }

  @Test
  public void ruleMatcherIsCaseSensitiveSoCallersMustCanonicalizeDatabaseIds() {
    ToggleStatusHtsJdbcRepository repository = Mockito.mock(ToggleStatusHtsJdbcRepository.class);
    when(repository.findAllByFeature("views"))
        .thenReturn(
            Collections.singletonList(
                TableToggleRule.builder()
                    .feature("views")
                    .databasePattern("dba")
                    .tablePattern("*")
                    .build()));

    ToggleStatusesServiceImpl service = service(repository);

    assertEquals(
        ToggleStatusEnum.ACTIVE,
        service.getTableToggleStatus("views", "dba", "views_gate_probe").getStatus());
    assertEquals(
        ToggleStatusEnum.INACTIVE,
        service.getTableToggleStatus("views", "DbA", "views_gate_probe").getStatus());
  }

  private static ToggleStatusesServiceImpl service(ToggleStatusHtsJdbcRepository repository) {
    ToggleStatusesServiceImpl service = new ToggleStatusesServiceImpl();
    ReflectionTestUtils.setField(service, "htsRepository", repository);
    ReflectionTestUtils.setField(service, "ruleMatcher", new WildcardTableToggleRuleMatcher());
    return service;
  }
}
