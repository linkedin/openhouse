package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.SHARED_TABLE_DTO;
import static com.linkedin.openhouse.tables.model.TableModelConstants.TEST_USER;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.services.DatabasesService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = SpringH2Application.class)
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@Slf4j
public class DatabasesServiceTest {

  @Autowired DatabasesService databasesService;

  @Autowired private ApplicationContext appContext;

  @Test
  public void tmp() {
    try {
      Object bean = appContext.getBean("storageManager");
      log.info("bean: {}", bean);
    } catch (Exception e) {
      log.error("ignore bean");
    }
  }

  @Test
  public void testUpdateAclPoliciesOnDatabase() {
    Assertions.assertDoesNotThrow(
        () ->
            databasesService.updateDatabaseAclPolicies(
                SHARED_TABLE_DTO.getDatabaseId(),
                UpdateAclPoliciesRequestBody.builder()
                    .role("AclEditor")
                    .principal("DUMMY_USER")
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .build(),
                TEST_USER));

    Assertions.assertDoesNotThrow(
        () ->
            databasesService.updateDatabaseAclPolicies(
                SHARED_TABLE_DTO.getDatabaseId(),
                UpdateAclPoliciesRequestBody.builder()
                    .role("AclEditor")
                    .principal("DUMMY_USER")
                    .operation(UpdateAclPoliciesRequestBody.Operation.REVOKE)
                    .build(),
                TEST_USER));
  }
}
