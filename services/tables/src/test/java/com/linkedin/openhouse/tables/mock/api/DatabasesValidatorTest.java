package com.linkedin.openhouse.tables.mock.api;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.validator.DatabasesApiValidator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

@SpringBootTest
@Slf4j
public class DatabasesValidatorTest {

  @Autowired private DatabasesApiValidator databasesApiValidator;

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
  public void validateUpdateAclPoliciesSpecialCharacter() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            databasesApiValidator.validateUpdateAclPolicies(
                "%%",
                UpdateAclPoliciesRequestBody.builder()
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .principal("DUMMY_USER")
                    .role("AclEditor")
                    .build()));
  }

  @Test
  public void validateUpdateAclPoliciesEmptyDatabaseId() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            databasesApiValidator.validateUpdateAclPolicies(
                "",
                UpdateAclPoliciesRequestBody.builder()
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .principal("DUMMY_USER")
                    .role("AclEditor")
                    .build()));
  }

  @Test
  public void validateUpdateAclPoliciesEmptyValues() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            databasesApiValidator.validateUpdateAclPolicies(
                "d",
                UpdateAclPoliciesRequestBody.builder()
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .principal("")
                    .role("AclEditor")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            databasesApiValidator.validateUpdateAclPolicies(
                "d",
                UpdateAclPoliciesRequestBody.builder()
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .principal("DUMMY_USER")
                    .role("")
                    .build()));
  }

  @Test
  public void validateUpdateAclPoliciesSuccess() {
    assertDoesNotThrow(
        () ->
            databasesApiValidator.validateUpdateAclPolicies(
                "d",
                UpdateAclPoliciesRequestBody.builder()
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .principal("DUMMY_USER")
                    .role("AclEditor")
                    .build()));
  }

  @Test
  public void validateGetAclPoliciesInvalidValues() {
    assertThrows(
        RequestValidationFailureException.class,
        () -> databasesApiValidator.validateGetAclPolicies(""));

    assertThrows(
        RequestValidationFailureException.class,
        () -> databasesApiValidator.validateGetAclPolicies("d#$"));
  }

  @Test
  public void validateGetAclPoliciesSuccess() {
    assertDoesNotThrow(() -> databasesApiValidator.validateGetAclPolicies("d"));
  }
}
