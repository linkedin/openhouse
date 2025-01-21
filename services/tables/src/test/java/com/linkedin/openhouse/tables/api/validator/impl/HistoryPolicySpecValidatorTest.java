package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.History;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HistoryPolicySpecValidatorTest {
  HistoryPolicySpecValidator validator;

  TableUri tableUri = TableUri.builder().build();

  @BeforeEach
  public void setup() {
    this.validator = new HistoryPolicySpecValidator();
  }

  @Test
  void testValidateRejectsUnstructuredmaxAge() {
    History historyWithNoGranularity = History.builder().maxAge(1).build();

    Assertions.assertFalse(
        this.validator.validate(
            createRequestBodyWithHistoryPolicy(historyWithNoGranularity), tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("Incorrect maxAge specified"));

    History historyWithNomaxAge =
        History.builder().granularity(TimePartitionSpec.Granularity.DAY).versions(3).build();

    Assertions.assertFalse(
        this.validator.validate(createRequestBodyWithHistoryPolicy(historyWithNomaxAge), tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("Incorrect maxAge specified"));
  }

  @Test
  void testValidateDefineNonNullRetentionPolicies() {
    History history = History.builder().build();

    Assertions.assertFalse(
        this.validator.validate(createRequestBodyWithHistoryPolicy(history), tableUri));
    Assertions.assertTrue(
        this.validator
            .getMessage()
            .contains("Must define either a time based retention or count based retention"));
  }

  @Test
  void testValidateHistoryMaximums() {
    // Exceed days
    History historyDaysExceeded =
        History.builder()
            .maxAge(4)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .versions(10)
            .build();
    Assertions.assertFalse(
        this.validator.validate(createRequestBodyWithHistoryPolicy(historyDaysExceeded), tableUri));

    // Exceed days in hours
    History historyHoursExceeded =
        History.builder().maxAge(100).granularity(TimePartitionSpec.Granularity.HOUR).build();
    Assertions.assertFalse(
        this.validator.validate(
            createRequestBodyWithHistoryPolicy(historyHoursExceeded), tableUri));

    // Exceed Granularity
    History historyGranularityExceeded =
        History.builder().maxAge(2).granularity(TimePartitionSpec.Granularity.MONTH).build();
    Assertions.assertFalse(
        this.validator.validate(
            createRequestBodyWithHistoryPolicy(historyGranularityExceeded), tableUri));

    // Exceed version count
    History historyCountExceeded = History.builder().versions(1000).build();
    Assertions.assertFalse(
        this.validator.validate(
            createRequestBodyWithHistoryPolicy(historyCountExceeded), tableUri));

    // Exceed both policies
    History historyBothExceeded =
        History.builder()
            .maxAge(100)
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .versions(1000)
            .build();
    Assertions.assertFalse(
        this.validator.validate(createRequestBodyWithHistoryPolicy(historyBothExceeded), tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("must be between"));
  }

  @Test
  void testValidateHistoryMinimums() {
    // Less than minimum number of hours
    History historyHoursMin =
        History.builder().maxAge(1).granularity(TimePartitionSpec.Granularity.HOUR).build();
    Assertions.assertFalse(
        this.validator.validate(createRequestBodyWithHistoryPolicy(historyHoursMin), tableUri));

    // Less than 2 versions
    History historyVersionsMin = History.builder().versions(1).build();
    Assertions.assertFalse(
        this.validator.validate(createRequestBodyWithHistoryPolicy(historyVersionsMin), tableUri));

    // Less than minimum number of days
    History historyDaysMin =
        History.builder().maxAge(0).granularity(TimePartitionSpec.Granularity.DAY).build();
    Assertions.assertFalse(
        this.validator.validate(createRequestBodyWithHistoryPolicy(historyDaysMin), tableUri));

    // Less than both policies
    History historyPolicyMin =
        History.builder()
            .maxAge(5)
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .versions(1)
            .build();
    Assertions.assertFalse(
        this.validator.validate(createRequestBodyWithHistoryPolicy(historyPolicyMin), tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("must be between"));
  }

  @Test
  void testValidatePoliciesPositive() {
    // Only define maxAge
    History history =
        History.builder().maxAge(36).granularity(TimePartitionSpec.Granularity.HOUR).build();
    Assertions.assertTrue(
        this.validator.validate(createRequestBodyWithHistoryPolicy(history), tableUri));

    history = History.builder().versions(50).build();
    Assertions.assertTrue(
        this.validator.validate(createRequestBodyWithHistoryPolicy(history), tableUri));

    // Only define versions
    history = History.builder().versions(10).build();
    Assertions.assertTrue(
        this.validator.validate(createRequestBodyWithHistoryPolicy(history), tableUri));

    // Define both maxAge and version count
    history =
        History.builder()
            .maxAge(3)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .versions(10)
            .build();
    Assertions.assertTrue(
        this.validator.validate(createRequestBodyWithHistoryPolicy(history), tableUri));
  }

  private CreateUpdateTableRequestBody createRequestBodyWithHistoryPolicy(History history) {
    Policies historyPolicies = Policies.builder().history(history).build();
    return CreateUpdateTableRequestBody.builder().policies(historyPolicies).build();
  }
}
