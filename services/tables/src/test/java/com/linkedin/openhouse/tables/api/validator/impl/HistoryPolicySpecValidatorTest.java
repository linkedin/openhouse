package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.History;
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

    Assertions.assertFalse(this.validator.validate(historyWithNoGranularity, tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("Incorrect maxAge specified"));

    History historyWithNomaxAge =
        History.builder().granularity(TimePartitionSpec.Granularity.DAY).versions(3).build();

    Assertions.assertFalse(this.validator.validate(historyWithNomaxAge, tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("Incorrect maxAge specified"));
  }

  @Test
  void testValidateDefineNonNullRetentionPolicies() {
    History history = History.builder().build();

    Assertions.assertFalse(this.validator.validate(history, tableUri));
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
    Assertions.assertFalse(this.validator.validate(historyDaysExceeded, tableUri));

    // Exceed days in hours
    History historyHoursExceeded =
        History.builder().maxAge(100).granularity(TimePartitionSpec.Granularity.HOUR).build();
    Assertions.assertFalse(this.validator.validate(historyHoursExceeded, tableUri));

    // Exceed Granularity
    History historyGranularityExceeded =
        History.builder().maxAge(2).granularity(TimePartitionSpec.Granularity.MONTH).build();
    Assertions.assertFalse(this.validator.validate(historyGranularityExceeded, tableUri));

    // Exceed version count
    History historyCountExceeded = History.builder().versions(1000).build();
    Assertions.assertFalse(this.validator.validate(historyCountExceeded, tableUri));

    // Exceed both policies
    History historyBothExceeded =
        History.builder()
            .maxAge(100)
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .versions(1000)
            .build();
    Assertions.assertFalse(this.validator.validate(historyBothExceeded, tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("cannot exceed"));
  }

  @Test
  void testValidatePoliciesPositive() {
    // Only define maxAge
    History history =
        History.builder().maxAge(36).granularity(TimePartitionSpec.Granularity.HOUR).build();
    Assertions.assertTrue(this.validator.validate(history, tableUri));

    history = History.builder().versions(50).build();
    Assertions.assertTrue(this.validator.validate(history, tableUri));

    // Only define versions
    history = History.builder().versions(10).build();
    Assertions.assertTrue(this.validator.validate(history, tableUri));

    // Define both maxAge and version count
    history =
        History.builder()
            .maxAge(3)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .versions(10)
            .build();
    Assertions.assertTrue(this.validator.validate(history, tableUri));
  }
}
