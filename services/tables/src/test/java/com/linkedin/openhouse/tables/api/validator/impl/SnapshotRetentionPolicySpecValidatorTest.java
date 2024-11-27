package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LogicalOperator;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.SnapshotRetention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnapshotRetentionPolicySpecValidatorTest {
  SnapshotRetentionPolicySpecValidator validator;

  TableUri tableUri = TableUri.builder().build();

  @BeforeEach
  public void setup() {
    this.validator = new SnapshotRetentionPolicySpecValidator();
  }

  @Test
  void testValidateRejectsUnstructuredTimeCount() {
    SnapshotRetention snapshotRetentionWithNoGranularity =
        SnapshotRetention.builder().timeCount(1).build();

    Assertions.assertFalse(this.validator.validate(snapshotRetentionWithNoGranularity, tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("Incorrect timeCount specified"));

    SnapshotRetention snapshotRetentionWithNoTimeCount =
        SnapshotRetention.builder()
            .granularity(TimePartitionSpec.Granularity.DAY)
            .versionCount(3)
            .logicalOperator(LogicalOperator.Operator.AND)
            .build();

    Assertions.assertFalse(this.validator.validate(snapshotRetentionWithNoTimeCount, tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("Incorrect timeCount specified"));
  }

  @Test
  void testValidateDefineNonNullRetentionPolicies() {
    SnapshotRetention snapshotRetention = SnapshotRetention.builder().build();

    Assertions.assertFalse(this.validator.validate(snapshotRetention, tableUri));
    Assertions.assertTrue(
        this.validator
            .getMessage()
            .contains("Must define either a time based retention or count based retention"));
  }

  @Test
  void testValidateRejectPolicyLogicalOperators() {
    SnapshotRetention snapshotRetentionNoOperator =
        SnapshotRetention.builder()
            .timeCount(3)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .versionCount(10)
            .build();

    Assertions.assertFalse(this.validator.validate(snapshotRetentionNoOperator, tableUri));
    Assertions.assertTrue(
        this.validator.getMessage().contains("Must define logical operator to describe behavior"));

    SnapshotRetention snapshotRetentionOperatorWithoutBothPolicies =
        SnapshotRetention.builder()
            .logicalOperator(LogicalOperator.Operator.AND)
            .versionCount(10)
            .build();

    Assertions.assertFalse(
        this.validator.validate(snapshotRetentionOperatorWithoutBothPolicies, tableUri));
    Assertions.assertTrue(
        this.validator.getMessage().contains("Must define logical operator to describe behavior"));
  }

  @Test
  void testValidateSnapshotRetentionMaximums() {
    // Exceed days
    SnapshotRetention snapshotRetentionDaysExceeded =
        SnapshotRetention.builder()
            .timeCount(4)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .versionCount(10)
            .logicalOperator(LogicalOperator.Operator.AND)
            .build();
    Assertions.assertFalse(this.validator.validate(snapshotRetentionDaysExceeded, tableUri));

    // Exceed days in hours
    SnapshotRetention snapshotRetentionHoursExceeded =
        SnapshotRetention.builder()
            .timeCount(100)
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .build();
    Assertions.assertFalse(this.validator.validate(snapshotRetentionHoursExceeded, tableUri));

    // Exceed Granularity
    SnapshotRetention snapshotRetentionGranularityExceeded =
        SnapshotRetention.builder()
            .timeCount(2)
            .granularity(TimePartitionSpec.Granularity.MONTH)
            .build();
    Assertions.assertFalse(this.validator.validate(snapshotRetentionGranularityExceeded, tableUri));

    // Exceed version count
    SnapshotRetention snapshotRetentionCountExceeded =
        SnapshotRetention.builder().versionCount(1000).build();
    Assertions.assertFalse(this.validator.validate(snapshotRetentionCountExceeded, tableUri));

    // Exceed both policies
    SnapshotRetention snapshotRetentionBothExceeded =
        SnapshotRetention.builder()
            .timeCount(100)
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .versionCount(1000)
            .logicalOperator(LogicalOperator.Operator.AND)
            .build();
    Assertions.assertFalse(this.validator.validate(snapshotRetentionBothExceeded, tableUri));
    Assertions.assertTrue(this.validator.getMessage().contains("cannot exceed"));
  }

  @Test
  void testValidatePoliciesPositive() {
    // Only define timeCount
    SnapshotRetention snapshotRetention =
        SnapshotRetention.builder()
            .timeCount(36)
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .build();
    Assertions.assertTrue(this.validator.validate(snapshotRetention, tableUri));

    snapshotRetention = SnapshotRetention.builder().versionCount(50).build();
    Assertions.assertTrue(this.validator.validate(snapshotRetention, tableUri));

    snapshotRetention =
        SnapshotRetention.builder()
            .timeCount(3)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .logicalOperator(LogicalOperator.Operator.OR)
            .versionCount(10)
            .build();
    Assertions.assertTrue(this.validator.validate(snapshotRetention, tableUri));
  }
}
