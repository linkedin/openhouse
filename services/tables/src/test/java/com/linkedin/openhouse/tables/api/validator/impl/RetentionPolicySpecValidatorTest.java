package com.linkedin.openhouse.tables.api.validator.impl;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;
import static org.apache.iceberg.types.Types.NestedField.*;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.RetentionColumnPattern;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import java.lang.reflect.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RetentionPolicySpecValidatorTest {

  RetentionPolicySpecValidator validator;

  private Schema dummySchema;

  private Schema nestedSchema;

  @BeforeEach
  public void setup() {
    this.validator = new RetentionPolicySpecValidator();
    this.dummySchema =
        new Schema(
            required(1, "id", Types.StringType.get()), required(2, "aa", Types.StringType.get()));

    // A nested version wrapping over dummySchema
    this.nestedSchema =
        new Schema(
            required(3, "top1", Types.StructType.of(dummySchema.columns())),
            required(4, "top2", Types.IntegerType.get()));
  }

  @Test
  void testValidatePatternPositive() {

    // With pattern
    RetentionColumnPattern pattern =
        RetentionColumnPattern.builder().pattern("yyyy-mm-dd-hh").columnName("aa").build();
    Retention retention1 =
        Retention.builder()
            .columnPattern(pattern)
            .count(1)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .build();

    Assertions.assertTrue(
        validator.validatePatternIfPresent(
            retention1, TableUri.builder().build(), getSchemaJsonFromSchema(dummySchema)));

    // Without Pattern
    Retention retention2 =
        Retention.builder()
            .columnPattern(null)
            .count(10)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .build();
    Assertions.assertTrue(
        validator.validatePatternIfPresent(
            retention2, TableUri.builder().build(), getSchemaJsonFromSchema(dummySchema)));

    // Able to find nested columns in the column existence check
    pattern =
        RetentionColumnPattern.builder().pattern("yyyy-mm-dd-hh").columnName("top1.aa").build();
    Retention retention3 =
        Retention.builder()
            .columnPattern(pattern)
            .count(1)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .build();
    Assertions.assertTrue(
        validator.validatePatternIfPresent(
            retention3, TableUri.builder().build(), getSchemaJsonFromSchema(nestedSchema)));

    // Empty pattern is valid
    pattern = RetentionColumnPattern.builder().pattern("").columnName("top1.aa").build();
    Retention retention4 =
        Retention.builder()
            .columnPattern(pattern)
            .count(1)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .build();
    Assertions.assertTrue(
        validator.validatePatternIfPresent(
            retention4, TableUri.builder().build(), getSchemaJsonFromSchema(nestedSchema)));
  }

  @Test
  void testValidatePatternNegative() {
    RetentionColumnPattern malformedPattern =
        RetentionColumnPattern.builder().pattern("random_pattern").columnName("aa").build();
    Retention testRetention =
        Retention.builder()
            .columnPattern(malformedPattern)
            .count(1)
            .granularity(TimePartitionSpec.Granularity.DAY)
            .build();
    Assertions.assertFalse(
        validator.validatePatternIfPresent(
            testRetention, TableUri.builder().build(), getSchemaJsonFromSchema(dummySchema)));
  }

  @Test
  void testValidate() {
    // Negative: declared retention column not exists
    CreateUpdateTableRequestBody requestBodyColumnNotExists =
        createRequestBodyWithRetentionPolicy(
            RetentionColumnPattern.builder().pattern("yyyy-mm-dd-hh").columnName("bb").build(),
            1,
            TimePartitionSpec.Granularity.DAY,
            null);
    Assertions.assertFalse(
        validator.validate(requestBodyColumnNotExists, TableUri.builder().build()));

    CreateUpdateTableRequestBody requestBodyInvalidColumnCasing =
        createRequestBodyWithRetentionPolicy(
            RetentionColumnPattern.builder().pattern("yyyy-mm-dd-hh").columnName("Aa").build(),
            1,
            TimePartitionSpec.Granularity.DAY,
            null);
    Assertions.assertFalse(
        validator.validate(requestBodyInvalidColumnCasing, TableUri.builder().build()));

    CreateUpdateTableRequestBody requestBodyNestedColumnNameNotFound =
        createRequestBodyWithRetentionPolicy(
            RetentionColumnPattern.builder()
                .pattern("yyyy-mm-dd-hh")
                .columnName("top1.aaa")
                .build(),
            1,
            TimePartitionSpec.Granularity.DAY,
            null);
    Assertions.assertFalse(
        validator.validate(requestBodyNestedColumnNameNotFound, TableUri.builder().build()));

    // Negative: Missing timepartitionspec AND pattern
    CreateUpdateTableRequestBody requestBodyMissingPatternAndTimePartitionSpec =
        createRequestBodyWithRetentionPolicy(null, 1, TimePartitionSpec.Granularity.DAY, null);
    Assertions.assertFalse(
        validator.validate(
            requestBodyMissingPatternAndTimePartitionSpec, TableUri.builder().build()));

    // Positive: Only have pattern but no timepartitionSpec
    CreateUpdateTableRequestBody requestBodyNoTimePartitionSpec =
        createRequestBodyWithRetentionPolicy(
            RetentionColumnPattern.builder().pattern("yyyy-mm-dd-hh").build(),
            1,
            TimePartitionSpec.Granularity.DAY,
            null);
    Assertions.assertTrue(
        validator.validate(requestBodyNoTimePartitionSpec, TableUri.builder().build()));

    // Negative: Having both timepartitionspec AND pattern
    CreateUpdateTableRequestBody requestBodyBothPatternAndTimePartitionSpec =
        createRequestBodyWithRetentionPolicy(
            RetentionColumnPattern.builder().pattern("yyyy-mm-dd-hh").build(),
            1,
            TimePartitionSpec.Granularity.DAY,
            TimePartitionSpec.builder()
                .columnName("ts")
                .granularity(TimePartitionSpec.Granularity.DAY)
                .build());
    Assertions.assertFalse(
        validator.validate(requestBodyBothPatternAndTimePartitionSpec, TableUri.builder().build()));

    // Negative: Having both timepartitionspec AND invalid-pattern
    RetentionColumnPattern malformedPattern =
        RetentionColumnPattern.builder().pattern("random_pattern").columnName("aa").build();
    CreateUpdateTableRequestBody requestBodyMalformedPattern =
        createRequestBodyWithRetentionPolicy(
            malformedPattern,
            1,
            TimePartitionSpec.Granularity.DAY,
            TimePartitionSpec.builder()
                .columnName("ts")
                .granularity(TimePartitionSpec.Granularity.DAY)
                .build());

    Assertions.assertFalse(
        validator.validate(requestBodyMalformedPattern, TableUri.builder().build()));

    Field failedMsg =
        org.springframework.util.ReflectionUtils.findField(
            RetentionPolicySpecValidator.class, "failureMessage");
    Assertions.assertNotNull(failedMsg);
    org.springframework.util.ReflectionUtils.makeAccessible(failedMsg);
    Assertions.assertTrue(
        ((String) org.springframework.util.ReflectionUtils.getField(failedMsg, validator))
            .contains("You can only specify retention column pattern on non-timestampPartitioned"));

    // Negative: having granularity not supported by defaultColumPattern
    CreateUpdateTableRequestBody requestBodyInvalidGranularity =
        createRequestBodyWithRetentionPolicy(
            RetentionColumnPattern.builder().columnName("aa").pattern("").build(),
            1,
            TimePartitionSpec.Granularity.MONTH,
            null);
    Assertions.assertFalse(
        validator.validate(requestBodyInvalidGranularity, TableUri.builder().build()));

    failedMsg =
        org.springframework.util.ReflectionUtils.findField(
            RetentionPolicySpecValidator.class, "failureMessage");
    Assertions.assertNotNull(failedMsg);
    org.springframework.util.ReflectionUtils.makeAccessible(failedMsg);
    Assertions.assertTrue(
        ((String) org.springframework.util.ReflectionUtils.getField(failedMsg, validator))
            .contains("Please define pattern in retention config"));

    // The granularity mismatch is covered in
    // com.linkedin.openhouse.tables.e2e.h2.TablesControllerTest.testCreateRequestFailsForWithGranularityDifferentFromTimePartitionSpec
    // with error message validation

  }

  private CreateUpdateTableRequestBody createRequestBodyWithRetentionPolicy(
      RetentionColumnPattern pattern,
      int retentionCount,
      TimePartitionSpec.Granularity granularity,
      TimePartitionSpec timePartitioning) {
    Retention retention =
        Retention.builder()
            .count(retentionCount)
            .granularity(granularity)
            .columnPattern(pattern)
            .build();
    Policies policiesInvalidColumnNameCasing = Policies.builder().retention(retention).build();
    return CreateUpdateTableRequestBody.builder()
        .policies(policiesInvalidColumnNameCasing)
        .schema(getSchemaJsonFromSchema(dummySchema))
        .timePartitioning(timePartitioning)
        .build();
  }
}
