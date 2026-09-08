package com.linkedin.openhouse.common.api.validator;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_ERROR_MSG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ApiValidatorUtilTest {

  /** Stand-in root bean, only used for the simple name that {@code getField} derives. */
  private static final class SampleRequestBody {}

  @Test
  public void testValidateIdentifierAcceptsLegalIdentifier() {
    List<String> validationFailures = new ArrayList<>();
    ApiValidatorUtil.validateIdentifier("databaseId", "db_1", validationFailures);
    Assertions.assertEquals(Collections.emptyList(), validationFailures);
  }

  @Test
  public void testValidateIdentifierRejectsEmpty() {
    List<String> validationFailures = new ArrayList<>();
    ApiValidatorUtil.validateIdentifier("databaseId", "", validationFailures);
    Assertions.assertEquals(
        Collections.singletonList("databaseId : Cannot be empty"), validationFailures);
  }

  @Test
  public void testValidateIdentifierRejectsNull() {
    List<String> validationFailures = new ArrayList<>();
    ApiValidatorUtil.validateIdentifier("tableId", null, validationFailures);
    Assertions.assertEquals(
        Collections.singletonList("tableId : Cannot be empty"), validationFailures);
  }

  @Test
  public void testValidateIdentifierRejectsIllegalCharacters() {
    List<String> validationFailures = new ArrayList<>();
    ApiValidatorUtil.validateIdentifier("tableId", "t#1", validationFailures);
    Assertions.assertEquals(
        Collections.singletonList("tableId : provided t#1, " + ALPHA_NUM_UNDERSCORE_ERROR_MSG),
        validationFailures);
  }

  @Test
  public void testValidateIdentifierAppendsWithoutClearing() {
    List<String> validationFailures = new ArrayList<>();
    validationFailures.add("pre-existing failure");
    ApiValidatorUtil.validateIdentifier("databaseId", "d$b", validationFailures);
    Assertions.assertEquals(
        Arrays.asList(
            "pre-existing failure", "databaseId : provided d$b, " + ALPHA_NUM_UNDERSCORE_ERROR_MSG),
        validationFailures);
  }

  @Test
  public void testCollectViolationsFormatsEachViolation() {
    SampleRequestBody requestBody = new SampleRequestBody();
    Set<ConstraintViolation<SampleRequestBody>> violations = new LinkedHashSet<>();
    violations.add(violation(requestBody, "tableId", "must not be empty"));
    violations.add(violation(requestBody, "", "must be a consistent request"));

    Validator validator = Mockito.mock(Validator.class);
    Mockito.when(validator.validate(requestBody)).thenReturn(violations);

    List<String> validationFailures = new ArrayList<>();
    validationFailures.add("pre-existing failure");
    ApiValidatorUtil.collectViolations(validator, requestBody, validationFailures);

    Assertions.assertEquals(
        Arrays.asList(
            "pre-existing failure",
            "SampleRequestBody.tableId : must not be empty",
            "SampleRequestBody : must be a consistent request"),
        validationFailures);
  }

  @Test
  public void testCollectViolationsAddsNothingWhenBeanIsValid() {
    SampleRequestBody requestBody = new SampleRequestBody();
    Validator validator = Mockito.mock(Validator.class);
    Mockito.when(validator.validate(requestBody)).thenReturn(Collections.emptySet());

    List<String> validationFailures = new ArrayList<>();
    ApiValidatorUtil.collectViolations(validator, requestBody, validationFailures);

    Assertions.assertEquals(Collections.emptyList(), validationFailures);
  }

  @SuppressWarnings("unchecked")
  private static ConstraintViolation<SampleRequestBody> violation(
      SampleRequestBody rootBean, String propertyPath, String message) {
    ConstraintViolation<SampleRequestBody> violation = Mockito.mock(ConstraintViolation.class);
    Mockito.when(violation.getRootBean()).thenReturn(rootBean);
    Mockito.when(violation.getPropertyPath()).thenReturn(path(propertyPath));
    Mockito.when(violation.getMessage()).thenReturn(message);
    return violation;
  }

  /** {@code getField} only reads the string form of a path, so an empty node list is enough. */
  private static Path path(String value) {
    return new Path() {
      @Override
      public Iterator<Node> iterator() {
        return Collections.<Node>emptyList().iterator();
      }

      @Override
      public String toString() {
        return value;
      }
    };
  }
}
