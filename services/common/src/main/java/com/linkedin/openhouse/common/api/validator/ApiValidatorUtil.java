package com.linkedin.openhouse.common.api.validator;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_ERROR_MSG;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_REGEX;

import java.util.List;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.apache.commons.lang3.StringUtils;

public final class ApiValidatorUtil {
  /**
   * Helper function to get the name of an offending field that fails validation.
   *
   * <p>For example, consider a class with field annotated as follows: class Example { @NotNull
   * private String tableId; } This function would return: "Example.tableId"
   *
   * @param violation, a violation returned from calling "validator.validate(object)".
   * @return string containing the path to the field. For example: "Example.tableId"
   */
  public static String getField(ConstraintViolation<?> violation) {
    final String rootBeanName = violation.getRootBean().getClass().getSimpleName();
    final String propertyPath = violation.getPropertyPath().toString();
    return propertyPath.isEmpty() ? rootBeanName : rootBeanName + '.' + propertyPath;
  }

  private ApiValidatorUtil() {
    // hide default constructor for utility class
  }

  /**
   * Common method to validate pageable parameters for pagination APIs.
   *
   * @param page
   * @param size
   * @param sortBy
   * @param validationFailures
   */
  public static void validatePageable(
      int page, int size, String sortBy, List<String> validationFailures) {
    if (page < 0) {
      validationFailures.add(String.format("page : provided %s, cannot be negative", page));
    }
    if (size <= 0) {
      validationFailures.add(String.format("size : provided %s, must be greater than 0", size));
    }
    if (sortBy != null && (sortBy.contains(",") || sortBy.contains(":"))) {
      validationFailures.add(
          String.format(
              "sortBy : provided %s, does not support multiple sort fields or directions", sortBy));
    }
  }

  /**
   * Common method to run bean validation on a request object and append one formatted message per
   * violation to the running list of validation failures.
   *
   * @param validator
   * @param object
   * @param validationFailures
   */
  public static <T> void collectViolations(
      Validator validator, T object, List<String> validationFailures) {
    for (ConstraintViolation<T> violation : validator.validate(object)) {
      validationFailures.add(String.format("%s : %s", getField(violation), violation.getMessage()));
    }
  }

  /**
   * Common method to validate that an identifier is present and contains only the characters
   * allowed by {@link ValidatorConstants#ALPHA_NUM_UNDERSCORE_REGEX}. At most one failure is
   * reported: an absent identifier is not additionally reported as malformed.
   *
   * @param fieldName
   * @param value
   * @param validationFailures
   */
  public static void validateIdentifier(
      String fieldName, String value, List<String> validationFailures) {
    if (StringUtils.isEmpty(value)) {
      validationFailures.add(String.format("%s : Cannot be empty", fieldName));
    } else if (!value.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      validationFailures.add(
          String.format("%s : provided %s, %s", fieldName, value, ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    }
  }
}
