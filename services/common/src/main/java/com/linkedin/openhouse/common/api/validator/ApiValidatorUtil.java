package com.linkedin.openhouse.common.api.validator;

import java.util.List;
import javax.validation.ConstraintViolation;

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
}
