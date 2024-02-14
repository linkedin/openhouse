package com.linkedin.openhouse.common.api.validator;

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
}
