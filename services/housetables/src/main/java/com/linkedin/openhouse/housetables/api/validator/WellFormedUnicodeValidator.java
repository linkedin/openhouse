package com.linkedin.openhouse.housetables.api.validator;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class WellFormedUnicodeValidator implements ConstraintValidator<WellFormedUnicode, String> {
  @Override
  public boolean isValid(String value, ConstraintValidatorContext context) {
    return Optional.ofNullable(value)
        .map(text -> StandardCharsets.UTF_8.newEncoder().canEncode(text))
        .orElse(true);
  }
}
