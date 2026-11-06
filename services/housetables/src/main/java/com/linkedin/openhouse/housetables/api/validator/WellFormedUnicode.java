package com.linkedin.openhouse.housetables.api.validator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

@Target({ElementType.TYPE_USE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = WellFormedUnicodeValidator.class)
public @interface WellFormedUnicode {
  String message() default "must contain well-formed Unicode";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
