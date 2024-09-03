package com.linkedin.openhouse.internal.catalog.toggle.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Used to gate the feature of the corresponding method where this annotation is applied. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface IcebergFeatureGate {
  String value();
}
