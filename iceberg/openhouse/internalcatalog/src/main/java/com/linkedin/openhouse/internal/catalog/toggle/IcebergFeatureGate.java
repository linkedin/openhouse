package com.linkedin.openhouse.internal.catalog.toggle;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to gate the feature of the corresponding method where this annotation is applied, on the
 * granularity of Iceberg table identified by {@link org.apache.iceberg.catalog.TableIdentifier}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface IcebergFeatureGate {
  String value();
}
