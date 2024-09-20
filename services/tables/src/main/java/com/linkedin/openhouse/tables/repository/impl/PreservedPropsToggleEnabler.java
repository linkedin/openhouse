package com.linkedin.openhouse.tables.repository.impl;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for the method with specific signature (see {@link
 * com.linkedin.openhouse.tables.toggle.FeatureToggleAspect} which controls if a table property is
 * considered preserved (read-only for users).
 *
 * <p>Usage of this annotation should be very specific to allow default-preserved keys to be
 * non-preserved. (This annotation is used as syntactical sugar to avoid contaminating main body of
 * service implementation)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PreservedPropsToggleEnabler {}
