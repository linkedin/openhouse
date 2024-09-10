package com.linkedin.openhouse.tables.repository.impl;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for the method with specific signature (see {@link
 * com.linkedin.openhouse.tables.toggle.FeatureToggleAspect} which controls if a table property is
 * considered preserved (read-only for users)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TblPropsEnabler {}
