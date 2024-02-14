package com.linkedin.openhouse.tablestest.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.platform.commons.util.AnnotationUtils;

/** Auxiliary data structure for {@link CustomParameterResolver}. */
public class MappedParameterContext implements ParameterContext {
  private final int index;
  private final Parameter parameter;
  private final Optional<Object> target;

  public MappedParameterContext(int index, Parameter parameter, Optional<Object> target) {
    this.index = index;
    this.parameter = parameter;
    this.target = target;
  }

  @Override
  public boolean isAnnotated(Class<? extends Annotation> annotationType) {
    return AnnotationUtils.isAnnotated(parameter, annotationType);
  }

  @Override
  public <A extends Annotation> Optional<A> findAnnotation(Class<A> annotationType) {
    return Optional.empty();
  }

  @Override
  public <A extends Annotation> List<A> findRepeatableAnnotations(Class<A> annotationType) {
    throw new UnsupportedOperationException("Not needed for ParameterContext conversion");
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public Parameter getParameter() {
    return parameter;
  }

  @Override
  public Optional<Object> getTarget() {
    return target;
  }
}
