package com.linkedin.openhouse.tablestest.annotation;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.engine.execution.BeforeEachMethodAdapter;
import org.junit.jupiter.engine.extension.ExtensionRegistry;

/**
 * Reference:
 * https://github.com/lamektomasz/AfterBeforeParameterizedTestExtension/commit/7ca50e3bc8046625c21ce0b0863dc54022fff637
 * This {@link ParameterResolver} hooks both {@link AfterEach} and {@link BeforeEach} annotation
 * together through the boolean condition in {@link #isAfterEachOrBeforeEachAnnotation(Annotation)},
 * the reason for doing that is {@link #parameterizedTestParameterResolver} has to be initialized in
 * {@link #invokeBeforeEachMethod} which invoked from {@link BeforeEach} annotation.
 *
 * <p>Constraints: - Annotated method need to have the same signature as testing method calling it.
 * - And testing method needs to be parameterized.
 */
public class CustomParameterResolver implements BeforeEachMethodAdapter, ParameterResolver {
  private ParameterResolver parameterizedTestParameterResolver = null;

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (isExecutedOnAfterOrBeforeMethod(parameterContext)) {
      ParameterContext pContext = getMappedContext(parameterContext, extensionContext);
      return parameterizedTestParameterResolver.supportsParameter(pContext, extensionContext);
    }
    return false;
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterizedTestParameterResolver.resolveParameter(
        getMappedContext(parameterContext, extensionContext), extensionContext);
  }

  private MappedParameterContext getMappedContext(
      ParameterContext parameterContext, ExtensionContext extensionContext) {
    return new MappedParameterContext(
        parameterContext.getIndex(),
        extensionContext.getRequiredTestMethod().getParameters()[parameterContext.getIndex()],
        Optional.of(parameterContext.getTarget()));
  }

  private boolean isExecutedOnAfterOrBeforeMethod(ParameterContext parameterContext) {
    return Arrays.stream(parameterContext.getDeclaringExecutable().getDeclaredAnnotations())
        .anyMatch(this::isAfterEachOrBeforeEachAnnotation);
  }

  private boolean isAfterEachOrBeforeEachAnnotation(Annotation annotation) {
    return annotation.annotationType() == BeforeEach.class
        || annotation.annotationType() == AfterEach.class;
  }

  @Override
  public void invokeBeforeEachMethod(ExtensionContext context, ExtensionRegistry registry)
      throws Throwable {
    Optional<ParameterResolver> resolverOptional =
        registry.getExtensions(ParameterResolver.class).stream()
            .filter(
                parameterResolver ->
                    parameterResolver
                        .getClass()
                        .getName()
                        .contains("ParameterizedTestParameterResolver"))
            .findFirst();
    if (!resolverOptional.isPresent()) {
      throw new IllegalStateException(
          "ParameterizedTestParameterResolver missed in the registry. Probably it's not a Parameterized Test");
    } else {
      parameterizedTestParameterResolver = resolverOptional.get();
    }
  }
}
