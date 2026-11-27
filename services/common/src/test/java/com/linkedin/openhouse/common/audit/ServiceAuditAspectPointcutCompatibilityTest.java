package com.linkedin.openhouse.common.audit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tables.exception.ViewExceptionHandlerShape;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.aspectj.lang.annotation.Around;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.AspectJExpressionPointcut;

public class ServiceAuditAspectPointcutCompatibilityTest {

  @Test
  public void optionalViewExceptionHandlerPointcutParsesWithoutTablesClassPresent()
      throws NoSuchMethodException {
    AspectJExpressionPointcut pointcut = new AspectJExpressionPointcut();

    assertDoesNotThrow(() -> pointcut.setExpression(failureAdviceExpression()));

    assertTrue(pointcut.matches(exceptionHandlerMethod(), ViewExceptionHandlerShape.class));
    assertFalse(
        pointcut.matches(publicHelperMethod(), ViewExceptionHandlerShape.class),
        "Only @ExceptionHandler methods should be captured by the optional view-advice branch.");
  }

  @Test
  public void productionFailureAuditPointcutDeclaresOptionalViewAdviceBranch() {
    String expression = failureAdviceExpression();

    assertTrue(expression.contains("ViewExceptionHandler*"));
    assertTrue(
        expression.contains(
            "@annotation(org.springframework.web.bind.annotation.ExceptionHandler)"));
  }

  private static Method exceptionHandlerMethod() throws NoSuchMethodException {
    return ViewExceptionHandlerShape.class.getDeclaredMethod("handle");
  }

  private static Method publicHelperMethod() throws NoSuchMethodException {
    return ViewExceptionHandlerShape.class.getDeclaredMethod("helper");
  }

  private static String failureAdviceExpression() {
    return Arrays.stream(ServiceAuditAspect.class.getDeclaredMethods())
        .flatMap(method -> Arrays.stream(method.getAnnotations()))
        .filter(annotation -> annotation.annotationType().equals(Around.class))
        .map(annotation -> ((Around) annotation).value())
        .filter(value -> value.contains("common.exception.handler"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("ServiceAuditAspect failure advice not found"));
  }
}
