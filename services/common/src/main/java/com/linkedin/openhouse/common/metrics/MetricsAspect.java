package com.linkedin.openhouse.common.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class MetricsAspect {
  @Autowired private MeterRegistry meterRegistry;

  @Around("@annotation(ReportExecutionTime) && args(key)")
  public Object reportExecutionTime(ProceedingJoinPoint joinPoint, String key) throws Throwable {
    long startTime = System.currentTimeMillis();
    Object result = joinPoint.proceed();
    meterRegistry.timer(key).record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    return result;
  }
}
