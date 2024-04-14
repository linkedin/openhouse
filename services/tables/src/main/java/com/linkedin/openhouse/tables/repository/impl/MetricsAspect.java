package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.TableIdentifier;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Slf4j
public class MetricsAspect {
  @Autowired private MeterRegistry meterRegistry;

  @Around("@annotation(timed) && args(tableDto)")
  public Object reportExecutionTime(ProceedingJoinPoint joinPoint, Timed timed, TableDto tableDto)
      throws Throwable {
    TableIdentifier tableId = TableIdentifier.of(tableDto.getDatabaseId(), tableDto.getTableId());
    return reportExecutionTime(joinPoint, timed.metricKey(), Optional.of("table " + tableId));
  }

  @Around("@annotation(timed) && args(tableDtoPrimaryKey)")
  public Object reportExecutionTime(
      ProceedingJoinPoint joinPoint, Timed timed, TableDtoPrimaryKey tableDtoPrimaryKey)
      throws Throwable {
    TableIdentifier tableId =
        TableIdentifier.of(tableDtoPrimaryKey.getDatabaseId(), tableDtoPrimaryKey.getTableId());
    return reportExecutionTime(joinPoint, timed.metricKey(), Optional.of("table " + tableId));
  }

  @Around("@annotation(timed) && args(databaseId)")
  public Object reportExecutionTime(ProceedingJoinPoint joinPoint, Timed timed, String databaseId)
      throws Throwable {
    return reportExecutionTime(joinPoint, timed.metricKey(), Optional.of("database " + databaseId));
  }

  @Around("@annotation(timed) && args()")
  public Object reportExecutionTime(ProceedingJoinPoint joinPoint, Timed timed) throws Throwable {
    return reportExecutionTime(joinPoint, timed.metricKey(), Optional.empty());
  }

  private Object reportExecutionTime(
      ProceedingJoinPoint joinPoint, String metricKey, Optional<String> entity) throws Throwable {
    long startTime = System.currentTimeMillis();
    try {
      return joinPoint.proceed();
    } finally {
      long executionTime = System.currentTimeMillis() - startTime;
      String entityMessage = entity.map(s -> " for " + s).orElse("");
      log.info(
          "OpenHouseInternalRepositoryImpl.{}{} took {} ms",
          joinPoint.getSignature().getName(),
          entityMessage,
          executionTime);
      meterRegistry.timer(metricKey).record(executionTime, TimeUnit.MILLISECONDS);
    }
  }
}
