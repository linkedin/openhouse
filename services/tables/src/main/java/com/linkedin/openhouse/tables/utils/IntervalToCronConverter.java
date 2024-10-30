package com.linkedin.openhouse.tables.utils;

import com.cronutils.builder.CronBuilder;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.expression.FieldExpressionFactory;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Utility class for generating a cron schedule given an interval for which a job should run */
@Slf4j
@Component
public class IntervalToCronConverter {

  /**
   * Public api to generate a cron schedule for a {@link ReplicationConfig} based on a given
   * interval string in the form 12H, 1D, 2D, 3D.
   *
   * @param interval
   * @return schedule
   */
  public static String generateCronExpression(String interval) {
    if (interval == null || interval.isEmpty()) {
      String errorMessage = "Replication interval is null or empty";
      log.error(errorMessage);
      throw new RequestValidationFailureException(errorMessage);
    }
    int count = Integer.parseInt(interval.substring(0, interval.length() - 1));
    // Generating random hourly and minute intervals for the cron expression
    int hour = new Random().nextInt(24);
    int minute = new int[] {0, 15, 30, 45}[new Random().nextInt(4)];

    String granularity = interval.substring(interval.length() - 1);
    String schedule;

    if (granularity.equals(TimePartitionSpec.Granularity.HOUR.getGranularity())) {
      schedule = generateHourlyCronExpression(hour, minute, count);
    } else {
      schedule = generateDailyCronExpression(hour, minute, count);
    }
    return schedule;
  }

  private static String generateDailyCronExpression(int hour, int minute, int dailyInterval) {
    return CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))
        .withYear(FieldExpressionFactory.always())
        .withDoM(FieldExpressionFactory.questionMark())
        .withDoW(FieldExpressionFactory.every(dailyInterval))
        .withMonth(FieldExpressionFactory.always())
        .withHour(FieldExpressionFactory.on(hour))
        .withMinute(FieldExpressionFactory.on(minute))
        .withSecond(FieldExpressionFactory.on(0))
        .instance()
        .asString();
  }

  private static String generateHourlyCronExpression(int hour, int minute, int hourlyInterval) {
    return CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))
        .withYear(FieldExpressionFactory.always())
        .withDoM(FieldExpressionFactory.questionMark())
        .withDoW(FieldExpressionFactory.always())
        .withMonth(FieldExpressionFactory.always())
        .withHour(FieldExpressionFactory.every(hourlyInterval))
        .withMinute(FieldExpressionFactory.on(minute))
        .withSecond(FieldExpressionFactory.on(0))
        .instance()
        .asString()
        .replace(
            String.format("*/%d", hourlyInterval), String.format("%d/%d", hour, hourlyInterval));
  }
}
