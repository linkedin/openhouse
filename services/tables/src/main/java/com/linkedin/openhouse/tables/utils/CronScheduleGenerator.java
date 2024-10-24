package com.linkedin.openhouse.tables.utils;

import com.cronutils.builder.CronBuilder;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.expression.FieldExpression;
import com.cronutils.model.field.expression.FieldExpressionFactory;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Utility class for generating a cron schedule given an interval for which a job should run */
@Slf4j
@Component
public class CronScheduleGenerator {

  /**
   * Public api to generate a cron schedule for a {@link ReplicationConfig} based on a given
   * interval string in the form 12H, 1D, 2D, 3D.
   *
   * @param interval
   * @return schedule
   */
  public static String buildCronExpression(String interval) {
    if (interval == null || interval.isEmpty()) {
      log.error("Replication interval is null or empty");
      throw new RequestValidationFailureException(
          String.format("Replication interval is null or empty"));
    }
    int count = Integer.parseInt(interval.substring(0, interval.length() - 1));
    int hour = new Random().nextInt(24);
    String granularity = interval.substring(interval.length() - 1);
    String schedule;

    if (granularity.equals("H")) {
      schedule = buildHourlyCronExpression(hour, count);
    } else {
      schedule = buildDailyCronExpression(hour, count);
    }
    return schedule;
  }

  private static String buildDailyCronExpression(int hour, int dailyInterval) {
    return CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))
        .withYear(FieldExpressionFactory.always())
        .withDoM(FieldExpression.questionMark())
        .withDoW(FieldExpressionFactory.every(dailyInterval))
        .withMonth(FieldExpressionFactory.always())
        .withHour(FieldExpressionFactory.on(hour))
        .withMinute(FieldExpressionFactory.on(0))
        .withSecond(FieldExpressionFactory.on(0))
        .instance()
        .asString();
  }

  private static String buildHourlyCronExpression(int hour, int hourlyInterval) {
    return CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))
        .withYear(FieldExpressionFactory.always())
        .withDoM(FieldExpressionFactory.questionMark())
        .withDoW(FieldExpressionFactory.always())
        .withMonth(FieldExpressionFactory.always())
        .withHour(FieldExpressionFactory.every(hourlyInterval))
        .withMinute(FieldExpressionFactory.on(0))
        .withSecond(FieldExpressionFactory.on(0))
        .instance()
        .asString()
        .replace(
            String.format("*/%d", hourlyInterval), String.format("%d/%d", hour, hourlyInterval));
  }
}
