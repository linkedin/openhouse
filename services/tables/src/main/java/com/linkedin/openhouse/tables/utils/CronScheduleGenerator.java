package com.linkedin.openhouse.tables.utils;

import com.cronutils.builder.CronBuilder;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.expression.FieldExpressionFactory;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Utility class for generating cron schedules based on interval input for {@link
 * ReplicationConfig}.
 */
@Slf4j
@Component
public class CronScheduleGenerator {
  public static String buildCronExpression(String interval) {
    int count = Integer.parseInt(interval.substring(0, interval.length() - 1));
    String granularity = interval.substring(interval.length() - 1);
    int hourSchedule = new Random().nextInt(24);
    String schedule;

    if (granularity.equals(ReplicationConfig.Granularity.H.toString())) {
      schedule = buildHourlyCronExpression(hourSchedule, count);
    } else {
      schedule = buildDailyCronExpression(hourSchedule, count);
    }
    return schedule;
  }

  private static String buildDailyCronExpression(int hourSchedule, int dailyInterval) {
    return CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(CronType.CRON4J))
        .withDoM(FieldExpressionFactory.every(dailyInterval))
        .withMonth(FieldExpressionFactory.always())
        .withDoW(FieldExpressionFactory.always())
        .withHour(FieldExpressionFactory.on(hourSchedule))
        .withMinute(FieldExpressionFactory.on(0))
        .instance()
        .asString();
  }

  private static String buildHourlyCronExpression(int hourSchedule, int hourlyInterval) {
    return CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(CronType.CRON4J))
        .withDoM(FieldExpressionFactory.always())
        .withMonth(FieldExpressionFactory.always())
        .withDoW(FieldExpressionFactory.always())
        .withHour(
            FieldExpressionFactory.every(hourlyInterval)
                .and(FieldExpressionFactory.on(hourSchedule)))
        .withMinute(FieldExpressionFactory.on(0))
        .instance()
        .asString();
  }
}
