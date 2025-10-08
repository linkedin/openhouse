package com.linkedin.openhouse.tables.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class IntervalToCronConverterTest {

  @Test
  void hourly_cron_uses_start_less_than_interval_and_exact_interval_value() {
    List<Integer> intervals = Arrays.asList(1, 3, 6, 8, 12);
    for (int interval : intervals) {
      IntervalToCronConverter cronConverter = Mockito.mock(IntervalToCronConverter.class);
      Mockito.doCallRealMethod()
          .when(cronConverter)
          .generateCronExpressionInstance(Mockito.anyString());
      cronConverter.generateCronExpressionInstance(interval + "H");
      // Ensure that the randomInt call for hour is bounded by the interval value
      Mockito.verify(cronConverter, Mockito.never()).randomInt(24);
      Mockito.verify(cronConverter, Mockito.times(1)).randomInt(interval);
      String schedule = IntervalToCronConverter.generateCronExpression(interval + "H");
      if (interval != 1) {
        String[] parts = schedule.split(" ");
        int startHour = Integer.parseInt(parts[2].split("/")[0]);
        int intervalHour = Integer.parseInt(parts[2].split("/")[1]);
        assertTrue(startHour < intervalHour);
        assertEquals(interval, intervalHour);
      }
    }
  }
}
