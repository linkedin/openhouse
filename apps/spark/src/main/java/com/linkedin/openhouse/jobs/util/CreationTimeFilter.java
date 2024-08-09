package com.linkedin.openhouse.jobs.util;

import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;

/** Filter class to collect table with creation time less than currentTime - cutoffHour */
@AllArgsConstructor
public class CreationTimeFilter implements TableFilter {

  private int cutoffHour;

  public static CreationTimeFilter of(int timeInHour) {
    return new CreationTimeFilter(timeInHour);
  }

  @Override
  public boolean apply(TableMetadata metadata) {
    return metadata.getCreationTime()
        < System.currentTimeMillis() - TimeUnit.HOURS.toMillis(cutoffHour);
  }
}
