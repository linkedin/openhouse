package com.linkedin.openhouse.jobs.scheduler;

import com.linkedin.openhouse.jobs.scheduler.tasks.TableRetentionTask;
import org.junit.jupiter.api.Test;

public class JobsSchedulerTest {
  @Test
  void testRegistryIsInitialized() {
    JobsScheduler.main(
        new String[] {
          "--type", TableRetentionTask.OPERATION_TYPE.getValue(),
          "--tablesURL", "http://test.openhouse.com",
          "--jobsURL", "unused",
          "--cluster", "unused",
        });
  }
}
