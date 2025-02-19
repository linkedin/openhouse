package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.JobsClientFactory;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.TablesClientFactory;
import com.linkedin.openhouse.jobs.util.Metadata;
import java.lang.reflect.InvocationTargetException;
import lombok.AllArgsConstructor;

/** Factory class for creating tasks of the given type per table. */
@AllArgsConstructor
public class OperationTaskFactory<T extends OperationTask<?>> {
  private Class<T> cls;
  private JobsClientFactory jobsClientFactory;
  private TablesClientFactory tablesClientFactory;
  private long pollIntervalMs;
  private long timeoutMs;

  public <S extends Metadata> T create(S metadata)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException,
          IllegalAccessException, IllegalStateException {
    return cls.getDeclaredConstructor(
            JobsClient.class, TablesClient.class, metadata.getClass(), long.class, long.class)
        .newInstance(
            jobsClientFactory.create(),
            tablesClientFactory.create(),
            metadata,
            pollIntervalMs,
            timeoutMs);
  }
}
