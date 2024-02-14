package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.JobsClientFactory;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.TablesClientFactory;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.lang.reflect.InvocationTargetException;
import lombok.AllArgsConstructor;

/** Factory class for creating tasks of the given type per table. */
@AllArgsConstructor
public class TableOperationTaskFactory<T extends TableOperationTask> {
  private Class<T> cls;
  private JobsClientFactory jobsClientFactory;
  private TablesClientFactory tablesClientFactory;

  public TableOperationTask create(TableMetadata tableMetadata)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException,
          IllegalAccessException {
    return cls.getDeclaredConstructor(JobsClient.class, TablesClient.class, TableMetadata.class)
        .newInstance(jobsClientFactory.create(), tablesClientFactory.create(), tableMetadata);
  }
}
