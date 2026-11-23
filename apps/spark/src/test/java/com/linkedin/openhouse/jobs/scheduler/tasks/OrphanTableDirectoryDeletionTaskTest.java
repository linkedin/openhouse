package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class OrphanTableDirectoryDeletionTaskTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testJobIsLaunchedOnlyIfDirectoryIsStillOrphaned(boolean orphaned) {
    JobsClient jobsClient = Mockito.mock(JobsClient.class);
    TablesClient tablesClient = Mockito.mock(TablesClient.class);
    DirectoryMetadata metadata =
        DirectoryMetadata.builder()
            .path(new Path("/data/openhouse/db/tb-" + UUID.randomUUID()))
            .build();
    Mockito.when(tablesClient.isOrphanTableDirectory(metadata.getPath())).thenReturn(orphaned);
    Mockito.when(
            jobsClient.launch(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.any(),
                ArgumentMatchers.anyString(),
                ArgumentMatchers.anyList()))
        .thenReturn(Optional.of("job-id"));
    OrphanTableDirectoryDeletionTask task =
        new OrphanTableDirectoryDeletionTask(jobsClient, tablesClient, metadata);
    task.setOperationMode(OperationMode.SUBMIT);
    task.setJobInfoManager(new JobInfoManager(OrphanTableDirectoryDeletionTask.OPERATION_TYPE));
    task.setOtelEmitter(new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry())));

    Assertions.assertEquals(
        orphaned ? Optional.of(JobState.SUBMITTED) : Optional.empty(), task.call());
    Mockito.verify(jobsClient, Mockito.times(orphaned ? 1 : 0))
        .launch(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any(),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyList());
  }
}
