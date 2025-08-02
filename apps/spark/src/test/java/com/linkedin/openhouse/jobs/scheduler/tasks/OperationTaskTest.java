package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.OtelEmitter;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobResponseBody;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import io.opentelemetry.api.common.Attributes;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@Disabled(
    "This class is disabled as it is not stable. Enable it to test operation task timeout locally")
public class OperationTaskTest {
  @Mock JobsClient jobsClient;
  @Mock TablesClient tablesClient;
  @Mock TableMetadata metadata;

  private final OtelEmitter otelEmitter = AppsOtelEmitter.getInstance();

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testPollJobStatusQueuedTimeout() {
    // setup
    String jobId = "job-123";
    Mockito.when(jobsClient.getState(jobId))
        .thenReturn(Optional.of(JobState.QUEUED))
        .thenReturn(Optional.of(JobState.QUEUED))
        .thenReturn(Optional.of(JobState.QUEUED));
    JobResponseBody jobResponseBody =
        createJobResponseBody(JobResponseBody.StateEnum.QUEUED, 0L, 0L, 0L);
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Attributes attributes = Attributes.builder().build();
    // test
    TableSnapshotsExpirationTask task =
        new TableSnapshotsExpirationTask(jobsClient, tablesClient, metadata, 1000, 2000, 3000);
    task.setJobId(jobId);
    task.setOtelEmitter(otelEmitter);
    long startTime = System.currentTimeMillis();
    Optional<JobState> result = task.pollJobStatus(attributes);
    long endTime = System.currentTimeMillis();
    // verify
    Assertions.assertEquals(result, Optional.of(JobState.QUEUED));
    Assertions.assertTrue(
        endTime - startTime > 2000 && endTime - startTime < 3000,
        "Task should have waited for between 2s and 3s");
  }

  @Test
  public void testPollJobStatusTaskTimeout() {
    // setup
    String jobId = "job-123";
    Mockito.when(jobsClient.getState(jobId))
        .thenReturn(Optional.of(JobState.QUEUED))
        .thenReturn(Optional.of(JobState.RUNNING))
        .thenReturn(Optional.of(JobState.RUNNING))
        .thenReturn(Optional.of(JobState.RUNNING));
    JobResponseBody jobResponseBody =
        createJobResponseBody(JobResponseBody.StateEnum.RUNNING, 0L, 0L, 0L);
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Attributes attributes = Attributes.builder().build();
    // test
    TableSnapshotsExpirationTask task =
        new TableSnapshotsExpirationTask(jobsClient, tablesClient, metadata, 1000, 2000, 3000);
    task.setJobId(jobId);
    task.setOtelEmitter(otelEmitter);
    long startTime = System.currentTimeMillis();
    Optional<JobState> result = task.pollJobStatus(attributes);
    long endTime = System.currentTimeMillis();
    // verify
    Assertions.assertEquals(result, Optional.of(JobState.RUNNING));
    Assertions.assertTrue(
        endTime - startTime > 3000 && endTime - startTime < 4000,
        "Task should have waited for between 3s and 4s");
  }

  @Test
  public void testPollJobStatusSchedulerTimeout()
      throws ExecutionException, InterruptedException, TimeoutException {
    // setup
    String jobId = "job-123";
    Mockito.when(jobsClient.getState(jobId))
        .thenReturn(Optional.of(JobState.QUEUED))
        .thenReturn(Optional.of(JobState.RUNNING))
        .thenReturn(Optional.of(JobState.RUNNING))
        .thenReturn(Optional.of(JobState.SUCCEEDED));
    JobResponseBody jobResponseBody =
        createJobResponseBody(JobResponseBody.StateEnum.SUCCEEDED, 0L, 0L, 0L);
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Attributes attributes = Attributes.builder().build();
    // test
    TableSnapshotsExpirationTask task =
        new TableSnapshotsExpirationTask(jobsClient, tablesClient, metadata, 1000, 2000, 3000);
    task.setJobId(jobId);
    task.setOtelEmitter(otelEmitter);
    FutureTask<Optional<JobState>> future = new FutureTask<>(() -> task.pollJobStatus(attributes));
    Thread thread = new Thread(future);
    thread.start();
    // Sleep 2 seconds and interrupt the task. The task should complete successfully.
    Thread.sleep(2000);
    thread.interrupt();
    Optional<JobState> result = future.get(2, TimeUnit.SECONDS);
    // verify
    Assertions.assertEquals(result, Optional.of(JobState.SUCCEEDED));
  }

  @Test
  public void testPollJobStatusFinishedJob() {
    // setup
    String jobId = "job-123";
    Mockito.when(jobsClient.getState(jobId))
        .thenReturn(Optional.of(JobState.QUEUED))
        .thenReturn(Optional.of(JobState.RUNNING))
        .thenReturn(Optional.of(JobState.SUCCEEDED));
    JobResponseBody jobResponseBody =
        createJobResponseBody(JobResponseBody.StateEnum.SUCCEEDED, 0L, 0L, 0L);
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Attributes attributes = Attributes.builder().build();
    // test
    TableSnapshotsExpirationTask task =
        new TableSnapshotsExpirationTask(jobsClient, tablesClient, metadata, 1000, 2000, 3000);
    task.setJobId(jobId);
    task.setOtelEmitter(otelEmitter);
    long startTime = System.currentTimeMillis();
    Optional<JobState> result = task.pollJobStatus(attributes);
    long endTime = System.currentTimeMillis();
    // verify
    Assertions.assertEquals(result, Optional.of(JobState.SUCCEEDED));
    Assertions.assertTrue(
        endTime - startTime > 3000 && endTime - startTime < 4000,
        "Task should have waited for between 3s and 4s");
  }

  private JobResponseBody createJobResponseBody(
      JobResponseBody.StateEnum state, long finishTimeMs, long startTimeMs, long creationTimeMs) {
    JobResponseBody jobResponseBody = new JobResponseBody();
    jobResponseBody.setState(state);
    jobResponseBody.setFinishTimeMs(finishTimeMs);
    jobResponseBody.setStartTimeMs(startTimeMs);
    jobResponseBody.setCreationTimeMs(creationTimeMs);
    return jobResponseBody;
  }
}
