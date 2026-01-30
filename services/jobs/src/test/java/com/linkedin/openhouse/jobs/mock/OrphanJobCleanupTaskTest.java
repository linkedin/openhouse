package com.linkedin.openhouse.jobs.mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.model.JobDto;
import com.linkedin.openhouse.jobs.repository.JobsInternalRepository;
import com.linkedin.openhouse.jobs.scheduler.OrphanJobCleanupTask;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class OrphanJobCleanupTaskTest {

  @Mock private JobsInternalRepository jobsRepository;

  @InjectMocks private OrphanJobCleanupTask cleanupTask;

  private static final long CURRENT_TIME_MS = Instant.now().toEpochMilli();
  private static final long ELEVEN_MINUTES_AGO = CURRENT_TIME_MS - (11 * 60 * 1000);
  private static final long FIVE_MINUTES_AGO = CURRENT_TIME_MS - (5 * 60 * 1000);

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testCleanupOrphanedJobsOrphanedQueuedJob() {
    // Given: A job in QUEUED state that hasn't been updated for 11 minutes (orphaned)
    JobDto orphanedJob =
        JobDto.builder()
            .jobId("orphaned-job-1")
            .jobName("test-job")
            .state(JobState.QUEUED)
            .lastUpdateTimeMs(ELEVEN_MINUTES_AGO)
            .creationTimeMs(ELEVEN_MINUTES_AGO)
            .clusterId("cluster1")
            .build();

    when(jobsRepository.findAll()).thenReturn(Arrays.asList(orphanedJob));
    when(jobsRepository.save(any(JobDto.class))).thenAnswer(i -> i.getArguments()[0]);

    // When: Cleanup task runs
    cleanupTask.cleanupOrphanedJobs();

    // Then: Job should be marked as FAILED
    ArgumentCaptor<JobDto> captor = ArgumentCaptor.forClass(JobDto.class);
    verify(jobsRepository, times(1)).save(captor.capture());

    JobDto savedJob = captor.getValue();
    assert savedJob.getState() == JobState.FAILED;
    assert savedJob.getJobId().equals("orphaned-job-1");
  }

  @Test
  public void testCleanupOrphanedJobsOrphanedRunningJob() {
    // Given: A job in RUNNING state that hasn't been updated for 15 minutes (orphaned)
    JobDto orphanedJob =
        JobDto.builder()
            .jobId("orphaned-job-2")
            .jobName("test-job")
            .state(JobState.RUNNING)
            .lastUpdateTimeMs(CURRENT_TIME_MS - (15 * 60 * 1000))
            .creationTimeMs(CURRENT_TIME_MS - (20 * 60 * 1000))
            .clusterId("cluster1")
            .build();

    when(jobsRepository.findAll()).thenReturn(Arrays.asList(orphanedJob));
    when(jobsRepository.save(any(JobDto.class))).thenAnswer(i -> i.getArguments()[0]);

    // When: Cleanup task runs
    cleanupTask.cleanupOrphanedJobs();

    // Then: Job should be marked as FAILED
    ArgumentCaptor<JobDto> captor = ArgumentCaptor.forClass(JobDto.class);
    verify(jobsRepository, times(1)).save(captor.capture());

    JobDto savedJob = captor.getValue();
    assert savedJob.getState() == JobState.FAILED;
  }

  @Test
  public void testCleanupOrphanedJobsRecentJobNotOrphaned() {
    // Given: A job in QUEUED state that was updated 5 minutes ago (NOT orphaned)
    JobDto recentJob =
        JobDto.builder()
            .jobId("recent-job")
            .jobName("test-job")
            .state(JobState.QUEUED)
            .lastUpdateTimeMs(FIVE_MINUTES_AGO)
            .creationTimeMs(FIVE_MINUTES_AGO)
            .clusterId("cluster1")
            .build();

    when(jobsRepository.findAll()).thenReturn(Arrays.asList(recentJob));

    // When: Cleanup task runs
    cleanupTask.cleanupOrphanedJobs();

    // Then: Job should NOT be updated
    verify(jobsRepository, never()).save(any(JobDto.class));
  }

  @Test
  public void testCleanupOrphanedJobsTerminalStateNotOrphaned() {
    // Given: Jobs in terminal states (SUCCEEDED, FAILED, CANCELLED) with old timestamps
    List<JobDto> terminalJobs =
        Arrays.asList(
            JobDto.builder()
                .jobId("succeeded-job")
                .state(JobState.SUCCEEDED)
                .lastUpdateTimeMs(ELEVEN_MINUTES_AGO)
                .clusterId("cluster1")
                .build(),
            JobDto.builder()
                .jobId("failed-job")
                .state(JobState.FAILED)
                .lastUpdateTimeMs(ELEVEN_MINUTES_AGO)
                .clusterId("cluster1")
                .build(),
            JobDto.builder()
                .jobId("cancelled-job")
                .state(JobState.CANCELLED)
                .lastUpdateTimeMs(ELEVEN_MINUTES_AGO)
                .clusterId("cluster1")
                .build());

    when(jobsRepository.findAll()).thenReturn(terminalJobs);

    // When: Cleanup task runs
    cleanupTask.cleanupOrphanedJobs();

    // Then: No jobs should be updated (terminal states are not orphans)
    verify(jobsRepository, never()).save(any(JobDto.class));
  }

  @Test
  public void testCleanupOrphanedJobsMixedJobs() {
    // Given: Mix of orphaned and non-orphaned jobs
    List<JobDto> jobs =
        Arrays.asList(
            // Orphaned jobs (should be marked as FAILED)
            JobDto.builder()
                .jobId("orphan-1")
                .state(JobState.QUEUED)
                .lastUpdateTimeMs(ELEVEN_MINUTES_AGO)
                .clusterId("cluster1")
                .build(),
            JobDto.builder()
                .jobId("orphan-2")
                .state(JobState.RUNNING)
                .lastUpdateTimeMs(ELEVEN_MINUTES_AGO)
                .clusterId("cluster1")
                .build(),
            // Non-orphaned jobs (should NOT be updated)
            JobDto.builder()
                .jobId("recent")
                .state(JobState.QUEUED)
                .lastUpdateTimeMs(FIVE_MINUTES_AGO)
                .clusterId("cluster1")
                .build(),
            JobDto.builder()
                .jobId("terminal")
                .state(JobState.SUCCEEDED)
                .lastUpdateTimeMs(ELEVEN_MINUTES_AGO)
                .clusterId("cluster1")
                .build());

    when(jobsRepository.findAll()).thenReturn(jobs);
    when(jobsRepository.save(any(JobDto.class))).thenAnswer(i -> i.getArguments()[0]);

    // When: Cleanup task runs
    cleanupTask.cleanupOrphanedJobs();

    // Then: Only 2 orphaned jobs should be marked as FAILED
    ArgumentCaptor<JobDto> captor = ArgumentCaptor.forClass(JobDto.class);
    verify(jobsRepository, times(2)).save(captor.capture());

    List<JobDto> savedJobs = captor.getAllValues();
    assert savedJobs.size() == 2;
    assert savedJobs.stream().allMatch(job -> job.getState() == JobState.FAILED);
    assert savedJobs.stream().anyMatch(job -> job.getJobId().equals("orphan-1"));
    assert savedJobs.stream().anyMatch(job -> job.getJobId().equals("orphan-2"));
  }

  @Test
  public void testCleanupOrphanedJobsEmptyJobList() {
    // Given: No jobs in the system
    when(jobsRepository.findAll()).thenReturn(Arrays.asList());

    // When: Cleanup task runs
    cleanupTask.cleanupOrphanedJobs();

    // Then: No updates should be made
    verify(jobsRepository, never()).save(any(JobDto.class));
  }

  @Test
  public void testCleanupOrphanedJobsUpdatedJobHasCorrectTimestamps() {
    // Given: An orphaned job
    JobDto orphanedJob =
        JobDto.builder()
            .jobId("orphan-with-timestamps")
            .state(JobState.QUEUED)
            .lastUpdateTimeMs(ELEVEN_MINUTES_AGO)
            .creationTimeMs(CURRENT_TIME_MS - (30 * 60 * 1000))
            .startTimeMs(0L)
            .finishTimeMs(0L)
            .clusterId("cluster1")
            .build();

    when(jobsRepository.findAll()).thenReturn(Arrays.asList(orphanedJob));
    when(jobsRepository.save(any(JobDto.class))).thenAnswer(i -> i.getArguments()[0]);

    // When: Cleanup task runs
    cleanupTask.cleanupOrphanedJobs();

    // Then: Updated job should have current timestamps for lastUpdateTimeMs and finishTimeMs
    ArgumentCaptor<JobDto> captor = ArgumentCaptor.forClass(JobDto.class);
    verify(jobsRepository, times(1)).save(captor.capture());

    JobDto savedJob = captor.getValue();
    assert savedJob.getState() == JobState.FAILED;
    assert savedJob.getLastUpdateTimeMs() > ELEVEN_MINUTES_AGO;
    assert savedJob.getFinishTimeMs() > ELEVEN_MINUTES_AGO;
    // Creation and start times should be preserved
    assert savedJob.getCreationTimeMs() == orphanedJob.getCreationTimeMs();
  }
}
