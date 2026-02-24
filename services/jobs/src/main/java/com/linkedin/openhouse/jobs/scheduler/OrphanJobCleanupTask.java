package com.linkedin.openhouse.jobs.scheduler;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.model.JobDto;
import com.linkedin.openhouse.jobs.repository.JobsInternalRepository;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Background task that runs periodically to detect and mark orphaned jobs as FAILED.
 *
 * <p>An orphaned job is one that:
 *
 * <ul>
 *   <li>Has a non-terminal state (not SUCCEEDED, FAILED, or CANCELLED)
 *   <li>Has not received a heartbeat/update for more than 10 minutes
 * </ul>
 *
 * <p>These jobs typically occur when a job fails to start in the execution engine (e.g., Livy) and
 * therefore never sends state updates or heartbeats.
 */
@Slf4j
@Component
@ConditionalOnProperty(
    value = "scheduler.orphan-job-cleanup.enabled",
    havingValue = "true",
    matchIfMissing = true)
public class OrphanJobCleanupTask {

  private static final long ORPHAN_THRESHOLD_MS = 10 * 60 * 1000; // 10 minutes in milliseconds

  @Autowired private JobsInternalRepository jobsRepository;

  public OrphanJobCleanupTask() {
    System.out.println("========================================");
    System.out.println("OrphanJobCleanupTask bean created - scheduler will start in 5 minutes");
    System.out.println("========================================");
  }

  /**
   * Scheduled task that runs every 5 minutes to detect and mark orphaned jobs as FAILED.
   *
   * <p>Execution schedule: - Initial delay: 5 minutes (to allow service to fully start) - Fixed
   * rate: 5 minutes
   */
  @Scheduled(initialDelay = 5 * 60 * 1000, fixedRate = 5 * 60 * 1000)
  public void cleanupOrphanedJobs() {
    System.out.println("========================================");
    System.out.println("OrphanJobCleanupTask: Starting orphaned job cleanup task");
    System.out.println("========================================");
    log.info("Starting orphaned job cleanup task");
    long startTime = System.currentTimeMillis();

    try {
      // Fetch all jobs from housetables
      Iterable<JobDto> allJobs = jobsRepository.findAll();
      long currentTimeMs = Instant.now().toEpochMilli();
      long orphanThresholdTimeMs = currentTimeMs - ORPHAN_THRESHOLD_MS;

      // Filter for orphaned jobs: non-terminal state and stale lastUpdateTimeMs
      List<JobDto> orphanedJobs =
          StreamSupport.stream(allJobs.spliterator(), false)
              .filter(job -> !job.getState().isTerminal())
              .filter(job -> job.getLastUpdateTimeMs() < orphanThresholdTimeMs)
              .collect(Collectors.toList());

      if (orphanedJobs.isEmpty()) {
        System.out.println("OrphanJobCleanupTask: No orphaned jobs found");
        log.info("No orphaned jobs found");
        return;
      }

      System.out.println(
          "OrphanJobCleanupTask: Found "
              + orphanedJobs.size()
              + " orphaned jobs to mark as FAILED");
      log.info("Found {} orphaned jobs to mark as FAILED", orphanedJobs.size());

      // Update each orphaned job to FAILED state
      int successCount = 0;
      int failureCount = 0;

      for (JobDto orphanedJob : orphanedJobs) {
        try {
          long minutesSinceUpdate =
              (currentTimeMs - orphanedJob.getLastUpdateTimeMs()) / (60 * 1000);

          log.info(
              "Marking orphaned job as FAILED: jobId={}, jobName={}, state={}, "
                  + "minutesSinceLastUpdate={}, executionId={}",
              orphanedJob.getJobId(),
              orphanedJob.getJobName(),
              orphanedJob.getState(),
              minutesSinceUpdate,
              orphanedJob.getExecutionId());

          // Create updated job with FAILED state
          JobDto updatedJob =
              orphanedJob
                  .toBuilder()
                  .state(JobState.FAILED)
                  .lastUpdateTimeMs(currentTimeMs)
                  .finishTimeMs(currentTimeMs)
                  .build();

          jobsRepository.save(updatedJob);
          successCount++;

          log.info("Successfully marked job {} as FAILED", orphanedJob.getJobId());

        } catch (Exception e) {
          failureCount++;
          log.error(
              "Failed to mark orphaned job {} as FAILED: {}",
              orphanedJob.getJobId(),
              e.getMessage(),
              e);
        }
      }

      long duration = System.currentTimeMillis() - startTime;
      System.out.println(
          "OrphanJobCleanupTask: Completed in "
              + duration
              + "ms. Success: "
              + successCount
              + ", Failures: "
              + failureCount
              + ", Total: "
              + orphanedJobs.size());
      log.info(
          "Orphaned job cleanup completed in {}ms. Success: {}, Failures: {}, Total: {}",
          duration,
          successCount,
          failureCount,
          orphanedJobs.size());

    } catch (Exception e) {
      System.err.println("OrphanJobCleanupTask ERROR: " + e.getMessage());
      e.printStackTrace();
      log.error("Error during orphaned job cleanup task: {}", e.getMessage(), e);
    }
  }
}
