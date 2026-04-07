package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Reads PENDING rows from the optimizer DB, bin-packs them, and submits one Spark job per bin. */
@Slf4j
@Component
@RequiredArgsConstructor
public class SchedulerRunner {

  private final TableOperationsRepository operationsRepo;
  private final TableStatsRepository statsRepo;
  private final JobsServiceClient jobsClient;

  @Value("${scheduler.bin-size-max-files}")
  private long maxFiles;

  @Value("${scheduler.operation-type}")
  private String operationType;

  @Value("${scheduler.results-endpoint}")
  private String resultsEndpoint;

  @Transactional
  public void schedule() {
    List<TableOperationRow> pending =
        operationsRepo.find(operationType, "PENDING", null, null, null);
    if (pending.isEmpty()) {
      log.info("No PENDING operations of type {}; nothing to schedule", operationType);
      return;
    }

    Set<String> uuids =
        pending.stream().map(TableOperationRow::getTableUuid).collect(Collectors.toSet());

    Map<String, Long> fileCountByUuid =
        statsRepo.findAllById(uuids).stream()
            .collect(
                Collectors.toMap(
                    row -> row.getTableUuid(),
                    row -> {
                      if (row.getStats() == null || row.getStats().getSnapshot() == null) {
                        return 0L;
                      }
                      Long count = row.getStats().getSnapshot().getNumCurrentFiles();
                      return count != null ? count : 0L;
                    }));

    List<List<TableOperationRow>> bins = BinPacker.pack(pending, fileCountByUuid, maxFiles);
    log.info(
        "Packed {} PENDING operations into {} bins (maxFiles={})",
        pending.size(),
        bins.size(),
        maxFiles);

    bins.forEach(this::submitBin);
  }

  private void submitBin(List<TableOperationRow> bin) {
    // 0. Cancel duplicate PENDING rows per (tableUuid, operationType) — keep only the first row.
    bin.stream()
        .collect(Collectors.groupingBy(TableOperationRow::getTableUuid))
        .forEach(
            (uuid, rows) -> {
              TableOperationRow keep = rows.get(0);
              operationsRepo.cancelDuplicatePending(uuid, operationType, keep.getId());
            });

    // 1. Claim all rows (PENDING → SCHEDULING) — prevents a second scheduler instance from
    //    double-submitting. Any row already claimed (returns 0) is excluded from this batch.
    List<TableOperationRow> claimed =
        bin.stream()
            .filter(
                r -> operationsRepo.markScheduling(r.getId(), r.getVersion(), Instant.now()) == 1)
            .collect(Collectors.toList());

    if (claimed.isEmpty()) {
      log.info("All rows in bin already claimed by another scheduler instance; skipping");
      return;
    }

    // 2. Submit a job for the claimed rows only.
    List<String> tableNames =
        claimed.stream()
            .map(r -> r.getDatabaseName() + "." + r.getTableName())
            .collect(Collectors.toList());
    List<String> opIds =
        claimed.stream().map(TableOperationRow::getId).collect(Collectors.toList());

    String jobName = "batched-" + operationType.toLowerCase() + "-" + Instant.now().toEpochMilli();
    Optional<String> jobId =
        jobsClient.launch(jobName, operationType, tableNames, opIds, resultsEndpoint);

    if (jobId.isPresent()) {
      // 3. Transition SCHEDULING → SCHEDULED with the jobId.
      for (TableOperationRow r : claimed) {
        operationsRepo.markScheduled(r.getId(), r.getVersion() + 1, jobId.get());
      }
      log.info("Submitted job {} for {} tables", jobId.get(), claimed.size());
    } else {
      log.warn(
          "Job submission failed for {} SCHEDULING rows; the analyzer's scheduledTimeout will"
              + " detect and overwrite stale rows",
          claimed.size());
    }
  }
}
