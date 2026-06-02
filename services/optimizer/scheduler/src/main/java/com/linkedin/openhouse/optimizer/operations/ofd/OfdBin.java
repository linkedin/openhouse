package com.linkedin.openhouse.optimizer.operations.ofd;

import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.scheduler.binpack.Bin;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

/**
 * A single OFD batch: a group of operations that will be submitted together as one batched
 * orphan-files-deletion Spark job. Claims its operations via CAS, narrows to the rows it actually
 * owns, launches the Spark job, and marks SCHEDULED or reverts to PENDING based on launch outcome.
 */
@Slf4j
public class OfdBin implements Bin {
  private final List<OfdBinItem> items;
  private final TableOperationsRepository operationsRepo;
  private final JobsServiceClient jobsClient;
  private final String resultsEndpoint;

  public OfdBin(
      List<OfdBinItem> items,
      TableOperationsRepository operationsRepo,
      JobsServiceClient jobsClient,
      String resultsEndpoint) {
    this.items = items;
    this.operationsRepo = operationsRepo;
    this.jobsClient = jobsClient;
    this.resultsEndpoint = resultsEndpoint;
  }

  @Override
  @Transactional
  public void schedule() {
    List<String> ids = items.stream().map(OfdBinItem::getOperationId).collect(Collectors.toList());

    // Claim in one batched UPDATE: PENDING → SCHEDULING. The aggregate row count alone doesn't
    // tell us *which* rows we own; re-query for SCHEDULING rows tagged with our scheduledAt
    // watermark to get that exact set.
    Instant claimedAt = Instant.now();
    operationsRepo.updateBatch(
        ids,
        OperationStatus.PENDING,
        OperationStatus.SCHEDULING,
        Optional.of(claimedAt),
        Optional.empty());
    List<String> claimedIds =
        operationsRepo
            .find(
                Optional.empty(),
                Optional.of(OperationStatus.SCHEDULING),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(claimedAt),
                Optional.of(ids),
                Pageable.unpaged())
            .stream()
            .map(TableOperationsRow::getId)
            .collect(Collectors.toList());
    if (claimedIds.isEmpty()) {
      log.info("All rows in bin already claimed by another scheduler instance; skipping");
      return;
    }
    if (claimedIds.size() < ids.size()) {
      log.info(
          "Partial claim: {} of {} ops in bin claimed; launching job for claimed subset only",
          claimedIds.size(),
          ids.size());
    }

    Set<String> claimedSet = new HashSet<>(claimedIds);
    List<OfdBinItem> claimedItems =
        items.stream()
            .filter(item -> claimedSet.contains(item.getOperationId()))
            .collect(Collectors.toList());
    List<String> tableNames =
        claimedItems.stream().map(OfdBinItem::getFqtn).collect(Collectors.toList());
    List<String> operationIds =
        claimedItems.stream().map(OfdBinItem::getOperationId).collect(Collectors.toList());

    String opTypeName = OperationTypeDto.ORPHAN_FILES_DELETION.name();
    String jobName = "batched-" + opTypeName.toLowerCase() + "-" + claimedAt.toEpochMilli();
    Optional<String> jobId =
        jobsClient.launch(jobName, opTypeName, tableNames, operationIds, resultsEndpoint);

    if (jobId.isPresent()) {
      int updated =
          operationsRepo.updateBatch(
              claimedIds,
              OperationStatus.SCHEDULING,
              OperationStatus.SCHEDULED,
              Optional.empty(),
              Optional.of(jobId.get()));
      log.info(
          "Submitted job {} for {} tables ({} rows marked SCHEDULED)",
          jobId.get(),
          claimedItems.size(),
          updated);
    } else {
      int reverted =
          operationsRepo.updateBatch(
              claimedIds,
              OperationStatus.SCHEDULING,
              OperationStatus.PENDING,
              Optional.empty(),
              Optional.empty());
      log.warn(
          "Job submission failed; reverted {} claimed rows back to PENDING for retry on the next"
              + " pass",
          reverted);
    }
  }
}
