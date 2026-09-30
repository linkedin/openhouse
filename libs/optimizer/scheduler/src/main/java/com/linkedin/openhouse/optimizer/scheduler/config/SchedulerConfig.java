package com.linkedin.openhouse.optimizer.scheduler.config;

import com.linkedin.openhouse.optimizer.binpack.FirstFitDecreasingBinPacker;
import com.linkedin.openhouse.optimizer.binpack.TotalFilesBinItem;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.optimizer.scheduler.SchedulerRunner;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Cross-cutting wiring (jobs-service client) plus the {@link SchedulerRunner} bean. Each operation
 * type's identity (type, packing strategy, item supplier) is composed in {@link #schedulerRunner};
 * the runner itself never names an operation type beyond the keys in its registry.
 */
@Configuration
public class SchedulerConfig {

  @Value("${optimizer.scheduler.jobs.base-uri}")
  private String jobsBaseUri;

  @Value("${optimizer.scheduler.cluster-id}")
  private String clusterId;

  @Bean
  public WebClient jobsWebClient() {
    return WebClient.builder().baseUrl(jobsBaseUri).build();
  }

  @Bean
  public JobsServiceClient jobsServiceClient(WebClient jobsWebClient) {
    return new JobsServiceClient(jobsWebClient, clusterId);
  }

  /**
   * Orphan files deletion: a {@link FirstFitDecreasingBinPacker} over {@link TotalFilesBinItem}.
   * Cost scales with file count — per-file list, manifest joins, and delete calls dominate
   * independent of file size.
   *
   * <p>Table stats collection: a {@link FirstFitDecreasingBinPacker} over {@link
   * TotalFilesBinItem}. Its Spark cost is driven by metadata cardinality — the job scans the
   * manifests, files, and all_entries metadata tables and collects them to the driver, so work
   * scales with file count, not table byte size (a few huge files are cheap; millions of tiny files
   * are expensive). Many low-file tables share one job (up to {@code max-tables-per-bin}, default
   * 25), while any table with more than {@code max-files-per-bin} files lands in a job of its own.
   */
  @Bean
  public SchedulerRunner schedulerRunner(
      TableOperationsRepository operationsRepo,
      TableStatsRepository statsRepo,
      JobsServiceClient jobsClient,
      @Value("${optimizer.scheduler.results-endpoint}") String resultsEndpoint,
      @Value("${optimizer.scheduler.ofd.max-files-per-bin}") long ofdMaxFilesPerBin,
      @Value("${optimizer.scheduler.ofd.max-tables-per-bin}") int ofdMaxTablesPerBin,
      @Value("${optimizer.scheduler.stats.max-files-per-bin}") long statsMaxFilesPerBin,
      @Value("${optimizer.scheduler.stats.max-tables-per-bin}") int statsMaxTablesPerBin) {
    return new SchedulerRunner(operationsRepo, statsRepo, jobsClient, resultsEndpoint)
        .registerOperation(
            OperationTypeDto.ORPHAN_FILES_DELETION,
            FirstFitDecreasingBinPacker.<TotalFilesBinItem>builder()
                .binItemSupplier(TotalFilesBinItem::new)
                .maxWeightPerBin(ofdMaxFilesPerBin)
                .maxItemsPerBin(ofdMaxTablesPerBin)
                .build())
        .registerOperation(
            OperationTypeDto.TABLE_STATS_COLLECTION,
            FirstFitDecreasingBinPacker.<TotalFilesBinItem>builder()
                .binItemSupplier(TotalFilesBinItem::new)
                .maxWeightPerBin(statsMaxFilesPerBin)
                .maxItemsPerBin(statsMaxTablesPerBin)
                .build());
  }
}
