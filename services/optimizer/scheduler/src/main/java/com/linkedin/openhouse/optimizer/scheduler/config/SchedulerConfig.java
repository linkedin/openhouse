package com.linkedin.openhouse.optimizer.scheduler.config;

import com.linkedin.openhouse.optimizer.binpack.FirstFitBinPacker;
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
   * Orphan files deletion: a {@link FirstFitBinPacker} over {@link TotalFilesBinItem}. Cost scales
   * with file count — per-file list, manifest joins, and delete calls dominate independent of file
   * size.
   */
  @Bean
  public SchedulerRunner schedulerRunner(
      TableOperationsRepository operationsRepo,
      TableStatsRepository statsRepo,
      JobsServiceClient jobsClient,
      @Value("${optimizer.scheduler.results-endpoint}") String resultsEndpoint,
      @Value("${optimizer.scheduler.ofd.max-files-per-bin}") long ofdMaxFilesPerBin,
      @Value("${optimizer.scheduler.ofd.max-tables-per-bin}") int ofdMaxTablesPerBin) {
    return new SchedulerRunner(operationsRepo, statsRepo, jobsClient, resultsEndpoint)
        .registerOperation(
            OperationTypeDto.ORPHAN_FILES_DELETION,
            new FirstFitBinPacker<>(TotalFilesBinItem::new, ofdMaxFilesPerBin, ofdMaxTablesPerBin));
  }
}
