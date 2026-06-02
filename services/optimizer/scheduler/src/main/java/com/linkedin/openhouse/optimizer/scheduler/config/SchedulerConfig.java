package com.linkedin.openhouse.optimizer.scheduler.config;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.scheduler.SchedulerRunner;
import com.linkedin.openhouse.optimizer.scheduler.binpack.FirstFitBinPacker;
import com.linkedin.openhouse.optimizer.scheduler.binpack.TotalFilesBinItem;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Cross-cutting wiring (jobs-service client) plus the per-operation-type registrations on the
 * {@link SchedulerRunner}. The {@link #registerOperations()} method is the one place each
 * operation's identity (type, packing strategy, item prototype) is composed; the scheduler itself
 * never names an operation type beyond the keys in its registry.
 */
@Configuration
public class SchedulerConfig {

  @Value("${optimizer.scheduler.jobs.base-uri}")
  private String jobsBaseUri;

  @Value("${optimizer.scheduler.cluster-id}")
  private String clusterId;

  @Value("${optimizer.scheduler.ofd.max-files-per-bin}")
  private long ofdMaxFilesPerBin;

  @Value("${optimizer.scheduler.ofd.max-tables-per-bin}")
  private int ofdMaxTablesPerBin;

  @Autowired private SchedulerRunner schedulerRunner;

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
  @PostConstruct
  public void registerOperations() {
    schedulerRunner.registerOperation(
        OperationTypeDto.ORPHAN_FILES_DELETION,
        new FirstFitBinPacker<>(TotalFilesBinItem::new, ofdMaxFilesPerBin, ofdMaxTablesPerBin));
  }
}
