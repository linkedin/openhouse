package com.linkedin.openhouse.optimizer.scheduler.config;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.operations.ofd.OfdBinItem;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinItem;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinPacker;
import com.linkedin.openhouse.optimizer.scheduler.binpack.FirstFitDecreasingBinPacker;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class SchedulerConfig {

  @Value("${optimizer.scheduler.jobs.base-uri}")
  private String jobsBaseUri;

  @Value("${optimizer.scheduler.cluster-id}")
  private String clusterId;

  /** Max table-current-file-count summed across one batched OFD Spark job. 0 disables. */
  @Value("${optimizer.scheduler.ofd.max-files-per-bin:1000000}")
  private long ofdMaxFilesPerBin;

  /** Max number of tables per batched OFD Spark job. 0 disables. */
  @Value("${optimizer.scheduler.ofd.max-tables-per-bin:50}")
  private int ofdMaxTablesPerBin;

  @Bean
  public WebClient jobsWebClient() {
    return WebClient.builder().baseUrl(jobsBaseUri).build();
  }

  @Bean
  public JobsServiceClient jobsServiceClient(WebClient jobsWebClient) {
    return new JobsServiceClient(jobsWebClient, clusterId);
  }

  /**
   * Map of {@link OperationTypeDto} to the {@link BinPacker} strategy that handles it. The packer
   * is parametric on the op type's concrete {@link BinItem} impl; the map value uses a wildcard
   * because heterogeneous parametric values aren't expressible directly. {@link
   * com.linkedin.openhouse.optimizer.scheduler.SchedulerRunner} narrows back to the concrete type
   * at dispatch. Adding a new operation type means adding an entry here, an impl of {@link
   * BinItem}, and a {@code scheduleXxx} branch in the runner.
   */
  @Bean
  public Map<OperationTypeDto, BinPacker<? extends BinItem>> binPackers() {
    return Map.of(
        OperationTypeDto.ORPHAN_FILES_DELETION,
        FirstFitDecreasingBinPacker.<OfdBinItem>builder()
            .maxWeightPerBin(ofdMaxFilesPerBin)
            .maxItemsPerBin(ofdMaxTablesPerBin)
            .build());
  }
}
