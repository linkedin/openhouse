package com.linkedin.openhouse.optimizer.scheduler.config;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.scheduler.BinPacker;
import com.linkedin.openhouse.optimizer.scheduler.FileCountBinPacker;
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

  @Value("${optimizer.scheduler.ofd.max-files-per-bin}")
  private long ofdMaxFilesPerBin;

  @Bean
  public WebClient jobsWebClient() {
    return WebClient.builder().baseUrl(jobsBaseUri).build();
  }

  @Bean
  public JobsServiceClient jobsServiceClient(WebClient jobsWebClient) {
    return new JobsServiceClient(jobsWebClient, clusterId);
  }

  /**
   * Map of {@link OperationTypeDto} to the {@link BinPacker} strategy that handles it. Adding a new
   * operation type means adding an entry here and configuring its packer; the strategy class itself
   * stays generic.
   */
  @Bean
  public Map<OperationTypeDto, BinPacker> binPackers() {
    return Map.of(
        OperationTypeDto.ORPHAN_FILES_DELETION,
        new FileCountBinPacker(OperationTypeDto.ORPHAN_FILES_DELETION, ofdMaxFilesPerBin));
  }
}
