package com.linkedin.openhouse.scheduler.config;

import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.scheduler.BinPacker;
import com.linkedin.openhouse.scheduler.FileCountBinPacker;
import com.linkedin.openhouse.scheduler.client.JobsServiceClient;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class SchedulerConfig {

  @Value("${jobs.base-uri}")
  private String jobsBaseUri;

  @Value("${scheduler.cluster-id}")
  private String clusterId;

  @Value("${scheduler.ofd.max-files-per-bin}")
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
   * Map of {@link OperationType} to the {@link BinPacker} strategy that handles it. Adding a new
   * operation type means adding an entry here and configuring its packer; the strategy class itself
   * stays generic.
   */
  @Bean
  public Map<OperationType, BinPacker> binPackers() {
    return Map.of(OperationType.ORPHAN_FILES_DELETION, new FileCountBinPacker(ofdMaxFilesPerBin));
  }
}
