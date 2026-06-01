package com.linkedin.openhouse.optimizer.scheduler.config;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
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

  /** OFD bin packer: max files per bin (primary cost dimension). 0 disables. */
  @Value("${optimizer.scheduler.ofd.max-weight-per-bin:1000000}")
  private long ofdMaxWeightPerBin;

  /** OFD bin packer: max on-disk size per bin in bytes. 0 disables. */
  @Value("${optimizer.scheduler.ofd.max-size-bytes-per-bin:5497558138880}")
  private long ofdMaxSizeBytesPerBin;

  /** OFD bin packer: max tables per bin. 0 disables. */
  @Value("${optimizer.scheduler.ofd.max-items-per-bin:50}")
  private int ofdMaxItemsPerBin;

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
   * operation type means adding an entry here and configuring its packer caps; the packer itself
   * stays generic over {@link com.linkedin.openhouse.optimizer.scheduler.binpack.BinItem}.
   */
  @Bean
  public Map<OperationTypeDto, BinPacker> binPackers() {
    return Map.of(
        OperationTypeDto.ORPHAN_FILES_DELETION,
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(ofdMaxWeightPerBin)
            .maxSizeBytesPerBin(ofdMaxSizeBytesPerBin)
            .maxItemsPerBin(ofdMaxItemsPerBin)
            .build());
  }
}
