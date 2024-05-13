package com.linkedin.openhouse.cluster.storage.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Configuration class to conditionally enable/disable creation of {@link
 * HdfsDelegationTokenRefresher} bean and enable delegation token refresh scheduling.
 */
@Slf4j
@Configuration
@EnableScheduling
public class HdfsDelegationTokenRefresherConfig {

  @Autowired private HdfsStorage hdfsStorage;

  private static final String HDFS_TOKEN_REFRESH_ENABLED = "token.refresh.enabled";

  /**
   * Conditionally provide the HdfsDelegationTokenRefresher bean if the parameter for token refresh
   * is enabled in the HdfsStorage properties. The relevant configuration in the cluster YAML file
   * is as follows:
   *
   * <pre>
   * cluster:
   *   storages:
   *     hdfs:
   *       parameter:
   *         token.refresh.enabled: true
   * </pre>
   *
   * @return HdfsDelegationTokenRefresher
   */
  @Bean
  public HdfsDelegationTokenRefresher getDelegationTokenRefresher() {
    if (!hdfsStorage.isConfigured()) {
      log.debug(
          "Hdfs storage is not configured, ignoring HdfsDelegationTokenRefresher bean creation");
      return null;
    }
    String refreshEnabled =
        hdfsStorage.getProperties().getOrDefault(HDFS_TOKEN_REFRESH_ENABLED, "false");
    if (Boolean.parseBoolean(refreshEnabled)) {
      log.info("Creating HdfsDelegationTokenRefresher bean");
      return new HdfsDelegationTokenRefresher();
    } else {
      log.debug(
          "Hdfs storage token refresh is not enabled, ignoring HdfsDelegationTokenRefresher bean creation");
      return null;
    }
  }
}
