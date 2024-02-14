package com.linkedin.openhouse.cluster.storage.filesystem;

import com.linkedin.openhouse.cluster.configs.YamlPropertySourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Configuration class to conditionally enable/disable creation of {@link
 * com.linkedin.openhouse.cluster.storage.filesystem.DelegationTokenRefresher} bean and enable
 * delegation token refresh scheduling.
 */
@Configuration
@EnableScheduling
@Slf4j
@PropertySource(
    name = "cluster",
    value = "file:${OPENHOUSE_CLUSTER_CONFIG_PATH:/var/config/cluster.yaml}",
    factory = YamlPropertySourceFactory.class,
    ignoreResourceNotFound = true)
public class DelegationTokenRefreshSchedulingConfig {

  /**
   * ConditionalOnProperty annotation works directly on the property defined in the config file i.e.
   * cluster.yaml and {@link com.linkedin.openhouse.cluster.configs.ClusterProperties} can't be used
   * here directly. Create DelegationTokenRefresher bean if the property value is defined as true,
   * otherwise don't create.
   *
   * @return DelegationTokenRefresher
   */
  @Bean
  @ConditionalOnProperty(
      value = "cluster.storage.hadoop.token.refresh.enabled",
      havingValue = "true")
  public DelegationTokenRefresher getDelegationTokenRefresher() {
    log.info("Creating DelegationTokenRefresher bean.....");
    return new DelegationTokenRefresher();
  }
}
