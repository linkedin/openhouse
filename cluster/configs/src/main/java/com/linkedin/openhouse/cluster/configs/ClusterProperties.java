package com.linkedin.openhouse.cluster.configs;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/** Class to load the Runtime Configuration from a YAML file into ClusterProperties object. */
@Configuration
@PropertySource(
    name = "cluster",
    value = "file:${OPENHOUSE_CLUSTER_CONFIG_PATH:/var/config/cluster.yaml}",
    factory = YamlPropertySourceFactory.class,
    ignoreResourceNotFound = true)
@Getter
public class ClusterProperties {

  @Value("${cluster.name:local-cluster}")
  private String clusterName;

  @Value("${cluster.storage.type:hadoop}")
  private String clusterStorageType;

  @Value("${cluster.storage.uri:#{null}}")
  private String clusterStorageURI;

  @Value("${cluster.storage.root-path:/tmp}")
  private String clusterStorageRootPath;

  @Value("${cluster.storage.hadoop.config.core-site.path:#{null}}")
  private String clusterStorageHadoopCoreSitePath;

  @Value("${cluster.storage.hadoop.config.hdfs-site.path:#{null}}")
  private String clusterStorageHadoopHdfsSitePath;

  @Value("${cluster.storage.hadoop.token.refresh.schedule.cron:0 0 0/12 * * ?}")
  private String clusterStorageHadoopTokenRefreshScheduleCron;

  @Value("${cluster.iceberg.write.format.default:orc}")
  private String clusterIcebergWriteFormatDefault;

  @Value("${cluster.iceberg.format-version:2}")
  private int clusterIcebergFormatVersion;

  @Value("${cluster.iceberg.write.metadata.delete-after-commit.enabled:false}")
  private boolean clusterIcebergWriteMetadataDeleteAfterCommitEnabled;

  @Value("${cluster.iceberg.write.metadata.previous-versions-max:100}")
  private int clusterIcebergWriteMetadataPreviousVersionsMax;

  @Value("${cluster.housetables.base-uri:#{null}}")
  private String clusterHouseTablesBaseUri;

  @Value("${cluster.housetables.database.type:IN_MEMORY}")
  private String clusterHouseTablesDatabaseType;

  @Value("${cluster.housetables.database.url:jdbc:h2:mem:htsdb;DB_CLOSE_DELAY=-1}")
  private String clusterHouseTablesDatabaseUrl;

  @Value("${HTS_DB_USER:}")
  private String clusterHouseTablesDatabaseUsername;

  @Value("${HTS_DB_PASSWORD:}")
  private String clusterHouseTablesDatabasePassword;

  @Value("${cluster.security.token.interceptor.classname:#{null}}")
  private String clusterSecurityTokenInterceptorClassname;

  @Value("${cluster.security.tables.authorization.enabled:false}")
  private boolean clusterSecurityTablesAuthorizationEnabled;
}
