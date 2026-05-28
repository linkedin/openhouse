package com.linkedin.openhouse.cluster.configs;

import java.util.List;
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

  @Value("${cluster.housetables.database.url:jdbc:h2:mem:htsdb;MODE=MYSQL;DB_CLOSE_DELAY=-1}")
  private String clusterHouseTablesDatabaseUrl;

  @Value("${HTS_DB_USER:}")
  private String clusterHouseTablesDatabaseUsername;

  @Value("${HTS_DB_PASSWORD:}")
  private String clusterHouseTablesDatabasePassword;

  // MySQL SSL/Certificate configuration for certificate-based authentication
  @Value("${cluster.housetables.database.cert-based-auth.enabled:false}")
  private boolean clusterHouseTablesDatabaseCertBasedAuthEnabled;

  @Value("${cluster.housetables.database.cert-based-auth.ssl-mode:VERIFY_IDENTITY}")
  private String clusterHouseTablesDatabaseCertBasedAuthSslMode;

  @Value("${cluster.housetables.database.cert-based-auth.client-cert-keystore-url:#{null}}")
  private String clusterHouseTablesDatabaseCertBasedAuthClientCertKeystoreUrl;

  @Value("${cluster.housetables.database.cert-based-auth.client-cert-keystore-password:#{null}}")
  private String clusterHouseTablesDatabaseCertBasedAuthClientCertKeystorePassword;

  @Value("${cluster.housetables.database.cert-based-auth.truststore-url:#{null}}")
  private String clusterHouseTablesDatabaseCertBasedAuthTruststoreUrl;

  @Value("${cluster.housetables.database.cert-based-auth.truststore-password:#{null}}")
  private String clusterHouseTablesDatabaseCertBasedAuthTruststorePassword;

  @Value("${cluster.security.token.interceptor.classname:#{null}}")
  private String clusterSecurityTokenInterceptorClassname;

  @Value("${cluster.security.tables.authorization.enabled:false}")
  private boolean clusterSecurityTablesAuthorizationEnabled;

  @Value("${cluster.security.tables.authorization.opa.base-uri:#{null}}")
  private String clusterSecurityTablesAuthorizationOpaBaseUri;

  // due to springboot lack of yaml list support, lists are represented in yaml as a comma separated
  // string
  @Value("${cluster.tables.allowed-client-name-values:}")
  private List<String> allowedClientNameValues;
}
