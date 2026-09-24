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

  @Value("${cluster.iceberg.write.orc.compression-codec:#{null}}")
  private String clusterIcebergWriteOrcCompressionCodec;

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

  @Value("${cluster.metadata.database.url:${cluster.housetables.database.url:}}")
  private String clusterMetadataDatabaseUrl;

  @Value("${cluster.metadata.database.username:${OPENHOUSE_DB_USER:${HTS_DB_USER:}}}")
  private String clusterMetadataDatabaseUsername;

  @Value("${cluster.metadata.database.password:${OPENHOUSE_DB_PASSWORD:${HTS_DB_PASSWORD:}}}")
  private String clusterMetadataDatabasePassword;

  // MySQL SSL/Certificate configuration for certificate-based authentication
  @Value(
      "${cluster.metadata.database.cert-based-auth.enabled:"
          + "${cluster.housetables.database.cert-based-auth.enabled:false}}")
  private boolean clusterMetadataDatabaseCertBasedAuthEnabled;

  @Value(
      "${cluster.metadata.database.cert-based-auth.ssl-mode:"
          + "${cluster.housetables.database.cert-based-auth.ssl-mode:VERIFY_IDENTITY}}")
  private String clusterMetadataDatabaseCertBasedAuthSslMode;

  @Value(
      "${cluster.metadata.database.cert-based-auth.client-cert-keystore-url:"
          + "${cluster.housetables.database.cert-based-auth.client-cert-keystore-url:#{null}}}")
  private String clusterMetadataDatabaseCertBasedAuthClientCertKeystoreUrl;

  @Value(
      "${cluster.metadata.database.cert-based-auth.client-cert-keystore-password:"
          + "${cluster.housetables.database.cert-based-auth.client-cert-keystore-password:"
          + "#{null}}}")
  private String clusterMetadataDatabaseCertBasedAuthClientCertKeystorePassword;

  @Value(
      "${cluster.metadata.database.cert-based-auth.truststore-url:"
          + "${cluster.housetables.database.cert-based-auth.truststore-url:#{null}}}")
  private String clusterMetadataDatabaseCertBasedAuthTruststoreUrl;

  @Value(
      "${cluster.metadata.database.cert-based-auth.truststore-password:"
          + "${cluster.housetables.database.cert-based-auth.truststore-password:#{null}}}")
  private String clusterMetadataDatabaseCertBasedAuthTruststorePassword;

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

  /** @deprecated Use {@link #getClusterMetadataDatabaseUrl()}. */
  @Deprecated
  public String getClusterHouseTablesDatabaseUrl() {
    return clusterMetadataDatabaseUrl;
  }

  /** @deprecated Use {@link #getClusterMetadataDatabaseUsername()}. */
  @Deprecated
  public String getClusterHouseTablesDatabaseUsername() {
    return clusterMetadataDatabaseUsername;
  }

  /** @deprecated Use {@link #getClusterMetadataDatabasePassword()}. */
  @Deprecated
  public String getClusterHouseTablesDatabasePassword() {
    return clusterMetadataDatabasePassword;
  }

  /** @deprecated Use {@link #isClusterMetadataDatabaseCertBasedAuthEnabled()}. */
  @Deprecated
  public boolean isClusterHouseTablesDatabaseCertBasedAuthEnabled() {
    return clusterMetadataDatabaseCertBasedAuthEnabled;
  }

  /** @deprecated Use {@link #getClusterMetadataDatabaseCertBasedAuthSslMode()}. */
  @Deprecated
  public String getClusterHouseTablesDatabaseCertBasedAuthSslMode() {
    return clusterMetadataDatabaseCertBasedAuthSslMode;
  }

  /** @deprecated Use {@link #getClusterMetadataDatabaseCertBasedAuthClientCertKeystoreUrl()}. */
  @Deprecated
  public String getClusterHouseTablesDatabaseCertBasedAuthClientCertKeystoreUrl() {
    return clusterMetadataDatabaseCertBasedAuthClientCertKeystoreUrl;
  }

  /**
   * @deprecated Use {@link #getClusterMetadataDatabaseCertBasedAuthClientCertKeystorePassword()}.
   */
  @Deprecated
  public String getClusterHouseTablesDatabaseCertBasedAuthClientCertKeystorePassword() {
    return clusterMetadataDatabaseCertBasedAuthClientCertKeystorePassword;
  }

  /** @deprecated Use {@link #getClusterMetadataDatabaseCertBasedAuthTruststoreUrl()}. */
  @Deprecated
  public String getClusterHouseTablesDatabaseCertBasedAuthTruststoreUrl() {
    return clusterMetadataDatabaseCertBasedAuthTruststoreUrl;
  }

  /** @deprecated Use {@link #getClusterMetadataDatabaseCertBasedAuthTruststorePassword()}. */
  @Deprecated
  public String getClusterHouseTablesDatabaseCertBasedAuthTruststorePassword() {
    return clusterMetadataDatabaseCertBasedAuthTruststorePassword;
  }
}
