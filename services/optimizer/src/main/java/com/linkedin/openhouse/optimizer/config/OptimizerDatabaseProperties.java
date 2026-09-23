package com.linkedin.openhouse.optimizer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Optimizer-only database settings loaded from the cluster configuration. */
@Getter
@Setter
@ConfigurationProperties("cluster.optimizer.database")
public class OptimizerDatabaseProperties {

  public enum DatabaseType {
    IN_MEMORY,
    MYSQL
  }

  private DatabaseType type;
  private String url;
  private final CertBasedAuth certBasedAuth = new CertBasedAuth();

  @Getter
  @Setter
  public static class CertBasedAuth {
    private boolean enabled;
    private String sslMode = "VERIFY_IDENTITY";
    private String clientCertKeystoreUrl;
    private String clientCertKeystorePassword;
    private String truststoreUrl;
    private String truststorePassword;
  }
}
