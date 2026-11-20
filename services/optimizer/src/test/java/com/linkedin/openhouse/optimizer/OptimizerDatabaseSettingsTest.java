package com.linkedin.openhouse.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * The database setup that DEPLOY.md describes. A deployment sets {@code OPTIMIZER_DB_URL}, {@code
 * OPTIMIZER_DB_USER}, and {@code OPTIMIZER_DB_PASSWORD}, and turns on certificate authentication
 * with Connector/J properties in the URL.
 */
class OptimizerDatabaseSettingsTest {

  private static final String URL =
      "jdbc:mysql://mysql:3306/oh_db"
          + "?sslMode=VERIFY_IDENTITY"
          + "&clientCertificateKeyStoreUrl=file:/path/identity.p12"
          + "&clientCertificateKeyStorePassword=identity-password"
          + "&trustCertificateKeyStoreUrl=file:/path/cacerts"
          + "&trustCertificateKeyStorePassword=cacerts-password";

  @Test
  void configuresTheDatabaseFromOptimizerDbVariables() throws SQLException {
    new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withConfiguration(AutoConfigurations.of(DataSourceAutoConfiguration.class))
        .withPropertyValues(
            "OPTIMIZER_DB_URL=" + URL,
            "OPTIMIZER_DB_USER=optimizer",
            "OPTIMIZER_DB_PASSWORD=optimizer-password")
        .run(
            context -> {
              HikariDataSource dataSource = context.getBean(HikariDataSource.class);
              assertThat(dataSource.getJdbcUrl()).isEqualTo(URL);
              assertThat(dataSource.getUsername()).isEqualTo("optimizer");
              assertThat(dataSource.getPassword()).isEqualTo("optimizer-password");
            });

    List<String> driverSettings =
        Arrays.stream(DriverManager.getDriver(URL).getPropertyInfo(URL, new Properties()))
            .map(setting -> setting.name + "=" + setting.value)
            .collect(Collectors.toList());
    assertThat(driverSettings)
        .as("Connector/J reads the certificate settings from the URL")
        .contains(
            "sslMode=VERIFY_IDENTITY",
            "clientCertificateKeyStoreUrl=file:/path/identity.p12",
            "trustCertificateKeyStoreUrl=file:/path/cacerts");
  }
}
