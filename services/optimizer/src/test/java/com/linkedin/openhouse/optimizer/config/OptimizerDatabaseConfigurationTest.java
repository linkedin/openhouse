package com.linkedin.openhouse.optimizer.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

class OptimizerDatabaseConfigurationTest {

  @TempDir Path temporaryDirectory;

  private ApplicationContextRunner contextRunner() {
    return new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withConfiguration(AutoConfigurations.of(DataSourceAutoConfiguration.class))
        .withUserConfiguration(OptimizerDatabaseConfiguration.class)
        .withPropertyValues(
            "OPENHOUSE_CLUSTER_CONFIG_PATH=" + temporaryDirectory.resolve("cluster.yaml"));
  }

  @Test
  void retainsServiceDefaultsWhenClusterFileIsMissing() {
    contextRunner()
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(DataSource.class);
              HikariDataSource dataSource = context.getBean(HikariDataSource.class);
              assertThat(dataSource.getJdbcUrl()).isEqualTo("jdbc:mysql://localhost:3306/oh_db");
              assertThat(dataSource.getDriverClassName()).isEqualTo("com.mysql.cj.jdbc.Driver");
              assertThat(dataSource.getMaximumPoolSize()).isEqualTo(20);
              assertThat(dataSource.getDataSourceProperties()).isEmpty();
            });
  }

  @Test
  void retainsOptimizerEnvironmentVariablesWithoutUsingHtsSettings() {
    contextRunner()
        .withPropertyValues(
            "OPTIMIZER_DB_URL=jdbc:mysql://legacy.invalid:3306/optimizer",
            "OPTIMIZER_DB_USER=optimizer_user",
            "OPTIMIZER_DB_PASSWORD=optimizer_test_password",
            "cluster.housetables.database.url=jdbc:mysql://hts.invalid:3306/hts",
            "HTS_DB_USER=hts_user",
            "HTS_DB_PASSWORD=hts_test_password")
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(DataSource.class);
              HikariDataSource dataSource = context.getBean(HikariDataSource.class);
              assertThat(dataSource.getJdbcUrl())
                  .isEqualTo("jdbc:mysql://legacy.invalid:3306/optimizer");
              assertThat(dataSource.getUsername()).isEqualTo("optimizer_user");
              assertThat(dataSource.getPassword()).isEqualTo("optimizer_test_password");
            });
  }

  @Test
  void loadsOptimizerYamlAndCertificateProperties() throws IOException {
    contextRunner()
        .withPropertyValues(
            "OPENHOUSE_CLUSTER_CONFIG_PATH="
                + new ClassPathResource("optimizer-cluster.yaml").getFile().getAbsolutePath(),
            "OPTIMIZER_DB_URL=jdbc:mysql://legacy.invalid:3306/optimizer",
            "OPTIMIZER_DB_USER=optimizer_user",
            "OPTIMIZER_DB_PASSWORD=optimizer_test_password",
            "OPTIMIZER_TEST_KEYSTORE_PASSWORD=client_test_password",
            "OPTIMIZER_TEST_TRUSTSTORE_PASSWORD=trust_test_password",
            "spring.datasource.hikari.maximum-pool-size=7",
            "spring.datasource.hikari.connection-timeout=15000",
            "spring.datasource.hikari.data-source-properties.socketTimeout=12000")
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(DataSource.class);
              HikariDataSource dataSource = context.getBean(HikariDataSource.class);
              assertThat(dataSource.getJdbcUrl())
                  .isEqualTo("jdbc:mysql://optimizer.invalid:3306/optimizer");
              assertThat(dataSource.getUsername()).isEqualTo("optimizer_user");
              assertThat(dataSource.getPassword()).isEqualTo("optimizer_test_password");
              assertThat(dataSource.getDriverClassName()).isEqualTo("com.mysql.cj.jdbc.Driver");
              assertThat(dataSource.getMaximumPoolSize()).isEqualTo(7);
              assertThat(dataSource.getConnectionTimeout()).isEqualTo(15000);
              assertThat(dataSource.getDataSourceProperties())
                  .containsEntry("sslMode", "VERIFY_IDENTITY")
                  .containsEntry("clientCertificateKeyStoreUrl", "file:/optimizer/client.jks")
                  .containsEntry("clientCertificateKeyStorePassword", "client_test_password")
                  .containsEntry("trustCertificateKeyStoreUrl", "file:/optimizer/trust.jks")
                  .containsEntry("trustCertificateKeyStorePassword", "trust_test_password")
                  .containsEntry("socketTimeout", "12000");
            });
  }

  @Test
  void supportsSslModeOverridesAndOptionalKeystorePasswords() {
    contextRunner()
        .withPropertyValues(
            "cluster.optimizer.database.cert-based-auth.enabled=true",
            "cluster.optimizer.database.cert-based-auth.ssl-mode=VERIFY_CA",
            "cluster.optimizer.database.cert-based-auth.client-cert-keystore-url=file:/client.jks",
            "cluster.optimizer.database.cert-based-auth.truststore-url=file:/trust.jks")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(HikariDataSource.class).getDataSourceProperties())
                  .hasSize(3)
                  .containsEntry("sslMode", "VERIFY_CA")
                  .containsEntry("clientCertificateKeyStoreUrl", "file:/client.jks")
                  .containsEntry("trustCertificateKeyStoreUrl", "file:/trust.jks");
            });
  }

  @Test
  void skipsBlankKeystoreUrls() {
    contextRunner()
        .withPropertyValues(
            "cluster.optimizer.database.cert-based-auth.enabled=true",
            "cluster.optimizer.database.cert-based-auth.client-cert-keystore-url= ",
            "cluster.optimizer.database.cert-based-auth.truststore-url=")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(HikariDataSource.class).getDataSourceProperties())
                  .hasSize(1)
                  .containsEntry("sslMode", "VERIFY_IDENTITY");
            });
  }

  @Test
  void leavesCertificatePropertiesUnsetWhenDisabled() {
    contextRunner()
        .withPropertyValues(
            "cluster.optimizer.database.cert-based-auth.enabled=false",
            "cluster.optimizer.database.cert-based-auth.client-cert-keystore-url=file:/client.jks",
            "cluster.optimizer.database.cert-based-auth.truststore-url=file:/trust.jks")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(HikariDataSource.class).getDataSourceProperties())
                  .isEmpty();
            });
  }

  @Test
  void infersH2DriverFromClusterUrlInsteadOfLegacyMysqlDefault() {
    contextRunner()
        .withPropertyValues(
            "cluster.optimizer.database.type=IN_MEMORY",
            "cluster.optimizer.database.url=jdbc:h2:mem:optimizer_config;DB_CLOSE_DELAY=-1",
            "OPTIMIZER_DB_USER=sa",
            "OPTIMIZER_DB_PASSWORD=")
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(DataSource.class);
              HikariDataSource dataSource = context.getBean(HikariDataSource.class);
              assertThat(dataSource.getDriverClassName()).isEqualTo("org.h2.Driver");
              assertThat(new JdbcTemplate(dataSource).queryForObject("SELECT 1", Integer.class))
                  .isEqualTo(1);
            });
  }

  @Test
  void infersMysqlDriverFromClusterUrlInsteadOfBatchH2Default() {
    contextRunner()
        .withPropertyValues(
            "spring.datasource.url=jdbc:h2:mem:analyzerdb;MODE=MySQL;DB_CLOSE_DELAY=-1",
            "cluster.optimizer.database.type=MYSQL",
            "cluster.optimizer.database.url=jdbc:mysql://optimizer.invalid:3306/optimizer",
            "cluster.optimizer.database.cert-based-auth.enabled=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(DataSource.class);
              HikariDataSource dataSource = context.getBean(HikariDataSource.class);
              assertThat(dataSource.getJdbcUrl())
                  .isEqualTo("jdbc:mysql://optimizer.invalid:3306/optimizer");
              assertThat(dataSource.getDriverClassName()).isEqualTo("com.mysql.cj.jdbc.Driver");
              assertThat(dataSource.getDataSourceProperties())
                  .containsEntry("sslMode", "VERIFY_IDENTITY");
            });
  }

  @Test
  void rejectsDatabaseTypeThatDoesNotMatchUrl() {
    contextRunner()
        .withPropertyValues(
            "cluster.optimizer.database.type=MYSQL",
            "cluster.optimizer.database.url=jdbc:h2:mem:optimizer_config")
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(context.getStartupFailure())
                  .hasRootCauseMessage("cluster.optimizer.database.type must match the JDBC URL");
            });
  }

  @Test
  void rejectsCertificateAuthenticationForNonMysqlDatabase() {
    contextRunner()
        .withPropertyValues(
            "cluster.optimizer.database.url=jdbc:h2:mem:optimizer_config",
            "cluster.optimizer.database.cert-based-auth.enabled=true")
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(context.getStartupFailure())
                  .hasRootCauseMessage(
                      "cluster.optimizer.database.cert-based-auth requires a MySQL JDBC URL");
            });
  }

  @Test
  void rejectsBlankClusterUrlInsteadOfFallingBack() {
    contextRunner()
        .withPropertyValues("cluster.optimizer.database.url=")
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(context.getStartupFailure())
                  .hasRootCauseMessage("cluster.optimizer.database.url must not be blank");
            });
  }

  @Test
  void rejectsBlankSslMode() {
    contextRunner()
        .withPropertyValues(
            "cluster.optimizer.database.cert-based-auth.enabled=true",
            "cluster.optimizer.database.cert-based-auth.ssl-mode=")
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(context.getStartupFailure())
                  .hasRootCauseMessage(
                      "cluster.optimizer.database.cert-based-auth.ssl-mode must not be blank");
            });
  }

  @Test
  void rejectsUnsupportedDatabaseType() {
    contextRunner()
        .withPropertyValues("cluster.optimizer.database.type=ICEBERG")
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void failsOnMalformedClusterYaml() throws IOException {
    Files.write(
        temporaryDirectory.resolve("cluster.yaml"),
        "cluster: [invalid".getBytes(StandardCharsets.UTF_8));
    contextRunner().run(context -> assertThat(context).hasFailed());
  }
}
