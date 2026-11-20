package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.HouseTablesSpringApplication;
import com.linkedin.openhouse.housetables.e2e.HtsMysqlDatabase;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.support.TestPropertySourceUtils;

/** Runs HTS over real HTTP using the same MySQL fixture as the HTS integration suite. */
public class ViewsHttpContextInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext context) {
    // Keep real metadata paths within the canonical MySQL VARCHAR(255), even at the ID limit.
    Path root;
    try {
      Path temp =
          Paths.get(
              System.getProperty("os.name").startsWith("Windows")
                  ? System.getProperty("java.io.tmpdir")
                  : "/tmp");
      root = Files.createTempDirectory(temp, "v");
    } catch (IOException e) {
      throw new UncheckedIOException("Could not create the views metadata directory", e);
    }
    DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) context.getBeanFactory();
    beanFactory.registerDisposableBean(
        "viewsMetadataDirectory", () -> FileUtils.deleteDirectory(root.toFile()));
    Map<String, Object> properties = new LinkedHashMap<>();
    HtsMysqlDatabase.configure((name, value) -> properties.put(name, value.get()));
    ServletWebServerApplicationContext hts =
        (ServletWebServerApplicationContext)
            new SpringApplicationBuilder(HouseTablesSpringApplication.class)
                .initializers(
                    application ->
                        application
                            .getEnvironment()
                            .getPropertySources()
                            .addFirst(new MapPropertySource("hts-mysql", properties)))
                .run(
                    "--server.port=0",
                    "--spring.autoconfigure.exclude="
                        + "org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration,"
                        + "org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration");
    beanFactory.registerDisposableBean("viewsHtsServer", hts::close);
    TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
        context,
        "cluster.storage.root-path=" + root,
        "cluster.tables.allowed-client-name-values=trino,spark",
        "cluster.housetables.base-uri=http://localhost:" + hts.getWebServer().getPort(),
        "cluster.storages.default-type=local",
        "cluster.storages.types.local.rootpath=" + root,
        "cluster.storages.types.local.endpoint=file:///",
        "cluster.storages.types.hdfs.rootpath=" + root,
        "cluster.storages.types.hdfs.endpoint=file:///",
        "spring.sql.init.mode=never",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration");
  }
}
