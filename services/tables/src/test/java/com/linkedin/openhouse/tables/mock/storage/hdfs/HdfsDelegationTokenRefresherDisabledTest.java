package com.linkedin.openhouse.tables.mock.storage.hdfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsDelegationTokenRefresher;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(
    initializers =
        HdfsDelegationTokenRefresherDisabledTest.DisabledTokenRefreshPropertiesInitializer.class)
public class HdfsDelegationTokenRefresherDisabledTest {

  @MockBean StorageManager storageManager;

  @Autowired private ApplicationContext ctx;

  /**
   * cluster-test-properties.yaml contains the following properties:
   *
   * <pre>
   *    token.refresh.enabled: "false"
   * </pre>
   */
  @Test
  public void testRefreshIsNotEnabled() {
    Assertions.assertThrows(
        NoSuchBeanDefinitionException.class, () -> ctx.getBean(HdfsDelegationTokenRefresher.class));
  }

  public static class DisabledTokenRefreshPropertiesInitializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
      try {
        Path tempFile = Files.createTempFile(null, ".yaml");
        String yamlContent = getYamlContent();
        Files.write(tempFile, yamlContent.getBytes());
        System.setProperty("OPENHOUSE_CLUSTER_CONFIG_PATH", tempFile.toString());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @SneakyThrows
    private String getYamlContent() {
      YAMLMapper yamlMapper = new YAMLMapper();
      JsonNode jsonNode =
          yamlMapper.readTree(
              "cluster:\n"
                  + "  storages:\n"
                  + "    default-type: \"hdfs\"\n"
                  + "    types:\n"
                  + "      hdfs:\n"
                  + "        rootpath: \"/tmp/unittest\"\n"
                  + "        endpoint: \"hdfs://localhost:9000\"\n"
                  + "        parameters:\n"
                  + "          token.refresh.enabled: \"false\"\n"
                  + "          token.refresh.schedule.cron: \"* * * * * ?\"");
      return yamlMapper.writeValueAsString(jsonNode);
    }
  }

  @AfterAll
  static void unsetSysProp() {
    System.clearProperty("OPENHOUSE_CLUSTER_CONFIG_PATH");
  }
}
