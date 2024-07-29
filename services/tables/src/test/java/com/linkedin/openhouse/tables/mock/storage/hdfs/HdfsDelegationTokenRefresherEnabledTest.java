package com.linkedin.openhouse.tables.mock.storage.hdfs;

import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsDelegationTokenRefresher;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(
    initializers =
        HdfsDelegationTokenRefresherEnabledTest.EnabledTokenRefreshPropertiesInitializer.class)
public class HdfsDelegationTokenRefresherEnabledTest {

  @MockBean StorageManager storageManager;

  @SpyBean private HdfsDelegationTokenRefresher hdfsDelegationTokenRefresher;

  /**
   * cluster-test-properties.yaml contains the following properties:
   *
   * <pre>
   *    token.refresh.enabled: "true"
   *    token.refresh.schedule.cron: "* * * * * ?"   // every second
   * </pre>
   */
  @Test
  public void testRefresh() throws InterruptedException {
    int arbitraryCountToAssertMultipleInvocations = 2;

    CountDownLatch latch = new CountDownLatch(arbitraryCountToAssertMultipleInvocations);

    doAnswer(
            (Answer<Void>)
                invocation -> {
                  latch.countDown();
                  return null;
                })
        .when(hdfsDelegationTokenRefresher)
        .refresh();

    if (!latch.await(5, TimeUnit.SECONDS)) {
      // timer had to wait for 5 seconds and still the latch was not counted down to 0
      Assertions.fail(
          String.format(
              "The refresh method was not called at least %d times within 5 seconds",
              arbitraryCountToAssertMultipleInvocations));
    }
  }

  public static class EnabledTokenRefreshPropertiesInitializer
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
                  + "          token.refresh.enabled: \"true\"\n"
                  + "          token.refresh.schedule.cron: \"* * * * * ?\"");
      return yamlMapper.writeValueAsString(jsonNode);
    }
  }

  @AfterAll
  static void unsetSysProp() {
    System.clearProperty("OPENHOUSE_CLUSTER_CONFIG_PATH");
  }
}
