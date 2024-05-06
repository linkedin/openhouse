package com.linkedin.openhouse.internal.catalog.fileio;

import com.linkedin.openhouse.cluster.storage.StorageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@SpringBootTest(classes = FileIOManagerTest.FileIOManagerTestConfig.class)
public class FileIOManagerTest {

  @Autowired FileIOManager fileIOManager;

  @Test
  public void testGetLocalFileIO() {
    // local storage is configured
    Assertions.assertNotNull(fileIOManager.getFileIO(StorageType.LOCAL));
  }

  @Test
  public void testGetUndefinedFileIOThrowsException() {
    // hdfs storage is not configured
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> fileIOManager.getFileIO(StorageType.HDFS));
  }

  @Configuration
  @ComponentScan(
      basePackages = {
        "com.linkedin.openhouse.internal.catalog.fileio",
        "com.linkedin.openhouse.cluster.storage",
        "com.linkedin.openhouse.cluster.configs"
      })
  public static class FileIOManagerTestConfig {}
}
