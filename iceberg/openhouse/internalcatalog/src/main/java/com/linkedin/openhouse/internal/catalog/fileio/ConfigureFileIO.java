package com.linkedin.openhouse.internal.catalog.fileio;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the FileIO beans for storages configured in {@link StorageManager}
 *
 * <p>Each storage type should have a corresponding FileIO bean defined in this class. The return
 * value of the bean is null if the storage type is not configured. The return class of the bean is
 * the FileIO implementation for the respective storage type. If conflicting class could be returned
 * for the same storage type, the bean name should be annotated with Qualifier to distinguish
 * between them.
 */
@Slf4j
@Configuration
public class ConfigureFileIO {

  @Autowired StorageManager storageManager;

  /**
   * Provides the HadoopFileIO bean for HDFS storage type
   *
   * @return HadoopFileIO bean for HDFS storage type, or null if HDFS storage type is not configured
   */
  @Bean("HadoopFileIO")
  HadoopFileIO provideHadoopFileIO() {
    try {
      FileSystem fs =
          (FileSystem) storageManager.getStorage(StorageType.HDFS).getClient().getNativeClient();
      return new HadoopFileIO(fs.getConf());
    } catch (IllegalArgumentException e) {
      // If the HDFS storage type is not configured, return null
      // Spring doesn't define the bean if the return value is null
      log.debug("HDFS storage type is not configured");
      return null;
    }
  }

  /**
   * Provides the HadoopFileIO bean for Local storage type
   *
   * @return HadoopFileIO bean for Local storage type, or null if Local storage type is not
   *     configured
   */
  @Bean("LocalFileIO")
  FileIO provideLocalFileIO() {
    try {
      FileSystem fs =
          (FileSystem) storageManager.getStorage(StorageType.LOCAL).getClient().getNativeClient();
      return new HadoopFileIO(fs.getConf());
    } catch (IllegalArgumentException e) {
      // If the Local storage type is not configured, return null
      // Spring doesn't define the bean if the return value is null
      log.debug("Local storage type is not configured");
      return null;
    }
  }
}
