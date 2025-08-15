package com.linkedin.openhouse.cluster.storage.hdfs;

import com.linkedin.openhouse.cluster.storage.BaseStorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.io.IOException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * The HdfsStorageClient class is an implementation of the StorageClient interface for HDFS Storage.
 * It uses the {@link FileSystem} class to interact with the HDFS file system.
 */
@Slf4j
@Lazy
@Component
public class HdfsStorageClient extends BaseStorageClient<FileSystem> {

  private FileSystem fs;

  // Holds storage properties for all configured storage types.
  @Autowired private StorageProperties storageProperties;

  private static final StorageType.Type HDFS_TYPE = StorageType.HDFS;

  /** Initialize the HdfsStorageClient when the bean is accessed for the first time. */
  @PostConstruct
  public synchronized void init() throws IOException {
    log.info("Initializing storage client for type: " + HDFS_TYPE);
    validateProperties();
    StorageProperties.StorageTypeProperties hdfsStorageProperties =
        storageProperties.getTypes().get(HDFS_TYPE.getValue());
    org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
    configuration.set("fs.defaultFS", hdfsStorageProperties.getEndpoint());
    fs = FileSystem.get(configuration);
  }

  @Override
  public FileSystem getNativeClient() {
    return fs;
  }

  @Override
  public StorageType.Type getStorageType() {
    return HDFS_TYPE;
  }

  /**
   * Checks if the path exists on the hdfs. Scheme is not specified in the path for local and hdfs
   * storage. See: https://github.com/linkedin/openhouse/issues/121 Example: For Hdfs and local file
   * system, the path would be /rootPath/db/table/file.
   *
   * @param path path to a file
   * @return true if path exists else false
   */
  @Override
  public boolean exists(String path) {
    try {
      return fs.exists(new Path(path));
    } catch (IOException e) {
      throw new ServiceUnavailableException(
          "Exception checking path existence " + e.getMessage(), e);
    }
  }

  /** Clean up resources when the bean is destroyed. */
  @PreDestroy
  public void cleanup() {
    if (fs != null) {
      try {
        log.info("Closing FileSystem for HdfsStorageClient");
        fs.close();
      } catch (IOException e) {
        log.error("Error closing FileSystem during HdfsStorageClient cleanup", e);
      }
    }
  }
}
