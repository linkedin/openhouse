package com.linkedin.openhouse.cluster.storage.hdfs;

import com.linkedin.openhouse.cluster.storage.BaseStorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.io.IOException;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

  @Override
  public boolean pathExists(String path) {
    try {
      return fs.exists(new Path(path));
    } catch (IOException e) {
      log.error("Table location {} does not exist: ", path);
      return false;
    }
  }
}
