package com.linkedin.openhouse.cluster.storage.hdfs;

import com.google.common.base.Preconditions;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.io.IOException;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * The HdfsStorageClient class is an implementation of the StorageClient interface for HDFS Storage.
 * It uses the {@link FileSystem} class to interact with the HDFS file system.
 */
@Slf4j
@Lazy
@Component
public class HdfsStorageClient implements StorageClient<FileSystem> {

  private FileSystem fs;

  @Autowired private StorageProperties storageProperties;

  private static final StorageType.Type HDFS_TYPE = StorageType.HDFS;

  /** Initialize the HdfsStorageClient when the bean is accessed for the first time. */
  @PostConstruct
  public synchronized void init() throws IOException {
    validateProperties();
    StorageProperties.StorageTypeProperties hdfsStorageProperties =
        storageProperties.getTypes().get(HDFS_TYPE.getValue());
    org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
    configuration.set("fs.defaultFS", hdfsStorageProperties.getEndpoint());
    fs = FileSystem.get(configuration);
  }

  /** Validate the storage properties. */
  private void validateProperties() {
    log.info("Initializing storage client for type: " + HDFS_TYPE);
    Preconditions.checkArgument(
        !CollectionUtils.isEmpty(storageProperties.getTypes())
            && storageProperties.getTypes().containsKey(HDFS_TYPE.getValue()),
        "Storage properties doesn't contain type: " + HDFS_TYPE.getValue());
    StorageProperties.StorageTypeProperties hdfsStorageProperties =
        storageProperties.getTypes().get(HDFS_TYPE.getValue());
    Preconditions.checkArgument(
        hdfsStorageProperties != null,
        "Storage properties doesn't contain type: " + HDFS_TYPE.getValue());
    Preconditions.checkArgument(
        hdfsStorageProperties.getEndpoint() != null,
        "Storage properties doesn't contain endpoint for: " + HDFS_TYPE.getValue());
    Preconditions.checkArgument(
        hdfsStorageProperties.getRootPath() != null,
        "Storage properties doesn't contain rootpath for: " + HDFS_TYPE.getValue());
  }

  @Override
  public FileSystem getNativeClient() {
    return fs;
  }

  @Override
  public String getEndpoint() {
    return storageProperties.getTypes().get(HDFS_TYPE.getValue()).getEndpoint();
  }

  @Override
  public String getRootPath() {
    return storageProperties.getTypes().get(HDFS_TYPE.getValue()).getRootPath();
  }
}
