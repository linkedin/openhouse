package com.linkedin.openhouse.cluster.storage.s3;

import com.linkedin.openhouse.cluster.storage.BaseStorage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * The S3Storage class is an implementation of the Storage interface for S3 storage. It uses a
 * S3StorageClient which in turn uses the native S3Client to interact with the S3 storage system.
 */
@Component
public class S3Storage extends BaseStorage {
  @Autowired @Lazy private S3StorageClient s3StorageClient;

  /**
   * Get the storage type for the S3 storage.
   *
   * @return the "S3" storage type
   */
  @Override
  public StorageType.Type getType() {
    return StorageType.S3;
  }

  /**
   * Get the S3 storage client.
   *
   * @return the S3 storage client
   */
  @Override
  public StorageClient<?> getClient() {
    return s3StorageClient;
  }
}
