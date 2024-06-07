package com.linkedin.openhouse.cluster.storage.s3;

import com.linkedin.openhouse.cluster.storage.BaseStorage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Component
public class S3Storage extends BaseStorage {

  @Autowired @Lazy private S3StorageClient s3StorageClient;

  @Override
  public StorageType.Type getType() {
    return StorageType.S3;
  }

  @Override
  public StorageClient<?> getClient() {
    return s3StorageClient;
  }
}
