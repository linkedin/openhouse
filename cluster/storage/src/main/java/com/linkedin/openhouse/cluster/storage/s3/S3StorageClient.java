package com.linkedin.openhouse.cluster.storage.s3;

import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.S3FileIOAwsClientFactories;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * S3StorageClient is an implementation of the StorageClient interface for S3. It uses the {@link
 * S3Client} to interact with S3 storage. An AWS_REGION must be set either in the environment
 * variable or in the storage properties for the client initialization.
 */
@Slf4j
@Lazy
@Component
public class S3StorageClient implements StorageClient<S3Client> {
  // Storage type.
  private static final StorageType.Type S3_TYPE = StorageType.S3;

  // Storage properties.
  @Autowired private StorageProperties storageProperties;

  // S3 client.
  private S3Client s3;

  /**
   * Initialize the S3 client when the bean is accessed the first time. AWS_REGION must be set in
   * the evironment variable or in the storage properties.
   */
  @PostConstruct
  public synchronized void init() {
    // TODO: Validate properties.
    Map properties =
        new HashMap(storageProperties.getTypes().get(S3_TYPE.getValue()).getParameters());

    S3FileIOProperties s3FileIOProperties =
        new S3FileIOProperties(
            storageProperties.getTypes().get(S3_TYPE.getValue()).getParameters());
    if (s3 == null) {
      Object clientFactory = S3FileIOAwsClientFactories.initialize(properties);
      if (clientFactory instanceof S3FileIOAwsClientFactory) {
        this.s3 = ((S3FileIOAwsClientFactory) clientFactory).s3();
      }
      if (clientFactory instanceof AwsClientFactory) {
        this.s3 = ((AwsClientFactory) clientFactory).s3();
      }
    }
  }

  @Override
  public S3Client getNativeClient() {
    return s3;
  }

  @Override
  public String getEndpoint() {
    return storageProperties.getTypes().get(S3_TYPE.getValue()).getEndpoint();
  }

  @Override
  public String getRootPrefix() {
    return storageProperties.getTypes().get(S3_TYPE.getValue()).getRootPath();
  }
}
