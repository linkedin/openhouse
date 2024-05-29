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

@Slf4j
@Lazy
@Component
public class S3StorageClient implements StorageClient<S3Client> {
  private static final StorageType.Type S3_TYPE = StorageType.S3;

  @Autowired private StorageProperties storageProperties;

  private S3Client s3;

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
