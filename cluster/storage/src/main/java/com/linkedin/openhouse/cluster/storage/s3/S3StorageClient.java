package com.linkedin.openhouse.cluster.storage.s3;

import com.linkedin.openhouse.cluster.storage.BaseStorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * S3StorageClient is an implementation of the StorageClient interface for S3. It uses the {@link
 * S3Client} to interact with S3 storage. An AWS_REGION must be set either in the environment
 * variable or in the storage properties for the client initialization.
 */
@Slf4j
@Lazy
@Component
public class S3StorageClient extends BaseStorageClient<S3Client> {
  // Storage properties.
  @Autowired private StorageProperties storageProperties;

  // Storage type.
  private static final StorageType.Type S3_TYPE = StorageType.S3;

  // S3 client.
  private S3Client s3;

  /**
   * Initialize the S3 client when the bean is accessed the first time. AWS_REGION must be set in
   * the environment variable or in the storage properties.
   */
  @PostConstruct
  public synchronized void init() {
    log.info("Initializing storage client for type: " + S3_TYPE);
    validateProperties();
    Map properties =
        new HashMap(storageProperties.getTypes().get(S3_TYPE.getValue()).getParameters());
    AwsClientFactory clientFactory = AwsClientFactories.from(properties);
    this.s3 = clientFactory.s3();
  }

  @Override
  public S3Client getNativeClient() {
    return s3;
  }

  @Override
  public StorageType.Type getStorageType() {
    return S3_TYPE;
  }

  /**
   * Checks if the blob/object exists on the s3 backend storage. The path is the absolute path to
   * the object including scheme (s3://)
   *
   * @param path absolute path to a file including scheme
   * @return true if path exists else false
   */
  @Override
  public boolean fileExists(String path) {
    try {
      HeadObjectRequest headObjectRequest =
          HeadObjectRequest.builder().bucket(getRootPrefix()).key(path).build();
      s3.headObject(headObjectRequest);
      return true;
    } catch (NoSuchKeyException e) {
      // Object does not exist
      return false;
    } catch (S3Exception e) {
      throw new RuntimeException("Error checking S3 object existence: " + e.getMessage(), e);
    }
  }
}
