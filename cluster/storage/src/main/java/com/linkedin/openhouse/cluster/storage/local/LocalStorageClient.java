package com.linkedin.openhouse.cluster.storage.local;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.linkedin.openhouse.cluster.storage.BaseStorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * The LocalStorageClient class is an implementation of the StorageClient interface for local
 * storage. It uses an Apache Hadoop FileSystem to interact with the local file system.
 */
@Slf4j
@Lazy
@Component
public class LocalStorageClient extends BaseStorageClient<FileSystem> {

  private FileSystem fs;

  private static final StorageType.Type LOCAL_TYPE = StorageType.LOCAL;

  private static final String DEFAULT_ENDPOINT = "file:";

  private static final String DEFAULT_ROOTPATH = "/tmp";

  private String endpoint;

  private String rootPath;

  @Autowired private StorageProperties storageProperties;

  /** Initialize the LocalStorageClient when the bean is accessed for the first time. */
  @PostConstruct
  public void init() throws IOException {
    init(new org.apache.hadoop.conf.Configuration());
  }

  @VisibleForTesting
  public synchronized void init(org.apache.hadoop.conf.Configuration hadoopConfig)
      throws IOException {
    log.info("Initializing storage client for type: " + LOCAL_TYPE);

    URI uri;
    if (storageProperties.getTypes() != null && !storageProperties.getTypes().isEmpty()) {
      Preconditions.checkArgument(
          storageProperties.getTypes().containsKey(LOCAL_TYPE.getValue()),
          "Storage properties doesn't contain type: " + LOCAL_TYPE.getValue());
      Preconditions.checkArgument(
          storageProperties.getTypes().get(LOCAL_TYPE.getValue()).getEndpoint() != null,
          "Storage properties doesn't contain endpoint for: " + LOCAL_TYPE.getValue());
      Preconditions.checkArgument(
          storageProperties.getTypes().get(LOCAL_TYPE.getValue()).getRootPath() != null,
          "Storage properties doesn't contain rootpath for: " + LOCAL_TYPE.getValue());
      Preconditions.checkArgument(
          storageProperties
              .getTypes()
              .get(LOCAL_TYPE.getValue())
              .getEndpoint()
              .startsWith(DEFAULT_ENDPOINT),
          "Storage properties endpoint was misconfigured for: " + LOCAL_TYPE.getValue());
      endpoint = storageProperties.getTypes().get(LOCAL_TYPE.getValue()).getEndpoint();
      rootPath = storageProperties.getTypes().get(LOCAL_TYPE.getValue()).getRootPath();
    } else {
      endpoint = DEFAULT_ENDPOINT;
      rootPath = DEFAULT_ROOTPATH;
    }
    try {
      uri = new URI(endpoint + rootPath);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Storage properties 'endpoint', 'rootpath' was incorrectly configured for: "
              + LOCAL_TYPE.getValue(),
          e);
    }
    this.fs = FileSystem.get(uri, hadoopConfig);
    assertLocalFileSystem(fs);
  }

  /**
   * Assert that the file system is a local file system, _or_ a wrapper around a local file system.
   * This is used to prevent against misconfigurations where a remote FS may be used.
   *
   * <p>This allows for wrappers around {@link LocalFileSystem} to be used (specifically {@link
   * FilterFileSystem} subclasses) in case calling environments have configured to set up custom
   * behavior around base file system implementations. This applies to, for example, Trino, which
   * wraps all file system implementations inside a {@code FileSystemWrapper} class. See <a
   * href="https://github.com/trinodb/trino/blob/master/lib/trino-hdfs/src/main/java/io/trino/hdfs/TrinoFileSystemCache.java">TrinoFileSystemCache</a>
   * for more on this example.
   *
   * @param fs The filesystem to check.
   */
  private static void assertLocalFileSystem(FileSystem fs) {
    if (fs instanceof LocalFileSystem) {
      // do nothing, this is the happy path
    } else if (fs instanceof FilterFileSystem) {
      assertLocalFileSystem(((FilterFileSystem) fs).getRawFileSystem());
    } else {
      throw new IllegalArgumentException(
          "Instantiation failed for LocalStorageClient, fileSystem is not a LocalFileSystem");
    }
  }

  @Override
  public FileSystem getNativeClient() {
    return fs;
  }

  @Override
  public StorageType.Type getStorageType() {
    return LOCAL_TYPE;
  }

  @Override
  public String getEndpoint() {
    return endpoint;
  }

  @Override
  public String getRootPrefix() {
    return rootPath;
  }

  /**
   * Checks if the path exists on the local file system. Scheme is not specified in the path for
   * local and hdfs storage. See: https://github.com/linkedin/openhouse/issues/121 Example: For Hdfs
   * and local file system, the path would be /rootPath/db/table/file.
   *
   * @param path path to a file
   * @return true if path exists else false
   */
  @Override
  public boolean fileExists(String path) {
    try {
      return fs.exists(new Path(path));
    } catch (IOException e) {
      throw new RuntimeException("Exception checking path existence " + e.getMessage(), e);
    }
  }
}
