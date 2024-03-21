package com.linkedin.openhouse.cluster.storage.filesystem;

import com.linkedin.openhouse.cluster.storage.exception.ConfigMismatchException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * An implementation of {@link com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider}
 * for on-prem HDFS. Apart from traits captured in {@link
 * com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider} there are on-prem HDFS
 * specific configurations.
 */
@Slf4j
public class ParameterizedHdfsStorageProvider implements FsStorageProvider {
  protected FileSystem fs;
  protected Map<String, String> storageProperties;
  private String storageType;
  private String baseUri;
  private String rootPath;
  private String clusterStorageHadoopCoreSitePath;
  private String clusterStorageHadoopHdfsSitePath;

  public static ParameterizedHdfsStorageProvider of(
      String storageType, String baseUri, String rootPath) {
    return new ParameterizedHdfsStorageProvider(storageType, baseUri, rootPath, null, null);
  }

  protected ParameterizedHdfsStorageProvider(
      String storageType,
      String baseUri,
      String rootPath,
      String clusterStorageHadoopCoreSitePath,
      String clusterStorageHadoopHdfsSitePath) {
    this.storageType = storageType;
    this.baseUri = baseUri;
    this.rootPath = rootPath;
    this.clusterStorageHadoopCoreSitePath = clusterStorageHadoopCoreSitePath;
    this.clusterStorageHadoopHdfsSitePath = clusterStorageHadoopHdfsSitePath;
  }

  @Override
  public String name() {
    return String.format("Type[%s], URI:[%s] ", storageType(), baseUri());
  }

  @Override
  public String storageType() {
    return storageType;
  }

  @Override
  public String baseUri() {
    return baseUri;
  }

  @Override
  public String rootPath() {
    return rootPath;
  }

  @Override
  public Map<String, String> storageProperties() {
    try {
      initializeClient();
      return this.storageProperties;
    } catch (IOException ioe) {
      throw new UncheckedIOException("Not able to initialize the storage client due to:", ioe);
    }
  }

  @Override
  public FileSystem storageClient() {
    try {
      initializeClient();
      return this.fs;
    } catch (IOException ioe) {
      throw new UncheckedIOException("Not able to initialize the storage client due to:", ioe);
    }
  }

  private synchronized void initializeClient() throws IOException {
    if (fs == null) {
      storageProperties = new HashMap<>();
      if ("hadoop".equals(storageType)) {
        org.apache.hadoop.conf.Configuration configuration =
            new org.apache.hadoop.conf.Configuration();
        if (baseUri != null) {
          configuration.set("fs.defaultFS", baseUri);
          storageProperties.put("fs.defaultFS", baseUri);
        }
        if (clusterStorageHadoopCoreSitePath != null) {
          configuration.addResource(new Path(clusterStorageHadoopCoreSitePath));
        }
        if (clusterStorageHadoopHdfsSitePath != null) {
          configuration.addResource(new Path(clusterStorageHadoopHdfsSitePath));
        }
        this.fs = FileSystem.get(configuration);
      } else {
        throw new ConfigMismatchException(
            "Not able to initialize the storage client with storage-type: ",
            storageType,
            "currentClass:" + this.getClass().getName());
      }
    } else {
      log.debug("Obtaining initialized file system object reference directly: {}", fs);
    }
  }
}
