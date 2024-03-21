package com.linkedin.openhouse.cluster.storage.filesystem;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.exception.ConfigMismatchException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * An implementation of {@link com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider}
 * for Hadoop compatible filesystem API. Apart from traits captured in {@link
 * com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider} there are on-prem HDFS
 * specific configurations.
 */
@Slf4j
@Component
public class HdfsStorageProvider implements FsStorageProvider {
  @Autowired protected ClusterProperties clusterProperties;
  private FileSystem fs;
  private Map<String, String> storageProperties;

  @Override
  public String name() {
    return String.format("Type[%s], URI:[%s] ", storageType(), baseUri());
  }

  @Override
  public String baseUri() {
    return clusterProperties.getClusterStorageURI();
  }

  @Override
  public String rootPath() {
    return clusterProperties.getClusterStorageRootPath();
  }

  @Override
  public String storageType() {
    return clusterProperties.getClusterStorageType();
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
      if ("hadoop".equals(clusterProperties.getClusterStorageType())) {
        org.apache.hadoop.conf.Configuration configuration =
            new org.apache.hadoop.conf.Configuration();
        if (clusterProperties.getClusterStorageURI() != null) {
          String storageUri = clusterProperties.getClusterStorageURI();
          configuration.set("fs.defaultFS", storageUri);
          storageProperties.put("fs.defaultFS", storageUri);
        }
        if (clusterProperties.getClusterStorageHadoopCoreSitePath() != null) {
          configuration.addResource(
              new Path(clusterProperties.getClusterStorageHadoopCoreSitePath()));
        }
        if (clusterProperties.getClusterStorageHadoopHdfsSitePath() != null) {
          configuration.addResource(
              new Path(clusterProperties.getClusterStorageHadoopHdfsSitePath()));
        }
        this.fs = FileSystem.get(configuration);
      } else {
        throw new ConfigMismatchException(
            "Not able to initialize the storage client with storage-type: ",
            "config-cluster-type:" + clusterProperties.getClusterStorageType(),
            "currentClass:" + this.getClass().getName());
      }
    } else {
      log.debug("Obtaining initialized file system object reference directly: {}", fs);
    }
  }
}
