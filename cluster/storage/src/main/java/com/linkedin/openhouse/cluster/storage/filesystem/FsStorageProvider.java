package com.linkedin.openhouse.cluster.storage.filesystem;

import com.linkedin.openhouse.cluster.storage.StorageProvider;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;

/**
 * An extension of base interface {@link StorageProvider} with parameterized client object type
 * being {@link FileSystem}. File systems implementation like HDFS and ADLSv2 that are {@link
 * FileSystem}-compliant is able to work with this interface.
 *
 * <p>We could obviously build interface for storage client and make {@link FileSystem}
 * implementation of that. However abstracting common API across different storage client is
 * challenging, while {@link FileSystem} API is the primary use cases for now, so we decide to stay
 * with simpler option for now.
 */
public interface FsStorageProvider extends StorageProvider<FileSystem> {

  /**
   * @return The root path for OpenHouse's controlled space.
   */
  String rootPath();

  /**
   * @return The list of storage properties set in configurations for Openhouse backed Storage .
   */
  Map<String, String> storageProperties();
}
