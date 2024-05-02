package com.linkedin.openhouse.internal.catalog.fileio;

import static com.linkedin.openhouse.cluster.storage.StorageType.HDFS;
import static com.linkedin.openhouse.cluster.storage.StorageType.LOCAL;

import com.linkedin.openhouse.cluster.storage.StorageType;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * This is the main class that provides the FileIO implementation based on the storage type. Each
 * storage type should have a corresponding FileIO bean field defined in this class and the
 * corresponding FileIO bean should be returned for appropriate storage type in the method {@link
 * #getFileIO(StorageType.Type)}. If the storage type is not configured, the method should throw an
 * IllegalArgumentException.
 */
@Component
public class FileIOManager {

  @Autowired(required = false)
  @Qualifier("HadoopFileIO")
  HadoopFileIO hadoopFileIO;

  @Autowired(required = false)
  @Qualifier("LocalFileIO")
  FileIO localFileIO;

  /**
   * Returns the FileIO implementation for the given storage type.
   *
   * @param storageType, the storage type for which the FileIO implementation is required
   * @return FileIO implementation for the given storage type
   * @throws IllegalArgumentException if the storage type is not configured
   */
  public FileIO getFileIO(StorageType.Type storageType) throws IllegalArgumentException {
    Supplier<? extends RuntimeException> exceptionSupplier =
        () -> new IllegalArgumentException(storageType.getValue() + " is not configured");
    if (HDFS.equals(storageType)) {
      return Optional.ofNullable(hadoopFileIO).orElseThrow(exceptionSupplier);
    } else if (LOCAL.equals(storageType)) {
      return Optional.ofNullable(localFileIO).orElseThrow(exceptionSupplier);
    } else {
      throw new IllegalArgumentException("FileIO not supported for storage type: " + storageType);
    }
  }
}
