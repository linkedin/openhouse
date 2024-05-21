package com.linkedin.openhouse.internal.catalog.fileio;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Configures the FileIO beans for storages configured in {@link StorageManager}
 *
 * <p>Each storage type should have a corresponding FileIO bean defined in this class. The return
 * value of the bean is null if the storage type is not configured. The return class of the bean is
 * the FileIO implementation for the respective storage type. If conflicting class could be returned
 * for the same storage type, the bean name should be annotated with Qualifier to distinguish
 * between them.
 */
@Slf4j
@Configuration
public class FileIOConfig {

  @Autowired StorageManager storageManager;

  public static final String PREFIX = "s3:/";

  /**
   * Provides the HdfsFileIO bean for HDFS storage type
   *
   * @return HdfsFileIO bean for HDFS storage type, or null if HDFS storage type is not configured
   */
  @Bean("HdfsFileIO")
  HadoopFileIO provideHdfsFileIO() {
    try {
      FileSystem fs =
          (FileSystem) storageManager.getStorage(StorageType.HDFS).getClient().getNativeClient();
      return new HadoopFileIO(fs.getConf());
    } catch (IllegalArgumentException e) {
      // If the HDFS storage type is not configured, return null
      // Spring doesn't define the bean if the return value is null
      log.debug("HDFS storage type is not configured", e);
      return null;
    }
  }

  /**
   * Provides the HdfsFileIO bean for Local storage type
   *
   * @return HdfsFileIO bean for Local storage type, or null if Local storage type is not configured
   */
  @Bean("LocalFileIO")
  FileIO provideLocalFileIO() {
    try {
      FileSystem fs =
          (FileSystem) storageManager.getStorage(StorageType.LOCAL).getClient().getNativeClient();
      return new HadoopFileIO(fs.getConf());
    } catch (IllegalArgumentException e) {
      // If the Local storage type is not configured, return null
      // Spring doesn't define the bean if the return value is null
      log.debug("Local storage type is not configured", e);
      return null;
    }
  }

  @Bean("S3FileIO")
  S3FileIO provideS3FileIO() {
    try {
      S3Client s3 =
          (S3Client) storageManager.getStorage(StorageType.S3).getClient().getNativeClient();
      return new S3FileIO(() -> s3) {

        @Override
        public OutputFile newOutputFile(String path) {
          return new DelegatingOutputFile(super.newOutputFile(PREFIX + path));
        }

        @Override
        public InputFile newInputFile(String path, long length) {
          return new DelegatingInputFile(super.newInputFile(PREFIX + path, length));
        }

        @Override
        public InputFile newInputFile(String path) {
          return new DelegatingInputFile(super.newInputFile(PREFIX + path));
        }
      };
    } catch (IllegalArgumentException e) {
      // If the S3 storage type is not configured, return null
      // Spring doesn't define the bean if the return value is null
      log.debug("S3 storage type is not configured", e);
      return null;
    }
  }

  @AllArgsConstructor
  public static class DelegatingInputFile implements InputFile {
    @Delegate InputFile delegate;

    @Override
    public String location() {
      if (delegate.location().startsWith(PREFIX)) {
        return delegate.location().substring(4);
      } else {
        return delegate.location();
      }
    }
  }

  @AllArgsConstructor
  public static class DelegatingOutputFile implements OutputFile {
    @Delegate OutputFile delegate;

    @Override
    public String location() {
      if (delegate.location().startsWith(PREFIX)) {
        return delegate.location().substring(4);
      } else {
        return delegate.location();
      }
    }

    @Override
    public InputFile toInputFile() {
      return new DelegatingInputFile(delegate.toInputFile());
    }
  }
}
