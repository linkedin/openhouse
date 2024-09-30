package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOConfig;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.iceberg.catalog.Catalog;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class MockApplication {
  public static void main(String[] args) {
    SpringApplication.run(MockApplication.class, args);
  }

  @MockBean Catalog openHouseInternalCatalog;

  @MockBean StorageManager storageManager;

  @MockBean FileIOManager fileIOManager;

  @MockBean FileIOConfig fileIOConfig;

  static final FsPermission FS_PERMISSION =
      new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  /**
   * Provide a mock FileSecurer
   *
   * @return
   */
  @Bean
  Consumer<Supplier<Path>> provideMockFileSecurer() {
    return pathSupplier -> {
      try {
        FileSystem fs = FileSystem.get(new Configuration());
        fs.setPermission(pathSupplier.get(), FS_PERMISSION);
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    };
  }
}
