package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

public class StorageClientTest {
  @TempDir java.nio.file.Path tempDir;

  private StorageClient storageClient;

  @BeforeEach
  void setup() throws IOException {
    FsStorageProvider fsStorageProvider = Mockito.mock(FsStorageProvider.class);
    Mockito.when(fsStorageProvider.storageClient())
        .thenReturn(FileSystem.getLocal(new Configuration()));
    storageClient = new StorageClient(fsStorageProvider);
  }

  @Test
  void testGetSubDirectoriesWithOwnersFailsWhenPathCannotBeListed() {
    Path missing = new Path(tempDir.resolve("missing").toString());

    UncheckedIOException e =
        Assertions.assertThrows(
            UncheckedIOException.class, () -> storageClient.getSubDirectoriesWithOwners(missing));
    Assertions.assertTrue(e.getMessage().startsWith("Cannot list " + missing), e.getMessage());
  }

  @Test
  void testIsDirectory() throws IOException {
    java.nio.file.Path file = Files.createFile(tempDir.resolve("file"));

    Assertions.assertTrue(storageClient.isDirectory(new Path(tempDir.toString())));
    Assertions.assertFalse(storageClient.isDirectory(new Path(file.toString())));
    Assertions.assertFalse(
        storageClient.isDirectory(new Path(tempDir.resolve("missing").toString())));
  }
}
