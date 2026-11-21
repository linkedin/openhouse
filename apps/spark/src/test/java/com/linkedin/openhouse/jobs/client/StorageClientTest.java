package com.linkedin.openhouse.jobs.client;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
  void testGetSubDirectoriesWithOwnersMarksDirectoriesOnHold() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path open = new Path(tempDir.resolve("open").toString());
    Path onHold = new Path(tempDir.resolve("held").toString());
    Path noOwnerAccess = new Path(tempDir.resolve("no_owner_access").toString());
    fs.mkdirs(open);
    fs.mkdirs(onHold);
    fs.mkdirs(noOwnerAccess);
    fs.setPermission(onHold, new FsPermission((short) 0));
    fs.setPermission(noOwnerAccess, new FsPermission((short) 0070));
    try {
      Map<String, Boolean> onHoldByName =
          storageClient.getSubDirectoriesWithOwners(new Path(tempDir.toString())).stream()
              .collect(
                  Collectors.toMap(DirectoryMetadata::getBaseName, DirectoryMetadata::isOnHold));

      Assertions.assertEquals(
          ImmutableMap.of("open", false, "held", true, "no_owner_access", false), onHoldByName);
    } finally {
      fs.setPermission(onHold, new FsPermission((short) 0700));
      fs.setPermission(noOwnerAccess, new FsPermission((short) 0700));
    }
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
