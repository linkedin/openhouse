package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/** A read-only client for interacting with file system. */
@Slf4j
@AllArgsConstructor
@Setter
public class StorageClient {
  private final FsStorageProvider fsStorageProvider;

  /**
   * For the given parent directory path, get all subdirectories metadaata
   *
   * @param dbPath subdirectories under the dbPath
   * @return a list of subdirectories metadata
   * @throws UncheckedIOException if the path cannot be listed, including when it does not exist
   */
  public List<DirectoryMetadata> getSubDirectoriesWithOwners(Path dbPath) {
    FileStatus[] fileStatuses;
    try {
      fileStatuses = fs().listStatus(dbPath);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot list " + dbPath + ": " + e.getMessage(), e);
    }
    List<DirectoryMetadata> subdirectories = new ArrayList<>();
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        // Get the owner of data subdirectory which user directly write into
        subdirectories.add(
            DirectoryMetadata.builder()
                .path(fileStatus.getPath())
                .creator(getOwner(fileStatus))
                .onHold(isOnHold(fileStatus))
                .build());
      }
    }
    return subdirectories;
  }

  /** Returns whether the path exists and is a directory. */
  public boolean isDirectory(Path path) {
    try {
      return fs().getFileStatus(path).isDirectory();
    } catch (FileNotFoundException e) {
      return false;
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read " + path + ": " + e.getMessage(), e);
    }
  }

  protected String getOwner(FileStatus fileStatus) {
    return fileStatus.getOwner();
  }

  /** Returns whether the permission grants no access to anyone, see {@link DirectoryMetadata}. */
  private static boolean isOnHold(FileStatus fileStatus) {
    FsPermission permission = fileStatus.getPermission();
    return permission.getUserAction() == FsAction.NONE
        && permission.getGroupAction() == FsAction.NONE
        && permission.getOtherAction() == FsAction.NONE;
  }

  protected FileSystem fs() {
    return fsStorageProvider.storageClient();
  }

  public String getRootPath() {
    return fsStorageProvider.rootPath();
  }
}
