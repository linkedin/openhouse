package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
   */
  public List<DirectoryMetadata> getSubDirectoriesWithOwners(Path dbPath) {
    List<DirectoryMetadata> subdirectories = new ArrayList<>();
    try {
      for (FileStatus fileStatus : fs().listStatus(dbPath)) {
        if (fileStatus.isDirectory()) {
          // Get the owner of data subdirectory which user directly write into
          subdirectories.add(DirectoryMetadata.of(fileStatus.getPath(), getOwner(fileStatus)));
        }
      }
    } catch (Exception e) {
      log.error("Unable to fetch filesystem given path: {}", dbPath, e);
    }
    return subdirectories;
  }

  protected String getOwner(FileStatus fileStatus) {
    return fileStatus.getOwner();
  }

  protected FileSystem fs() {
    return fsStorageProvider.storageClient();
  }

  public String getRootPath() {
    return fsStorageProvider.rootPath();
  }
}
