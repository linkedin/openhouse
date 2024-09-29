package com.linkedin.openhouse.jobs.util;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import org.apache.hadoop.fs.Path;

/** Directory metadata, including path and creator. */
@EqualsAndHashCode(callSuper = true)
public class DirectoryMetadata extends Metadata {
  Path path;

  @Builder
  public DirectoryMetadata(String creator, Path directoryPath) {
    super(creator);
    this.path = directoryPath;
  }

  public static DirectoryMetadata of(Path directoryPath, String creator) {
    return new DirectoryMetadata(creator, directoryPath);
  }

  @Override
  public String toString() {
    return String.format("%s(path: %s, creator: %s)", getClass().getSimpleName(), path, creator);
  }

  @Override
  public String getValue() {
    return path.toString();
  }

  public String getDirectoryName() {
    return path.getName();
  }
}
