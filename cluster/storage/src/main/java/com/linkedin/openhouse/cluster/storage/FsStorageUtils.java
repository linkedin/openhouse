package com.linkedin.openhouse.cluster.storage;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public final class FsStorageUtils {
  private FsStorageUtils() {
    // Utility class ctor no-op
  }

  static final String OPENHOUSE_GROUP_NAME = "openhouse";

  /** A file permission that grants all to OpenHouse but nothing to other entities. */
  static final FsPermission PERM = new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.NONE);

  public static void securePath(FileSystem fs, Path path) throws IOException {
    /* set null means user won't be changed */
    fs.setOwner(path, null, OPENHOUSE_GROUP_NAME);
    fs.setPermission(path, PERM);
  }
}
