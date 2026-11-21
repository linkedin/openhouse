package com.linkedin.openhouse.jobs.util;

import lombok.Builder;
import lombok.Value;
import org.apache.hadoop.fs.Path;

/** A hold to put on an orphan table directory, with the directory as it was before the hold. */
@Value
@Builder
public class DirectoryHold {
  Path path;
  String tableUuid;
  String owner;
  String group;
  /** Octal permission, such as {@code 770}, that releases the hold when set back. */
  String previousPermission;

  long heldAtMillis;
}
