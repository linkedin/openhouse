package com.linkedin.openhouse.jobs.util;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hadoop.fs.Path;

/** Directory metadata, including path and creator. */
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString
public class DirectoryMetadata extends Metadata {
  private Path path;

  /**
   * Whether the directory grants no access to anyone, as after {@code chmod 000}. Operators put an
   * orphan directory on hold this way to make sure nothing uses it before it is deleted. Jobs
   * cannot read such a directory, so a superuser has to delete it.
   */
  private boolean onHold;

  @Override
  public String getEntityName() {
    return getPath().toString();
  }

  public String getBaseName() {
    return path.getName();
  }
}
