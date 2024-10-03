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

  @Override
  public String getEntityName() {
    return getPath().toString();
  }

  public String getBaseName() {
    return path.getName();
  }
}
