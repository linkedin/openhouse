package com.linkedin.openhouse.cluster.storage;

import lombok.*;

/**
 * Enum for supported storage types.
 *
 * <p>New types should be added here as public static final fields, and their corresponding
 * implementations should be added to the fromString method.
 */
public class StorageType {
  public static final Type HDFS = new Type("hdfs");
  public static final Type LOCAL = new Type("local");

  @AllArgsConstructor
  @EqualsAndHashCode
  @Getter
  public static class Type {
    private String value;
  }

  public Type fromString(String type) {
    if (type == null) {
      return null;
    }
    if (type.equals(HDFS.getValue())) {
      return HDFS;
    } else if (type.equals(LOCAL.getValue())) {
      return LOCAL;
    } else {
      return null;
    }
  }
}
