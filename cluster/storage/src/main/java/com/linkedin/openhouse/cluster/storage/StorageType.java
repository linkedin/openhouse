package com.linkedin.openhouse.cluster.storage;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.springframework.stereotype.Component;

/**
 * Enum for supported storage types.
 *
 * <p>New types should be added here as public static final fields, and their corresponding
 * implementations should be added to the fromString method.
 */
@Component
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
    if (HDFS.getValue().equals(type)) {
      return HDFS;
    } else if (LOCAL.getValue().equals(type)) {
      return LOCAL;
    } else {
      throw new IllegalArgumentException("Unknown storage type: " + type);
    }
  }
}
