package com.linkedin.openhouse.tables.repository.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.TableProperties;

/**
 * Preserves Iceberg's {@link TableProperties} on top of the keys {@link BasePreservedKeyChecker}
 * preserves: clients cannot add, alter or drop them, and table creation drops them. Clients can
 * still set the properties in {@code CLIENT_WRITABLE}.
 *
 * <p>Tables-service uses it when {@code cluster.tables.preserved-iceberg-properties.enabled} is
 * {@code true}. A deployment that registers its own primary {@code PreservedKeyChecker} can extend
 * it instead.
 */
public class IcebergPropertiesPreservedKeyChecker extends BasePreservedKeyChecker {

  private static final Set<String> CLIENT_WRITABLE =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  TableProperties.SPLIT_SIZE,
                  TableProperties.WRITE_AUDIT_PUBLISH_ENABLED,
                  TableProperties.WRITE_DISTRIBUTION_MODE,
                  TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                  TableProperties.DELETE_FILE_REPLICATION,
                  TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
                  TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA)));

  private static final Set<String> RESERVED_ICEBERG_PROPERTIES =
      Arrays.stream(TableProperties.class.getDeclaredFields())
          .filter(
              field ->
                  Modifier.isPublic(field.getModifiers())
                      && Modifier.isStatic(field.getModifiers())
                      && field.getType().equals(String.class)
                      && !field.getName().contains("_DEFAULT"))
          .map(IcebergPropertiesPreservedKeyChecker::value)
          .filter(key -> !CLIENT_WRITABLE.contains(key))
          .collect(Collectors.toSet());

  @Override
  public boolean isKeyPreserved(String key) {
    return super.isKeyPreserved(key) || RESERVED_ICEBERG_PROPERTIES.contains(key);
  }

  @Override
  public String describePreservedSpace() {
    return super.describePreservedSpace() + ", and Iceberg's TableProperties cannot be modified";
  }

  private static String value(Field field) {
    try {
      return (String) field.get(null);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Cannot read TableProperties." + field.getName(), e);
    }
  }
}
