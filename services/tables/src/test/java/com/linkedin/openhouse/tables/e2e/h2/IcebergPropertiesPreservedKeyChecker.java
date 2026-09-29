package com.linkedin.openhouse.tables.e2e.h2;

import com.google.common.collect.ImmutableSet;
import com.linkedin.openhouse.tables.repository.impl.BasePreservedKeyChecker;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.TableProperties;

/**
 * Mirrors li-openhouse's {@code LiPreservedKeyChecker}, which reserves Iceberg's {@link
 * TableProperties} on top of {@code openhouse.*} and {@code policies}: clients cannot add, alter or
 * drop an Iceberg table property outside {@link #CLIENT_WRITABLE}, and table creation drops them.
 * OSS otherwise runs {@link BasePreservedKeyChecker}, where a client-side write of an Iceberg
 * property, such as the snapshot-expiration backfill of {@code history.expire.max-ref-age-ms} in
 * #708, passes tests and then fails in that deployment.
 *
 * <p>Not a component: register it as the {@code @Primary} {@link
 * com.linkedin.openhouse.tables.repository.PreservedKeyChecker} in the tests that need it.
 */
public class IcebergPropertiesPreservedKeyChecker extends BasePreservedKeyChecker {

  /**
   * {@code LiPreservedKeyChecker}'s allowlist. It also allows {@code
   * write.delete-file-replication}, which this Iceberg version does not define.
   */
  private static final Set<String> CLIENT_WRITABLE =
      ImmutableSet.of(
          TableProperties.SPLIT_SIZE,
          TableProperties.WRITE_AUDIT_PUBLISH_ENABLED,
          TableProperties.WRITE_DISTRIBUTION_MODE,
          TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
          TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
          TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA);

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
