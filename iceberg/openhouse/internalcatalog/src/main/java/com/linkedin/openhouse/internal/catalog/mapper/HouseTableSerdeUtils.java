package com.linkedin.openhouse.internal.catalog.mapper;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

/**
 * Utility class used for mapping and persisting {@link
 * com.linkedin.openhouse.internal.catalog.model.HouseTable} as a map internally at OpenHouse.
 */
@Slf4j
public final class HouseTableSerdeUtils {

  @VisibleForTesting public static final String OPENHOUSE_NAMESPACE = "openhouse.";

  @VisibleForTesting
  public static final Predicate<String> IS_OH_PREFIXED = s -> s.startsWith(OPENHOUSE_NAMESPACE);

  public static final Set<String> HTS_FIELD_NAMES =
      Arrays.stream(HouseTable.class.getDeclaredFields())
          .filter(
              field ->
                  Modifier.isPrivate(field.getModifiers())
                      && !Modifier.isStatic(field.getModifiers()))
          .map(Field::getName)
          .collect(Collectors.toSet());

  private HouseTableSerdeUtils() {
    // no-op for util class constructor
  }

  @VisibleForTesting public static final String ENTITY_TYPE_FIELD_NAME = "entityType";

  public static final String TABLE_ENTITY_TYPE = "TABLE";

  public static final String VIEW_ENTITY_TYPE = "VIEW";

  /**
   * {@code null} means table: the column is nullable and not backfilled. Any other unrecognized
   * value is neither a table nor a view, so table operations fail closed on it.
   */
  public static boolean isTableEntityType(String entityType) {
    return entityType == null || TABLE_ENTITY_TYPE.equalsIgnoreCase(entityType);
  }

  public static boolean isViewEntityType(String entityType) {
    return VIEW_ENTITY_TYPE.equalsIgnoreCase(entityType);
  }

  @VisibleForTesting
  public static String getCanonicalFieldName(String htsField) {
    return OPENHOUSE_NAMESPACE + htsField;
  }
}
