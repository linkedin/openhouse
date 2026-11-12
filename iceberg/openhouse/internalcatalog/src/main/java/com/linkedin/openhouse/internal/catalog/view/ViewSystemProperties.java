package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils;
import java.util.Locale;
import org.apache.iceberg.view.ViewProperties;

/** Shared system-property checks used by views API validation and the view commit engine. */
public final class ViewSystemProperties {
  public static final String SOURCE_DIALECT = "openhouse.view.sourceDialect";

  private ViewSystemProperties() {}

  public static boolean isSystemProperty(String key) {
    String normalized = key.toLowerCase(Locale.ROOT);
    return HouseTableSerdeUtils.IS_OH_PREFIXED.test(normalized)
        || ViewProperties.REPLACE_DROP_DIALECT_ALLOWED.equals(normalized)
        || "policies".equals(normalized);
  }
}
