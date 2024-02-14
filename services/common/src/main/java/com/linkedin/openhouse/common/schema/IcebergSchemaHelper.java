package com.linkedin.openhouse.common.schema;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

public final class IcebergSchemaHelper {
  private IcebergSchemaHelper() {
    // utility class no-op constructor
  }

  public static Schema getSchemaFromSchemaJson(String schemaJson) {
    return SchemaParser.fromJson(schemaJson);
  }

  public static String getSchemaJsonFromSchema(Schema schema) {
    return SchemaParser.toJson(schema);
  }

  /**
   * @param schema Schema object
   * @param columnName Name of the column to be inspected
   * @return true if the columnName exists in the provided schema string
   */
  public static boolean columnExists(Schema schema, String columnName) {
    return schema.findField(columnName) != null;
  }
}
