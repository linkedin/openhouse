package com.linkedin.openhouse.tables.repository.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Utility for validating Iceberg schemas with respect to case-insensitive column naming.
 *
 * <p>Promoted from li-openhouse's {@code SchemaValidationUtil} to the upstream open-source repo so
 * that both sides share a single implementation. The two callers want opposite outcomes from the
 * same predicate:
 *
 * <ul>
 *   <li><b>li-openhouse write rejection</b>: if duplicates exist → reject the write.
 *   <li><b>{@link BaseIcebergSchemaValidator} normalization guard</b>: if duplicates exist → skip
 *       normalization (write may still succeed for exact-casing).
 * </ul>
 *
 * <p>Coverage: struct fields at any nesting depth, list element types, and map key/value types.
 * Fields with the same name in <em>different</em> structs (e.g. {@code user.id} and {@code
 * session.id}) are correctly treated as independent — only <em>siblings</em> within the same struct
 * are compared.
 */
public final class SchemaValidationUtil {

  private SchemaValidationUtil() {}

  /**
   * Finds all sibling-field pairs at any nesting depth whose names are equal case-insensitively but
   * not case-sensitively.
   *
   * @param schema the Iceberg schema to validate
   * @return list of conflict descriptions, e.g. {@code "[userid, userId]"} or {@code "[col1.userid,
   *     col1.userId]"}; empty if no conflicts
   */
  public static List<String> findDuplicateCaseInsensitiveColumnNames(Schema schema) {
    List<String> conflicts = new ArrayList<>();
    checkFieldsForDuplicates(schema.columns(), "", conflicts);
    return conflicts;
  }

  /**
   * Returns {@code true} if the schema has any case-insensitive duplicate field names at any
   * nesting depth.
   */
  public static boolean hasDuplicateCaseInsensitiveColumnNames(Schema schema) {
    return !findDuplicateCaseInsensitiveColumnNames(schema).isEmpty();
  }

  /**
   * Checks {@code fields} for sibling-level case-insensitive duplicates, records any conflicts, and
   * recurses into each field's type.
   */
  private static void checkFieldsForDuplicates(
      List<Types.NestedField> fields, String pathPrefix, List<String> conflicts) {
    Map<String, String> seenLowerToName = new HashMap<>();
    for (Types.NestedField field : fields) {
      String name = field.name();
      String lower = name.toLowerCase(Locale.ROOT);
      if (seenLowerToName.containsKey(lower)) {
        String first = seenLowerToName.get(lower);
        if (!first.equals(name)) {
          String qualifier = pathPrefix.isEmpty() ? "" : pathPrefix + ".";
          conflicts.add(String.format("[%s%s, %s%s]", qualifier, first, qualifier, name));
        }
      } else {
        seenLowerToName.put(lower, name);
      }
    }
    for (Types.NestedField field : fields) {
      String childPath = pathPrefix.isEmpty() ? field.name() : pathPrefix + "." + field.name();
      checkTypeForDuplicates(field.type(), childPath, conflicts);
    }
  }

  /** Recurses into compound types (struct, list element, map key/value). Primitives are a no-op. */
  private static void checkTypeForDuplicates(Type type, String path, List<String> conflicts) {
    if (type instanceof Types.StructType) {
      checkFieldsForDuplicates(((Types.StructType) type).fields(), path, conflicts);
    } else if (type instanceof Types.ListType) {
      checkTypeForDuplicates(((Types.ListType) type).elementType(), path, conflicts);
    } else if (type instanceof Types.MapType) {
      Types.MapType mapType = (Types.MapType) type;
      checkTypeForDuplicates(mapType.keyType(), path, conflicts);
      checkTypeForDuplicates(mapType.valueType(), path, conflicts);
    }
    // Primitive types have no nested fields; nothing to recurse into.
  }
}
