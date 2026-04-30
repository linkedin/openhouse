package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.tables.repository.SchemaValidator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.MapDifference;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.springframework.stereotype.Component;

/**
 * A base implementation which delegates the validation to iceberg library that is configured to: -
 * not allow to write optional values to a required field. - allow input schema to have different
 * ordering than table.
 *
 * <p>Also provides static helpers used by {@link OpenHouseInternalRepositoryImpl} to normalize a
 * write schema's column names — at top level and at any nesting depth — to the casing already
 * present in the table schema, enabling case-insensitive writes without mutating the table's
 * existing column casing.
 */
@Component
public class BaseIcebergSchemaValidator implements SchemaValidator {

  /**
   * Rewrites every field name in {@code writeSchema} — at top level <em>and</em> inside nested
   * structs, list elements, and map keys/values — to use the casing from {@code tableSchema},
   * matched by Iceberg field ID. Fields whose ID is absent from {@code tableSchema} (genuinely new
   * columns) are left unchanged. All other field attributes (type, doc, optional/required,
   * identifier-field-ids) are preserved.
   *
   * <p>Uses {@code TypeUtil.indexById} to build a single flat {@code Map<Integer, NestedField>} for
   * the entire table schema in one O(n) walk, giving O(1) name lookup at any depth. Uses {@code
   * TypeUtil.visit} to recurse through the write schema — covering struct, list, and map container
   * types exhaustively via Iceberg's visitor contract.
   */
  static Schema normalizeSchemaCasingToTable(Schema writeSchema, Schema tableSchema) {
    // One O(n) walk over the full table schema tree; lookup at any depth is then O(1).
    final Map<Integer, Types.NestedField> tableById = TypeUtil.indexById(tableSchema.asStruct());

    Type rewritten =
        TypeUtil.visit(
            writeSchema,
            new TypeUtil.SchemaVisitor<Type>() {
              @Override
              public Type schema(Schema s, Type structResult) {
                return structResult;
              }

              @Override
              public Type struct(Types.StructType struct, List<Type> fieldTypes) {
                List<Types.NestedField> originals = struct.fields();
                List<Types.NestedField> rebuilt = new ArrayList<>(originals.size());
                for (int i = 0; i < originals.size(); i++) {
                  Types.NestedField original = originals.get(i);
                  Types.NestedField tableField = tableById.get(original.fieldId());
                  String name = (tableField != null) ? tableField.name() : original.name();
                  Type type = fieldTypes.get(i);
                  // Reuse the original if nothing changed (cheap reference-equality short-circuit).
                  if (name.equals(original.name()) && type == original.type()) {
                    rebuilt.add(original);
                  } else {
                    rebuilt.add(
                        original.isOptional()
                            ? Types.NestedField.optional(
                                original.fieldId(), name, type, original.doc())
                            : Types.NestedField.required(
                                original.fieldId(), name, type, original.doc()));
                  }
                }
                return Types.StructType.of(rebuilt);
              }

              @Override
              public Type field(Types.NestedField f, Type fieldResult) {
                return fieldResult;
              }

              @Override
              public Type list(Types.ListType list, Type elementResult) {
                Types.NestedField elem = list.fields().get(0);
                return elem.isOptional()
                    ? Types.ListType.ofOptional(elem.fieldId(), elementResult)
                    : Types.ListType.ofRequired(elem.fieldId(), elementResult);
              }

              @Override
              public Type map(Types.MapType map, Type keyResult, Type valueResult) {
                Types.NestedField k = map.fields().get(0);
                Types.NestedField v = map.fields().get(1);
                return v.isOptional()
                    ? Types.MapType.ofOptional(k.fieldId(), v.fieldId(), keyResult, valueResult)
                    : Types.MapType.ofRequired(k.fieldId(), v.fieldId(), keyResult, valueResult);
              }

              @Override
              public Type primitive(Type.PrimitiveType p) {
                return p;
              }
            });

    Types.StructType normalizedStruct = (Types.StructType) rewritten;
    return new Schema(
        writeSchema.schemaId(), normalizedStruct.fields(), writeSchema.identifierFieldIds());
  }

  @Override
  public void validateWriteSchema(Schema oldSchema, Schema newSchema, String tableUri)
      throws InvalidSchemaEvolutionException, IllegalArgumentException {
    validateFields(oldSchema, newSchema, tableUri);
    enforceDeleteColumnFailure(oldSchema, newSchema, tableUri);
    TypeUtil.validateSchema(
        "OpenHouse Server Schema validation Validation", newSchema, oldSchema, true, false);
  }

  private void enforceDeleteColumnFailure(Schema oldSchema, Schema newSchema, String tableUri) {
    // if old schema couldn't find a key in the new schema map, it should fail
    MapDifference<Integer, String> diff =
        Maps.difference(oldSchema.idToName(), newSchema.idToName());

    if (!diff.entriesOnlyOnLeft().isEmpty()) {
      throw new InvalidSchemaEvolutionException(
          tableUri, newSchema.toString(), oldSchema.toString(), "Some columns are dropped");
    }
    if (!diff.entriesDiffering().isEmpty()) {
      throw new InvalidSchemaEvolutionException(
          tableUri,
          newSchema.toString(),
          oldSchema.toString(),
          "Given same id, column name is different");
    }
  }

  /**
   * Since @param newSchema is considered to be the new source-of-truth table schema, OpenHouse just
   * need to ensure all field-Id are still matching before directly use it to assemble {@link
   * org.apache.iceberg.TableMetadata}
   *
   * <p>Note that Iceberg by itself is case-sensitive by default, the casing info can be lost
   * through the SQL engine layer. Here OH is honoring the default case sensitivity for column
   * name-based resolution.
   *
   * @param oldSchema existing schema in table catalog
   * @param newSchema user-provided schema that may contain evolution
   * @param tableUri tableUri
   */
  protected void validateFields(Schema oldSchema, Schema newSchema, String tableUri) {
    for (Types.NestedField field : oldSchema.columns()) {
      Types.NestedField columnInNewSchema = newSchema.findField(field.name());
      if (columnInNewSchema == null) {
        throw new InvalidSchemaEvolutionException(
            tableUri,
            newSchema.toString(),
            oldSchema.toString(),
            String.format("Column[%s] not found in newSchema", field.name()));
      }

      if (columnInNewSchema.fieldId() != field.fieldId()) {
        throw new InvalidSchemaEvolutionException(
            tableUri,
            newSchema.toString(),
            oldSchema.toString(),
            String.format(
                "Internal Error: Column name [%s] in newSchema has a different fieldId",
                field.name()));
      }
    }
  }
}
