package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.view.model.SqlViewRepresentationIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitOperation;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;

/**
 * Shared constants and builders for the view commit engine tests. Every intent carries the three
 * create-side physical fields, and locations derive from a per-test root so no two tests collide.
 */
public final class ViewTestFixtures {

  public static final String DB = "viewdb";
  public static final String VIEW = "v1";
  public static final String CREATOR = "test_user";
  public static final String SPARK_DIALECT = "spark";
  public static final String TRINO_DIALECT = "trino";
  public static final String LOCAL_STORAGE_TYPE = "local";

  public static final String VIEW_UUID = "11111111-1111-1111-1111-111111111111";

  /** A second request generates its own identity; it never reuses the first. */
  public static final String SECOND_VIEW_UUID = "22222222-2222-2222-2222-222222222222";

  public static final String ENTITY_TYPE_VIEW = "VIEW";

  public static final String ENTITY_TYPE_TABLE = "TABLE";

  /** Unrecognized: a create colliding with one must fail closed. */
  public static final String ENTITY_TYPE_UNKNOWN = "MATERIALIZED_VIEW";

  public static final String SOURCE_DIALECT_SUMMARY_KEY = "sourceDialect";

  public static final String SQL_V1 = "SELECT id, name FROM viewdb.base_table";
  public static final String SQL_V2 = "SELECT id, name, region FROM viewdb.base_table WHERE id > 0";
  public static final String SQL_V3 = "SELECT id FROM viewdb.other_table";

  private ViewTestFixtures() {}

  public static Schema schemaV1() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()));
  }

  /** Materially different from {@link #schemaV1()}, so no structural de-dup. */
  public static Schema schemaV2() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "region", Types.StringType.get()));
  }

  public static SqlViewRepresentationIntent sql(String sqlText, String dialect) {
    return SqlViewRepresentationIntent.builder().sql(sqlText).dialect(dialect).build();
  }

  /** The shape a storage implementation allocates, with identity embedded in the directory. */
  public static String viewLocation(Path root, String databaseId, String viewId, String viewUuid) {
    return root.resolve(databaseId).resolve(viewId + "-" + viewUuid).toString();
  }

  public static String viewLocation(Path root) {
    return viewLocation(root, DB, VIEW, VIEW_UUID);
  }

  public static String allocatedViewLocation(
      Path root, String databaseId, String viewId, String viewUuid) {
    String location = viewLocation(root, databaseId, viewId, viewUuid);
    try {
      Files.createDirectories(Paths.get(location));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return location;
  }

  /**
   * Operation and captured row are explicit: the caller decides CREATE vs REPLACE, and supplies the
   * server-read snapshot (or null for a completed lookup that found absence). Neither is inferred.
   */
  public static ViewCommitIntent createIntent(Path root, HouseTable baseRow) {
    return baseIntent(root, ViewCommitOperation.CREATE, baseRow).build();
  }

  public static ViewCommitIntent replaceIntent(Path root, HouseTable baseRow) {
    return baseIntent(root, ViewCommitOperation.REPLACE, baseRow).build();
  }

  public static ViewCommitIntent.ViewCommitIntentBuilder baseIntent(
      Path root, ViewCommitOperation operation, HouseTable baseRow) {
    return ViewCommitIntent.builder()
        .databaseId(DB)
        .viewId(VIEW)
        .schema(schemaV1())
        .representations(Collections.singletonList(sql(SQL_V1, SPARK_DIALECT)))
        .sourceDialect(SPARK_DIALECT)
        .defaultCatalog("openhouse")
        .defaultNamespace(Namespace.of(DB))
        .viewProperties(userProperties("a", "1"))
        .creator(CREATOR)
        .viewUuid(VIEW_UUID)
        .viewLocation(allocatedViewLocation(root, DB, VIEW, VIEW_UUID))
        .storageType(LOCAL_STORAGE_TYPE)
        .operation(operation)
        .baseRow(baseRow);
  }

  public static Map<String, String> userProperties(String key, String value) {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put(key, value);
    return properties;
  }

  public static List<SqlViewRepresentationIntent> sparkAndTrino(String sqlText) {
    return Arrays.asList(sql(sqlText, SPARK_DIALECT), sql(sqlText, TRINO_DIALECT));
  }

  public static HouseTablePrimaryKey key(String databaseId, String viewId) {
    return HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(viewId).build();
  }

  public static HouseTable row(String entityType, String metadataLocation) {
    return HouseTable.builder()
        .databaseId(DB)
        .tableId(VIEW)
        .tableLocation(metadataLocation)
        .tableVersion("INITIAL_VERSION")
        .storageType(LOCAL_STORAGE_TYPE)
        .entityType(entityType)
        .build();
  }

  public static HouseTable viewRow(String metadataLocation) {
    return row(ENTITY_TYPE_VIEW, metadataLocation);
  }

  public static HouseTable tableRow(String metadataLocation) {
    return row(ENTITY_TYPE_TABLE, metadataLocation);
  }

  /** Pre-discriminator row: House Table resolves it to TABLE on hydration. */
  public static HouseTable legacyRow(String metadataLocation) {
    return row(null, metadataLocation);
  }
}
