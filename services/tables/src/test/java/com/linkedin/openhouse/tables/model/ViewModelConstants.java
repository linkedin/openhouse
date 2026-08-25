package com.linkedin.openhouse.tables.model;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

/**
 * Deterministic fixtures for the /v2 views wire surface. Everything here is a fixed literal: no
 * random identifiers, no {@code UUID.randomUUID()} and no {@code System.currentTimeMillis()}, so
 * contract assertions stay byte-stable across runs.
 */
public final class ViewModelConstants {

  private ViewModelConstants() {}

  public static final String VIEW_ID = "my_view";
  public static final String DATABASE_ID = "my_database";
  public static final String CLUSTER_ID = "my-cluster";
  public static final String VIEW_URI = "my-cluster.my_database.my_view";
  public static final String METADATA_LOCATION =
      "file:/tmp/openhouse/my_database/my_view/metadata/00000-fixed.metadata.json";
  public static final String VIEW_VERSION =
      "file:/tmp/openhouse/my_database/my_view/metadata/00000-fixed.metadata.json";
  public static final long CREATION_TIME = 1651002318265L;

  /**
   * Distinct sentinels for {@code metadataLocation} and {@code viewVersion}. In production the two
   * are equal (design doc §5: viewVersion is the current metadataLocation), which is what the
   * general fixtures model. That equality would, however, let a serialization bug swap the two
   * Jackson property associations undetected, so this pair deliberately breaks it for the
   * serialization-freeze test and pins each field independently.
   */
  public static final String DISTINCT_METADATA_LOCATION =
      "file:/tmp/openhouse/my_database/my_view/metadata/sentinel-metadata-location.metadata.json";

  public static final String DISTINCT_VIEW_VERSION =
      "file:/tmp/openhouse/my_database/my_view/metadata/sentinel-view-version.metadata.json";

  public static final String SOURCE_DIALECT = "spark";
  public static final String SQL_REPRESENTATION_TYPE = "sql";
  public static final String VIEW_SQL = "SELECT id, name FROM my_database.my_table";
  public static final String DEFAULT_CATALOG = "openhouse";

  /** Iceberg schema JSON with unique, explicit field ids. */
  public static final String VIEW_SCHEMA_LITERAL =
      "{\"type\": \"struct\", \"schema-id\": 0, \"fields\": ["
          + "{\"id\": 1, \"required\": true, \"name\": \"id\", \"type\": \"string\"}, "
          + "{\"id\": 2, \"required\": true, \"name\": \"name\", \"type\": \"string\"}]}";

  public static final ViewRepresentation SPARK_REPRESENTATION =
      ViewRepresentation.builder()
          .type(SQL_REPRESENTATION_TYPE)
          .sql(VIEW_SQL)
          .dialect(SOURCE_DIALECT)
          .build();

  public static final List<String> DEFAULT_NAMESPACE =
      Collections.unmodifiableList(Collections.singletonList(DATABASE_ID));

  public static final Map<String, String> VIEW_PROPERTIES;

  static {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("owner", "openhouse");
    VIEW_PROPERTIES = Collections.unmodifiableMap(properties);
  }

  /** Every field populated, including both nullable optional fields and a replace token. */
  public static CreateUpdateViewRequestBody fullyPopulatedRequest() {
    return baseRequestBuilder().baseViewVersion(METADATA_LOCATION).build();
  }

  /** POST create shape where the caller omits the base version entirely. */
  public static CreateUpdateViewRequestBody createRequestWithoutBaseVersion() {
    return baseRequestBuilder().build();
  }

  /** POST create shape where the caller sends the table-style initial version token. */
  public static CreateUpdateViewRequestBody createRequestWithInitialBaseVersion() {
    return baseRequestBuilder().baseViewVersion(INITIAL_TABLE_VERSION).build();
  }

  /** Fully populated pointer response for an item GET. */
  public static GetViewResponseBody pointerResponse() {
    return GetViewResponseBody.builder()
        .viewId(VIEW_ID)
        .databaseId(DATABASE_ID)
        .clusterId(CLUSTER_ID)
        .viewUri(VIEW_URI)
        .metadataLocation(METADATA_LOCATION)
        .viewVersion(VIEW_VERSION)
        .creationTime(CREATION_TIME)
        .build();
  }

  /** Sparse list element: identifiers only, as the list path populates nothing else. */
  public static GetViewResponseBody sparseListElement(String viewId) {
    return GetViewResponseBody.builder().viewId(viewId).databaseId(DATABASE_ID).build();
  }

  /**
   * Pointer response whose {@code metadataLocation} and {@code viewVersion} carry distinct
   * sentinels rather than the production-equal value, so a serialization freeze can pin the two
   * fields independently. Use {@link #pointerResponse()} wherever production equality matters.
   */
  public static GetViewResponseBody pointerResponseWithDistinctPointers() {
    return pointerResponse()
        .toBuilder()
        .metadataLocation(DISTINCT_METADATA_LOCATION)
        .viewVersion(DISTINCT_VIEW_VERSION)
        .build();
  }

  /** Deterministic single page of sparse identifier-only elements. */
  public static Page<GetViewResponseBody> sparseListPage() {
    List<GetViewResponseBody> content =
        Arrays.asList(sparseListElement("my_view"), sparseListElement("my_other_view"));
    return new PageImpl<>(content, PageRequest.of(0, 50), content.size());
  }

  public static GetAllViewsResponseBody listResponse() {
    return GetAllViewsResponseBody.builder().pageResults(sparseListPage()).build();
  }

  // -----------------------------------------------------------------------------------------
  // Negative-path fixtures
  // -----------------------------------------------------------------------------------------

  /** Not JSON at all. */
  public static final String MALFORMED_SCHEMA_LITERAL = "{\"type\": \"struct\", \"fields\": [";

  /**
   * The Spark {@code StructType} JSON an engine may reach for by mistake: structurally similar, but
   * its fields carry {@code nullable}/{@code metadata} instead of the field ids Iceberg requires.
   */
  public static final String SPARK_STRUCT_TYPE_SCHEMA_LITERAL =
      "{\"type\": \"struct\", \"fields\": ["
          + "{\"name\": \"id\", \"type\": \"string\", \"nullable\": true, \"metadata\": {}}, "
          + "{\"name\": \"name\", \"type\": \"string\", \"nullable\": true, \"metadata\": {}}]}";

  /**
   * Two fields sharing field id 1. Iceberg's own parser rejects this while building the schema, so
   * the validator deliberately has no duplicate-id check of its own.
   */
  public static final String DUPLICATE_FIELD_ID_SCHEMA_LITERAL =
      "{\"type\": \"struct\", \"schema-id\": 0, \"fields\": ["
          + "{\"id\": 1, \"required\": true, \"name\": \"id\", \"type\": \"string\"}, "
          + "{\"id\": 1, \"required\": true, \"name\": \"name\", \"type\": \"string\"}]}";

  /**
   * A valid Iceberg schema padded with insignificant JSON whitespace to exactly {@code totalBytes}
   * UTF-8 bytes, so a size boundary can be probed without also changing whether it parses.
   */
  public static String schemaOfExactUtf8Size(int totalBytes) {
    int padding = totalBytes - VIEW_SCHEMA_LITERAL.length();
    if (padding < 0) {
      throw new IllegalArgumentException(
          "Requested schema size is smaller than the base schema literal");
    }
    return "{" + spaces(padding) + VIEW_SCHEMA_LITERAL.substring(1);
  }

  /** Opaque SQL padded to exactly {@code totalBytes} UTF-8 bytes. */
  public static String sqlOfExactUtf8Size(int totalBytes) {
    int padding = totalBytes - VIEW_SQL.length();
    if (padding < 0) {
      throw new IllegalArgumentException("Requested SQL size is smaller than the base SQL literal");
    }
    return VIEW_SQL + spaces(padding);
  }

  /**
   * SQL built entirely from a two-byte character, so its character count and its UTF-8 byte count
   * differ by a factor of two. Used to prove the size rules count bytes.
   */
  public static String multiByteSql(int characterCount) {
    char[] characters = new char[characterCount];
    Arrays.fill(characters, 'é');
    return new String(characters);
  }

  private static String spaces(int count) {
    char[] characters = new char[count];
    Arrays.fill(characters, ' ');
    return new String(characters);
  }

  private static CreateUpdateViewRequestBody.CreateUpdateViewRequestBodyBuilder
      baseRequestBuilder() {
    return CreateUpdateViewRequestBody.builder()
        .viewId(VIEW_ID)
        .databaseId(DATABASE_ID)
        .clusterId(CLUSTER_ID)
        .schema(VIEW_SCHEMA_LITERAL)
        .representations(Collections.singletonList(SPARK_REPRESENTATION))
        .sourceDialect(SOURCE_DIALECT)
        .defaultCatalog(DEFAULT_CATALOG)
        .defaultNamespace(DEFAULT_NAMESPACE)
        .viewProperties(VIEW_PROPERTIES);
  }
}
