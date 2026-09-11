package com.linkedin.openhouse.internal.catalog.view;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;

/**
 * Builds real {@link ViewMetadata}; the candidate version id is arbitrary, Iceberg reassigns it.
 */
public final class ViewMetadataTestUtil {

  private ViewMetadataTestUtil() {}

  public static ViewMetadata metadata(
      String location, Schema schema, Map<String, String> properties) {
    return metadata(
        location,
        schema,
        properties,
        Collections.singletonList(
            ImmutableSQLViewRepresentation.builder()
                .sql(ViewTestFixtures.SQL_V1)
                .dialect(ViewTestFixtures.SPARK_DIALECT)
                .build()),
        UUID.randomUUID().toString());
  }

  public static ViewMetadata metadata(
      String location,
      Schema schema,
      Map<String, String> properties,
      List<? extends ViewRepresentation> representations,
      String uuid) {
    ViewVersion version =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(System.currentTimeMillis())
            .schemaId(schema.schemaId())
            .defaultCatalog("openhouse")
            .defaultNamespace(Namespace.of(ViewTestFixtures.DB))
            .putSummary("operation", "create")
            .putSummary(ViewTestFixtures.SOURCE_DIALECT_SUMMARY_KEY, ViewTestFixtures.SPARK_DIALECT)
            .addAllRepresentations(representations)
            .build();

    return ViewMetadata.builder()
        .assignUUID(uuid)
        .setLocation(location)
        .setCurrentVersion(version, schema)
        .setProperties(properties)
        .build();
  }
}
