package com.linkedin.openhouse.tables.rest.adapter;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.common.TableType;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.springframework.stereotype.Component;

/**
 * Builds an OpenHouse {@link CreateUpdateTableRequestBody} from an Iceberg {@link TableMetadata}
 * produced by a REST commit (i.e. base + MetadataUpdates replayed).
 *
 * <p>v0 simplifications:
 *
 * <ul>
 *   <li>timePartitioning / clustering — null (OpenHouse stores the spec inside the metadata file
 *       directly; the REST adapter does not synthesize OpenHouse's extra hints)
 *   <li>newIntermediateSchemas — null
 *   <li>policies — null
 *   <li>tableType — PRIMARY_TABLE always; replicas are not writable via REST
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class CommitAdapter {

  private final ClusterProperties clusterProperties;

  /**
   * @param namespace OpenHouse database namespace (depth 1)
   * @param tableName table name within the namespace
   * @param newMetadata the post-update Iceberg metadata
   * @param baseTableVersion {@code base.metadataFileLocation()} for updates, or {@code null} on
   *     create — in which case "INITIAL_VERSION" is used
   */
  public CreateUpdateTableRequestBody buildCreateUpdateBody(
      Namespace namespace, String tableName, TableMetadata newMetadata, String baseTableVersion) {
    NamespaceUtilRest.requireDepthOne(namespace);

    Map<String, String> tableProperties = new HashMap<>();
    if (newMetadata.properties() != null) {
      for (Map.Entry<String, String> e : newMetadata.properties().entrySet()) {
        if (e.getKey() != null && e.getValue() != null) {
          tableProperties.put(e.getKey(), e.getValue());
        }
      }
    }

    return CreateUpdateTableRequestBody.builder()
        .tableId(tableName)
        .databaseId(namespace.level(0))
        .clusterId(clusterProperties.getClusterName())
        .schema(SchemaParser.toJson(newMetadata.schema(), false))
        .sortOrder(SortOrderParser.toJson(newMetadata.sortOrder()))
        .baseTableVersion(baseTableVersion == null ? "INITIAL_VERSION" : baseTableVersion)
        .tableProperties(tableProperties)
        .tableType(TableType.PRIMARY_TABLE)
        // v0: timePartitioning / clustering / policies / newIntermediateSchemas left null
        .build();
  }
}
