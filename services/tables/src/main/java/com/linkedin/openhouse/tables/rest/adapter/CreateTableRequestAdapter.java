package com.linkedin.openhouse.tables.rest.adapter;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.common.TableType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.springframework.stereotype.Component;

/**
 * Adapter from Iceberg REST {@link CreateTableRequest} to OpenHouse {@link
 * CreateUpdateTableRequestBody}.
 *
 * <p>v0: partition-spec / clustering / policies are not propagated separately — OpenHouse stores
 * the Iceberg PartitionSpec inside its own metadata file. Stage-create requests are rejected at the
 * controller layer.
 */
@Component
@RequiredArgsConstructor
public class CreateTableRequestAdapter {

  private final ClusterProperties clusterProperties;

  public CreateUpdateTableRequestBody buildFromCreate(Namespace namespace, CreateTableRequest req) {
    NamespaceUtilRest.requireDepthOne(namespace);

    Map<String, String> tableProperties = new HashMap<>();
    Map<String, String> incoming =
        req.properties() == null ? Collections.emptyMap() : req.properties();
    for (Map.Entry<String, String> e : incoming.entrySet()) {
      if (e.getKey() != null && e.getValue() != null) {
        tableProperties.put(e.getKey(), e.getValue());
      }
    }

    String sortOrderJson;
    if (req.writeOrder() != null) {
      sortOrderJson = SortOrderParser.toJson(req.writeOrder());
    } else {
      sortOrderJson = SortOrderParser.toJson(SortOrder.unsorted());
    }

    return CreateUpdateTableRequestBody.builder()
        .tableId(req.name())
        .databaseId(namespace.level(0))
        .clusterId(clusterProperties.getClusterName())
        .schema(SchemaParser.toJson(req.schema(), false))
        .sortOrder(sortOrderJson)
        .baseTableVersion("INITIAL_VERSION")
        .tableProperties(tableProperties)
        .tableType(TableType.PRIMARY_TABLE)
        .stageCreate(false)
        // v0: timePartitioning / clustering / policies / newIntermediateSchemas left null
        .build();
  }
}
