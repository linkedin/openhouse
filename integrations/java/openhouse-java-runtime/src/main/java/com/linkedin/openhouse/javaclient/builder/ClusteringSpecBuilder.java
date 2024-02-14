package com.linkedin.openhouse.javaclient.builder;

import com.linkedin.openhouse.tables.client.model.ClusteringColumn;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

public final class ClusteringSpecBuilder extends PartitionSpecBuilder {

  private ClusteringSpecBuilder(Schema schema, PartitionSpec spec) {
    super(schema, spec);
  }

  public static ClusteringSpecBuilder builderFor(Schema schema, PartitionSpec spec) {
    return new ClusteringSpecBuilder(schema, spec);
  }

  public List<ClusteringColumn> build() throws UnsupportedOperationException {
    List<PartitionField> clusteringFields =
        partitionSpec.fields().stream()
            .filter(
                x -> {
                  return ALLOWED_CLUSTERING_TYPEIDS.contains(
                      schema.findField(x.sourceId()).type().typeId());
                })
            .collect(Collectors.toList());
    List<ClusteringColumn> clustering = null;
    if (!clusteringFields.isEmpty()) {
      clustering =
          clusteringFields.stream()
              .map(
                  x -> {
                    ClusteringColumn clusteringColumn = new ClusteringColumn();
                    clusteringColumn.columnName(x.name());
                    return clusteringColumn;
                  })
              .collect(Collectors.toList());
    }
    return clustering;
  }
}
