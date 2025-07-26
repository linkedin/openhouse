package com.linkedin.openhouse.javaclient.builder;

import com.linkedin.openhouse.tables.client.model.ClusteringColumn;
import com.linkedin.openhouse.tables.client.model.Transform;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;

public final class ClusteringSpecBuilder extends PartitionSpecBuilder {

  private ClusteringSpecBuilder(Schema schema, PartitionSpec spec) {
    super(schema, spec);
  }

  public static ClusteringSpecBuilder builderFor(Schema schema, PartitionSpec spec) {
    return new ClusteringSpecBuilder(schema, spec);
  }

  public List<ClusteringColumn> build() {
    return partitionSpec.fields().stream()
        .flatMap(
            field -> {
              Type.TypeID typeId = schema.findField(field.sourceId()).type().typeId();

              if (!ALLOWED_CLUSTERING_TYPEIDS.contains(typeId)) {
                return java.util.stream.Stream.empty(); // Skip invalid fields
              }

              // Single string comparison to validate and determine transform type
              String transformString = field.transform().toString();
              Transform transform = null;

              if (transformString.equals("identity")) {
                // Identity transform - leave transform as null
              } else if (transformString.startsWith("truncate[")) {
                transform = new Transform();
                transform.setTransformType(Transform.TransformTypeEnum.TRUNCATE);
                transform.setTransformParams(extractClusterTransformParameters(field.transform()));
              } else if (transformString.startsWith("bucket[")) {
                transform = new Transform();
                transform.setTransformType(Transform.TransformTypeEnum.BUCKET);
                transform.setTransformParams(extractClusterTransformParameters(field.transform()));
              } else {
                // Unsupported transform - skip this field
                return java.util.stream.Stream.empty();
              }

              // Build the clustering column
              ClusteringColumn clusteringColumn = new ClusteringColumn();
              clusteringColumn.columnName(partitionSpec.schema().findColumnName(field.sourceId()));
              clusteringColumn.transform(transform);
              return java.util.stream.Stream.of(clusteringColumn);
            })
        .collect(Collectors.toList());
  }

  /** Extract parameters from transform strings like "truncate[10]" or "bucket[4]" */
  private List<String> extractClusterTransformParameters(
      org.apache.iceberg.transforms.Transform<?, ?> transform) {
    String transformString = transform.toString();
    if (transformString.startsWith("truncate[") || transformString.startsWith("bucket[")) {
      // Extract number from within brackets
      int start = transformString.indexOf('[') + 1;
      int end = transformString.indexOf(']');
      if (start > 0 && end > start) {
        return Collections.singletonList(transformString.substring(start, end));
      }
    }
    return Collections.emptyList();
  }
}
