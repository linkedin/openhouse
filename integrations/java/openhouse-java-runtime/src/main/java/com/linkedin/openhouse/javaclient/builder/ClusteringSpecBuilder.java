package com.linkedin.openhouse.javaclient.builder;

import com.linkedin.openhouse.tables.client.model.ClusteringColumn;
import com.linkedin.openhouse.tables.client.model.Transform;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
                col ->
                    ALLOWED_CLUSTERING_TYPEIDS.contains(
                        schema.findField(col.sourceId()).type().typeId()))
            .collect(Collectors.toList());
    List<ClusteringColumn> clustering = null;
    if (!clusteringFields.isEmpty()) {
      clustering =
          clusteringFields.stream()
              .map(
                  col -> {
                    ClusteringColumn clusteringColumn = new ClusteringColumn();
                    clusteringColumn.columnName(
                        partitionSpec.schema().findColumnName(col.sourceId()));
                    clusteringColumn.transform(
                        buildTransform(col.transform().toString()).orElse(null));
                    return clusteringColumn;
                  })
              .collect(Collectors.toList());
    }
    return clustering;
  }

  private Optional<Transform> buildTransform(String transformStr) {
    Transform transform = new Transform();
    Matcher truncateMatcher = Pattern.compile(TRUNCATE_REGEX).matcher(transformStr);
    if ("identity".equals(transformStr)) {
      return Optional.empty();
    } else if (truncateMatcher.matches()) {
      String width = truncateMatcher.group(1);
      transform.setTransformType(Transform.TransformTypeEnum.TRUNCATE);
      transform.setTransformParams(Arrays.asList(width));
    }
    return Optional.of(transform);
  }
}
