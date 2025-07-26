package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Transform;
import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ClusteringSpecValidator {
  public void validate(
      List<ClusteringColumn> clusteringColumns, String databaseId, String tableId) {
    validate(clusteringColumns, databaseId, tableId, null);
  }

  public void validate(
      List<ClusteringColumn> clusteringColumns,
      String databaseId,
      String tableId,
      String schemaJson) {
    if (clusteringColumns.size() > ValidatorConstants.MAX_ALLOWED_CLUSTERING_COLUMNS) {
      throw new RequestValidationFailureException(
          String.format(
              "table %s.%s has %s clustering columns specified, max clustering columns supported is %d",
              databaseId,
              tableId,
              clusteringColumns.size(),
              ValidatorConstants.MAX_ALLOWED_CLUSTERING_COLUMNS));
    }
    IntStream.range(0, clusteringColumns.size())
        .forEach(
            idx -> {
              if (clusteringColumns.get(idx) == null) {
                throw new RequestValidationFailureException(
                    String.format(
                        "table %s.%s clustering[%d] : cannot be null", databaseId, tableId, idx));
              }
            });

    for (ClusteringColumn col : clusteringColumns) {
      Transform transform = col.getTransform();
      if (transform != null) {
        if (transform.getTransformType() == Transform.TransformType.TRUNCATE) {
          validateTruncateTransform(transform);
        } else if (transform.getTransformType() == Transform.TransformType.BUCKET) {
          validateBucketTransform(transform);
        }
      }
    }
  }

  private void validateTruncateTransform(Transform transform) {
    List<String> transformParams = transform.getTransformParams();
    if (CollectionUtils.isEmpty(transformParams)) {
      throw new RequestValidationFailureException(
          String.format(
              "%s transform: parameters can not be empty", Transform.TransformType.TRUNCATE));
    }
    if (transformParams.size() > 1) {
      throw new RequestValidationFailureException(
          String.format(
              "%s transform: cannot have more than one parameter",
              Transform.TransformType.TRUNCATE));
    }
    String width = transformParams.get(0);
    try {
      int w = Integer.parseInt(width);
      if (w <= 0) {
        throw new RequestValidationFailureException(
            String.format(
                "%s transform: width must be positive, got %d",
                Transform.TransformType.TRUNCATE, w));
      }
    } catch (NumberFormatException e) {
      throw new RequestValidationFailureException(
          String.format(
              "%s transform: parameters must be numeric string", Transform.TransformType.TRUNCATE));
    }
  }

  private void validateBucketTransform(Transform transform) {
    List<String> transformParams = transform.getTransformParams();
    if (CollectionUtils.isEmpty(transformParams)) {
      throw new RequestValidationFailureException(
          String.format(
              "%s transform: parameters can not be empty", Transform.TransformType.BUCKET));
    }
    if (transformParams.size() > 1) {
      throw new RequestValidationFailureException(
          String.format(
              "%s transform: cannot have more than one parameter", Transform.TransformType.BUCKET));
    }
    String bucketCount = transformParams.get(0);
    try {
      int count = Integer.parseInt(bucketCount);
      if (count <= 0) {
        throw new RequestValidationFailureException(
            String.format(
                "%s transform: bucket count must be positive, got %d",
                Transform.TransformType.BUCKET, count));
      }
    } catch (NumberFormatException e) {
      throw new RequestValidationFailureException(
          String.format(
              "%s transform: parameters must be numeric string", Transform.TransformType.BUCKET));
    }
  }
}
