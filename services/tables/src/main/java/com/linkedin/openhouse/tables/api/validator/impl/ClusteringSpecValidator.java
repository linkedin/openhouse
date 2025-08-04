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
    validatePositiveIntegerParameter(transform, Transform.TransformType.TRUNCATE, "width");
  }

  private void validateBucketTransform(Transform transform) {
    validatePositiveIntegerParameter(transform, Transform.TransformType.BUCKET, "bucket count");
  }

  private void validatePositiveIntegerParameter(
      Transform transform, Transform.TransformType transformType, String parameterName) {
    List<String> transformParams = transform.getTransformParams();
    if (CollectionUtils.isEmpty(transformParams)) {
      throw new RequestValidationFailureException(
          String.format("%s transform: parameters can not be empty", transformType));
    }
    if (transformParams.size() > 1) {
      throw new RequestValidationFailureException(
          String.format("%s transform: cannot have more than one parameter", transformType));
    }
    String paramValue = transformParams.get(0);
    try {
      int value = Integer.parseInt(paramValue);
      if (value <= 0) {
        throw new RequestValidationFailureException(
            String.format(
                "%s transform: %s must be positive, got %d", transformType, parameterName, value));
      }
    } catch (NumberFormatException e) {
      throw new RequestValidationFailureException(
          String.format("%s transform: parameters must be numeric string", transformType));
    }
  }
}
