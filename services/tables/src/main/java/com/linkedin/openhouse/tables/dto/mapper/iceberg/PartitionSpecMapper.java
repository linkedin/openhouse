package com.linkedin.openhouse.tables.dto.mapper.iceberg;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;

import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimeGranularity;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Transform;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.mapstruct.Mapper;
import org.mapstruct.Named;

/**
 * Mapper class to bridge Iceberg's {@link PartitionSpec} and OpenHouse's {@link TimePartitionSpec}
 * and {@link ClusteringColumn}.
 */
@Mapper(componentModel = "spring")
public class PartitionSpecMapper {

  private static final int MAX_TIME_PARTITIONING_COLUMNS = 1;
  private static final Type.TypeID ALLOWED_PARTITION_TYPEID = Type.TypeID.TIMESTAMP;
  private static final Set<Type.TypeID> ALLOWED_CLUSTERING_TYPEIDS =
      Collections.unmodifiableSet(
          new HashSet<Type.TypeID>(
              Arrays.asList(Type.TypeID.STRING, Type.TypeID.INTEGER, Type.TypeID.LONG)));
  private static final String TRUNCATE_REGEX = "truncate\\[(\\d+)\\]";
  private static final Set<String> SUPPORTED_TRANSFORMS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("identity", TRUNCATE_REGEX)));

  /**
   * Given an Iceberg {@link Table}, extract OpenHouse {@link TimePartitionSpec} If Table is
   * un-partitioned returns a null value.
   *
   * @param table an iceberg table
   * @return TimePartitionSpec
   */
  @Named("toTimePartitionSpec")
  public TimePartitionSpec toTimePartitionSpec(Table table) {
    PartitionSpec partitionSpec = table.spec();
    List<PartitionField> timeBasedPartitionFields =
        partitionSpec.fields().stream()
            .filter(
                x -> {
                  validatePartitionField(table.schema(), x);
                  return table.schema().findField(x.sourceId()).type().typeId()
                      == ALLOWED_PARTITION_TYPEID;
                })
            .collect(Collectors.toList());
    if (timeBasedPartitionFields.size() > MAX_TIME_PARTITIONING_COLUMNS) {
      throw new IllegalStateException("Partitioning has more than one time-based columns");
    }
    TimePartitionSpec timePartitionSpec = null;
    if (!timeBasedPartitionFields.isEmpty()) {
      timePartitionSpec =
          TimePartitionSpec.builder()
              .columnName(
                  partitionSpec
                      .schema()
                      .findField(timeBasedPartitionFields.get(0).sourceId())
                      .name())
              .granularity(toGranularity(timeBasedPartitionFields.get(0)).get())
              .build();
    }
    return timePartitionSpec;
  }

  /**
   * Given an input partition field, validate if constraints for partitioning and clustering are
   * met.
   *
   * @param schema table schema to lookup the partition field.
   * @param partitionField partition field to be validated.
   * @throws {@link IllegalStateException} if the partition field object is in an unexpected state.
   */
  private void validatePartitionField(Schema schema, PartitionField partitionField) {
    Type.TypeID typeID = schema.findField(partitionField.sourceId()).type().typeId();
    if (ALLOWED_PARTITION_TYPEID == typeID) {
      if (toGranularity(partitionField).isPresent()) {
        return;
      }
    } else if (ALLOWED_CLUSTERING_TYPEIDS.contains(typeID)) {
      String transform = partitionField.transform().toString();
      if (SUPPORTED_TRANSFORMS.stream().anyMatch(pattern -> transform.matches(pattern))) {
        return;
      }
    }
    throw new IllegalStateException(
        String.format(
            "For field %s supplied type %s and transform %s not supported",
            partitionField.name(), typeID.name(), partitionField.transform().toString()));
  }

  /**
   * Given an Iceberg {@link Table}, extract OpenHouse {@link ClusteringColumn} If Table is not
   * clustered returns a null value.
   *
   * @param table an iceberg table
   * @return {@link ClusteringColumn}
   */
  @Named("toClusteringSpec")
  public List<ClusteringColumn> toClusteringSpec(Table table) {
    PartitionSpec partitionSpec = table.spec();
    List<PartitionField> clusteringFields =
        partitionSpec.fields().stream()
            .filter(
                x -> {
                  validatePartitionField(table.schema(), x);
                  return ALLOWED_CLUSTERING_TYPEIDS.contains(
                      table.schema().findField(x.sourceId()).type().typeId());
                })
            .collect(Collectors.toList());
    if (clusteringFields.size() > ValidatorConstants.MAX_ALLOWED_CLUSTERING_COLUMNS) {
      throw new IllegalStateException(
          String.format(
              "Max allowed clustering columns supported %s, actual %s",
              ValidatorConstants.MAX_ALLOWED_CLUSTERING_COLUMNS, clusteringFields.size()));
    }
    List<ClusteringColumn> clustering = null;
    if (!clusteringFields.isEmpty()) {
      clustering =
          clusteringFields.stream()
              .map(
                  x ->
                      ClusteringColumn.builder()
                          .columnName(table.schema().findColumnName(x.sourceId()))
                          .transform(toTransform(x).orElse(null))
                          .build())
              .collect(Collectors.toList());
    }
    return clustering;
  }

  /**
   * Given a OpenHouse {@link TableDto} generate an Iceberg {@link PartitionSpec} from it.
   *
   * @return tableDto TableDto
   * @throws RequestValidationFailureException when partitioning column is invalid.
   */
  public PartitionSpec toPartitionSpec(TableDto tableDto) {
    Schema schema = getSchemaFromSchemaJson(tableDto.getSchema().trim());
    TimePartitionSpec timePartitioning = tableDto.getTimePartitioning();
    List<ClusteringColumn> clustering = tableDto.getClustering();
    PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
    try {
      if (timePartitioning != null) {
        switch (timePartitioning.getGranularity()) {
          case DAY:
            partitionSpecBuilder.day(timePartitioning.getColumnName());
            break;
          case HOUR:
            partitionSpecBuilder.hour(timePartitioning.getColumnName());
            break;
          case MONTH:
            partitionSpecBuilder.month(timePartitioning.getColumnName());
            break;
          case YEAR:
            partitionSpecBuilder.year(timePartitioning.getColumnName());
            break;
          default:
            throw new IllegalArgumentException(
                String.format(
                    "Granularity: %s is not supported", timePartitioning.getGranularity()));
        }
      }
      if (clustering != null) {
        if (clustering.size() > ValidatorConstants.MAX_ALLOWED_CLUSTERING_COLUMNS) {
          throw new IllegalArgumentException(
              String.format(
                  "Max allowed clustering columns supported are %s, specified are %s",
                  ValidatorConstants.MAX_ALLOWED_CLUSTERING_COLUMNS, clustering.size()));
        }
        for (ClusteringColumn clusteringField : clustering) {
          Types.NestedField field = schema.findField(clusteringField.getColumnName());
          if (field == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Clustering column %s not found in the schema",
                    clusteringField.getColumnName()));
          }
          Type.TypeID typeID = field.type().typeId();
          if (!ALLOWED_CLUSTERING_TYPEIDS.contains(typeID)) {
            throw new IllegalArgumentException(
                String.format(
                    "Column name %s of type %s is not supported clustering type",
                    clusteringField.getColumnName(), typeID.name()));
          }
          if (clusteringField.getTransform() != null) {
            Transform transform = clusteringField.getTransform();
            switch (transform.getTransformType()) {
              case TRUNCATE:
                partitionSpecBuilder.truncate(
                    clusteringField.getColumnName(),
                    Integer.parseInt(transform.getTransformParams().get(0)));
                break;
              default:
                throw new IllegalArgumentException(
                    String.format(
                        "Unsupported transform %s for clustering column %s",
                        transform.getTransformType().toString(), clusteringField.getColumnName()));
            }
          } else {
            // identity transform
            partitionSpecBuilder.identity(clusteringField.getColumnName());
          }
        }
      }
    } catch (IllegalArgumentException ex) {
      // iceberg throws this exception for .day()/.hour()/.month()/.year() when column is missing
      throw new RequestValidationFailureException(
          "Adding partition spec failed:" + ex.getMessage());
    }
    return partitionSpecBuilder.build();
  }

  /**
   * Given a {@link PartitionField}, determine if its transformation is a time-based one, ie.
   * hour(), day(), month(), year() and return the corresponding granularity if transformation is
   * non time-based one, return empty optional.
   *
   * @param partitionField partitionField
   * @return optional.empty() or optional.of(granularity)
   */
  private Optional<TimeGranularity> toGranularity(PartitionField partitionField) {
    /* String based comparison is necessary as the classes are package-private */
    TimeGranularity granularity = null;
    switch (partitionField.transform().toString()) {
      case "year":
        granularity = TimeGranularity.YEAR;
        break;
      case "month":
        granularity = TimeGranularity.MONTH;
        break;
      case "hour":
        granularity = TimeGranularity.HOUR;
        break;
      case "day":
        granularity = TimeGranularity.DAY;
        break;
      default:
        break;
    }
    return Optional.ofNullable(granularity);
  }

  /**
   * Given a {@link PartitionField}, determine if its transformation is a clustering one, ie.
   * truncate, and return the corresponding transform.
   *
   * @param partitionField partitionField
   * @return Transform
   */
  private Optional<Transform> toTransform(PartitionField partitionField) {
    /* String based comparison is necessary as the classes are package-private */
    Transform transform = null;
    String icebergTransform = partitionField.transform().toString();
    Matcher truncateMatcher = Pattern.compile(TRUNCATE_REGEX).matcher(icebergTransform);
    if (truncateMatcher.matches()) {
      String width = truncateMatcher.group(1);
      transform =
          Transform.builder()
              .transformType(Transform.TransformType.TRUNCATE)
              .transformParams(Arrays.asList(width))
              .build();
    }
    return Optional.ofNullable(transform);
  }
}
