package com.linkedin.openhouse.javaclient.builder;

import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;

/** Parent class to help build OpenHouse specific partitioning and clustering objects. */
public abstract class PartitionSpecBuilder {

  protected static final int MAX_TIME_PARTITIONING_COLUMNS = 1;
  protected static final Type.TypeID ALLOWED_PARTITION_TYPEID = Type.TypeID.TIMESTAMP;

  public static final Set<Type.TypeID> ALLOWED_CLUSTERING_TYPEIDS =
      Collections.unmodifiableSet(
          new HashSet<Type.TypeID>(
              Arrays.asList(Type.TypeID.STRING, Type.TypeID.INTEGER, Type.TypeID.LONG)));
  protected static final String TRUNCATE_REGEX = "truncate\\[(\\d+)\\]";
  public static final Set<String> SUPPORTED_TRANSFORMS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("identity", TRUNCATE_REGEX)));

  protected Schema schema;
  protected PartitionSpec partitionSpec;

  PartitionSpecBuilder(Schema schema, PartitionSpec spec) {
    Preconditions.checkState(
        Collections.disjoint(
            PartitionSpecBuilder.ALLOWED_CLUSTERING_TYPEIDS,
            new HashSet<>(
                Collections.singletonList(PartitionSpecBuilder.ALLOWED_PARTITION_TYPEID))));
    this.schema = schema;
    this.partitionSpec = spec;
    validatePartitionSpec();
  }

  /**
   * Iterate through the partition spec and validate if it is a valid partition spec. Types and
   * Transforms supported: (x) TIMESTAMP type support hour, day, month, year. (x) STRING, INTEGER,
   * LONG type support identity and truncate. No other types are supported.
   */
  private void validatePartitionSpec() {
    partitionSpec.fields().stream()
        .forEach(
            partitionField -> {
              Type.TypeID typeID = schema.findField(partitionField.sourceId()).type().typeId();
              String partitionFieldName =
                  partitionSpec.schema().findField(partitionField.sourceId()).name();
              if (ALLOWED_PARTITION_TYPEID == typeID) {
                if (getSupportedTimePartitionTransforms()
                    .contains(partitionField.transform().toString())) {
                  return;
                }
                throw new IllegalArgumentException(
                    String.format(
                        "Unsupported column: %s, transform: %s provided, "
                            + "please provide one of the following transforms (%s), "
                            + "for example: PARTITIONED BY hours(timestampCol)",
                        partitionFieldName,
                        partitionField.transform().toString(),
                        String.join(",", getSupportedTimePartitionTransforms())));
              } else if (ALLOWED_CLUSTERING_TYPEIDS.contains(typeID)) {
                if (isClusteringSpec(partitionField)) {
                  return;
                }
                throw new IllegalArgumentException(
                    String.format(
                        "Unsupported column: %s, transform: %s provided, "
                            + "please provide one of the following transforms (%s), "
                            + "for example: PARTITIONED BY category",
                        partitionFieldName,
                        partitionField.transform().toString(),
                        String.join(",", getSupportedTimePartitionTransforms())));
              }
              throw new IllegalArgumentException(
                  String.format(
                      "For field %s supplied type %s and transform %s not supported. Supported types are (%s)",
                      partitionFieldName,
                      typeID.name(),
                      partitionField.transform().toString(),
                      String.join(
                          ",",
                          Stream.concat(
                                  Arrays.asList(ALLOWED_PARTITION_TYPEID.name()).stream(),
                                  ALLOWED_CLUSTERING_TYPEIDS.stream()
                                      .map(x -> x.name())
                                      .collect(Collectors.toList())
                                      .stream())
                              .collect(Collectors.toList()))));
            });
  }

  /** Get Supported transforms such as day(), hour(), month(), year() */
  private List<String> getSupportedTimePartitionTransforms() {
    return Arrays.stream(TimePartitionSpec.GranularityEnum.values())
        .map(x -> x.toString().toLowerCase())
        .collect(Collectors.toList());
  }

  private boolean isClusteringSpec(PartitionField partitionField) {
    String transform = partitionField.transform().toString();
    return SUPPORTED_TRANSFORMS.stream().anyMatch(pattern -> transform.matches(pattern));
  }
}
