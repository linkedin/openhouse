package com.linkedin.openhouse.javaclient.builder;

import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;

/** Parent class to help build OpenHouse specific partitioning and clustering objects. */
public abstract class PartitionSpecBuilder {

  protected static final int MAX_TIME_PARTITIONING_COLUMNS = 1;
  protected static final Type.TypeID ALLOWED_PARTITION_TYPEID = Type.TypeID.TIMESTAMP;

  protected static final Set<Type.TypeID> ALLOWED_CLUSTERING_TYPEIDS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  Type.TypeID.STRING, Type.TypeID.INTEGER, Type.TypeID.LONG, Type.TypeID.DATE)));

  protected Schema schema;
  protected PartitionSpec partitionSpec;

  PartitionSpecBuilder(Schema schema, PartitionSpec spec) {
    Preconditions.checkState(
        Collections.disjoint(
            ALLOWED_CLUSTERING_TYPEIDS,
            new HashSet<>(Collections.singletonList(ALLOWED_PARTITION_TYPEID))));
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
    partitionSpec
        .fields()
        .forEach(
            field -> {
              Type.TypeID typeId = schema.findField(field.sourceId()).type().typeId();
              String fieldName = partitionSpec.schema().findField(field.sourceId()).name();
              String transformString = field.transform().toString();

              if (ALLOWED_PARTITION_TYPEID == typeId) {
                // TIMESTAMP type - check for supported time partition transforms
                if (!getSupportedTimePartitionTransforms().contains(transformString)) {
                  throw new IllegalArgumentException(
                      String.format(
                          "Unsupported column: %s, transform: %s provided, "
                              + "please provide one of the following transforms (%s), "
                              + "for example: PARTITIONED BY hours(timestampCol)",
                          fieldName,
                          transformString,
                          String.join(",", getSupportedTimePartitionTransforms())));
                }
              } else if (ALLOWED_CLUSTERING_TYPEIDS.contains(typeId)) {
                // Clustering types - check for supported clustering transforms
                if (!isValidClusteringTransform(transformString)) {
                  throw new IllegalArgumentException(
                      String.format(
                          "Unsupported column: %s, transform: %s provided, "
                              + "please provide one of the following transforms (identity, truncate, bucket), "
                              + "for example: PARTITIONED BY category",
                          fieldName, transformString));
                }
              } else {
                // Unsupported type
                throw new IllegalArgumentException(
                    String.format(
                        "For field %s supplied type %s and transform %s not supported. Supported types are (%s)",
                        fieldName,
                        typeId.name(),
                        transformString,
                        String.join(
                            ",",
                            Stream.concat(
                                    Arrays.asList(ALLOWED_PARTITION_TYPEID.name()).stream(),
                                    ALLOWED_CLUSTERING_TYPEIDS.stream().map(Enum::name))
                                .collect(Collectors.toList()))));
              }
            });
  }

  /** Get supported time partition transforms such as day, hour, month, year */
  private List<String> getSupportedTimePartitionTransforms() {
    return Arrays.stream(TimePartitionSpec.GranularityEnum.values())
        .map(x -> x.toString().toLowerCase())
        .collect(Collectors.toList());
  }

  /** Check if transform is valid for clustering (identity, truncate, bucket) */
  private boolean isValidClusteringTransform(String transformString) {
    return transformString.equals("identity")
        || transformString.startsWith("truncate[")
        || transformString.startsWith("bucket[");
  }
}
