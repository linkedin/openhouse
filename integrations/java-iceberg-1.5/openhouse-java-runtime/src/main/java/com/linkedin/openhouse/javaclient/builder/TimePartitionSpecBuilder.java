package com.linkedin.openhouse.javaclient.builder;

import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * A builder to build {@link TimePartitionSpec} from {@link PartitionSpec}.
 *
 * <p>The {@link #build()} method validates that {@link PartitionSpec} has single timestamp-based
 * column partitioning. Throws useful exception otherwise.
 */
public final class TimePartitionSpecBuilder extends PartitionSpecBuilder {

  private TimePartitionSpecBuilder(Schema schema, PartitionSpec spec) {
    super(schema, spec);
  }

  public static TimePartitionSpecBuilder builderFor(Schema schema, PartitionSpec spec) {
    return new TimePartitionSpecBuilder(schema, spec);
  }

  public TimePartitionSpec build() throws UnsupportedOperationException {
    List<PartitionField> timeBasedPartitionFields =
        partitionSpec.fields().stream()
            .filter(
                x -> {
                  return schema.findField(x.sourceId()).type().typeId() == ALLOWED_PARTITION_TYPEID;
                })
            .collect(Collectors.toList());
    if (timeBasedPartitionFields.size() > MAX_TIME_PARTITIONING_COLUMNS) {
      throw new IllegalArgumentException(
          String.format(
              "OpenHouse only supports %d timestamp-based column partitioning, %s were provided: %s",
              MAX_TIME_PARTITIONING_COLUMNS,
              timeBasedPartitionFields.size(),
              String.join(
                  ", ",
                  timeBasedPartitionFields.stream()
                      .map(x -> x.name())
                      .collect(Collectors.toList()))));
    }
    TimePartitionSpec timePartitionSpec = null;
    if (!timeBasedPartitionFields.isEmpty()) {
      timePartitionSpec = new TimePartitionSpec();
      timePartitionSpec.columnName(
          partitionSpec.schema().findField(timeBasedPartitionFields.get(0).sourceId()).name());
      timePartitionSpec.setGranularity(
          TimePartitionSpec.GranularityEnum.fromValue(
              timeBasedPartitionFields.get(0).transform().toString().toUpperCase()));
    }
    return timePartitionSpec;
  }
}
