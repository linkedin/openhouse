package com.linkedin.openhouse.hts.catalog.model.replication;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;

/** Generic persisted replication-state row used by the destination and checkpoint tables. */
@Builder
@Getter
public class ReplicationStateIcebergRow implements IcebergRow {
  private String sourceClusterId;
  private String sourceTableUUID;
  private Long sourceCreationTime;
  private String destinationClusterId;
  private String destinationTableUUID;
  private Long destinationCreationTime;
  private String payload;
  private String rowVersion;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "sourceClusterId", Types.StringType.get()),
        Types.NestedField.required(2, "sourceTableUUID", Types.StringType.get()),
        Types.NestedField.required(3, "sourceCreationTime", Types.LongType.get()),
        Types.NestedField.required(4, "destinationClusterId", Types.StringType.get()),
        Types.NestedField.required(5, "destinationTableUUID", Types.StringType.get()),
        Types.NestedField.required(6, "destinationCreationTime", Types.LongType.get()),
        Types.NestedField.required(7, "payload", Types.StringType.get()),
        Types.NestedField.optional(8, "rowVersion", Types.StringType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord record = GenericRecord.create(getSchema());
    record.setField("sourceClusterId", sourceClusterId);
    record.setField("sourceTableUUID", sourceTableUUID);
    record.setField("sourceCreationTime", sourceCreationTime);
    record.setField("destinationClusterId", destinationClusterId);
    record.setField("destinationTableUUID", destinationTableUUID);
    record.setField("destinationCreationTime", destinationCreationTime);
    record.setField("payload", payload);
    record.setField("rowVersion", rowVersion);
    return record;
  }

  @Override
  public String getVersionColumnName() {
    return "rowVersion";
  }

  @Override
  public String getNextVersion() {
    return UUID.randomUUID().toString();
  }

  @Override
  public IcebergRowPrimaryKey getIcebergRowPrimaryKey() {
    return ReplicationStateIcebergRowPrimaryKey.builder()
        .sourceClusterId(sourceClusterId)
        .sourceTableUUID(sourceTableUUID)
        .sourceCreationTime(sourceCreationTime)
        .destinationClusterId(destinationClusterId)
        .destinationTableUUID(destinationTableUUID)
        .destinationCreationTime(destinationCreationTime)
        .build();
  }
}
