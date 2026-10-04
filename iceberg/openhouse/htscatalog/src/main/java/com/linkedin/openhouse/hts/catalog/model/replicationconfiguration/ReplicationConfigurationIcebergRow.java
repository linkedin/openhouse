package com.linkedin.openhouse.hts.catalog.model.replicationconfiguration;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import lombok.Builder;
import lombok.Getter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;

@Builder
@Getter
public class ReplicationConfigurationIcebergRow implements IcebergRow {
  private String sourceDatabaseId;

  private String sourceTableId;

  private String destinationClusterId;

  private String destinationDatabaseId;

  private String destinationTableId;

  private String replicationInterval;

  private String version;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "sourceDatabaseId", Types.StringType.get()),
        Types.NestedField.required(2, "sourceTableId", Types.StringType.get()),
        Types.NestedField.required(3, "destinationClusterId", Types.StringType.get()),
        Types.NestedField.required(4, "destinationDatabaseId", Types.StringType.get()),
        Types.NestedField.required(5, "destinationTableId", Types.StringType.get()),
        Types.NestedField.required(6, "replicationInterval", Types.StringType.get()),
        Types.NestedField.required(7, "version", Types.StringType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord record = GenericRecord.create(getSchema());
    record.setField("sourceDatabaseId", sourceDatabaseId);
    record.setField("sourceTableId", sourceTableId);
    record.setField("destinationClusterId", destinationClusterId);
    record.setField("destinationDatabaseId", destinationDatabaseId);
    record.setField("destinationTableId", destinationTableId);
    record.setField("replicationInterval", replicationInterval);
    record.setField("version", version);
    return record;
  }

  @Override
  public String getVersionColumnName() {
    return "version";
  }

  @Override
  public IcebergRowPrimaryKey getIcebergRowPrimaryKey() {
    return ReplicationConfigurationIcebergRowPrimaryKey.builder()
        .sourceDatabaseId(sourceDatabaseId)
        .sourceTableId(sourceTableId)
        .destinationClusterId(destinationClusterId)
        .destinationDatabaseId(destinationDatabaseId)
        .destinationTableId(destinationTableId)
        .build();
  }
}
