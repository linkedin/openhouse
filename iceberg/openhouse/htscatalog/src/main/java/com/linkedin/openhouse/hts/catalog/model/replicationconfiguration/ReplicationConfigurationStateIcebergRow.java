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
public class ReplicationConfigurationStateIcebergRow implements IcebergRow {
  private String sourceDatabaseId;

  private String sourceTableId;

  private Boolean configured;

  private String version;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "sourceDatabaseId", Types.StringType.get()),
        Types.NestedField.required(2, "sourceTableId", Types.StringType.get()),
        Types.NestedField.required(3, "configured", Types.BooleanType.get()),
        Types.NestedField.required(4, "version", Types.StringType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord record = GenericRecord.create(getSchema());
    record.setField("sourceDatabaseId", sourceDatabaseId);
    record.setField("sourceTableId", sourceTableId);
    record.setField("configured", configured);
    record.setField("version", version);
    return record;
  }

  @Override
  public String getVersionColumnName() {
    return "version";
  }

  @Override
  public IcebergRowPrimaryKey getIcebergRowPrimaryKey() {
    return ReplicationConfigurationStateIcebergRowPrimaryKey.builder()
        .sourceDatabaseId(sourceDatabaseId)
        .sourceTableId(sourceTableId)
        .build();
  }
}
