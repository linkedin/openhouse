package com.linkedin.openhouse.hts.catalog.model.replicationconfiguration;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import lombok.Builder;
import lombok.Getter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

@Builder
@Getter
public class ReplicationConfigurationStateIcebergRowPrimaryKey implements IcebergRowPrimaryKey {
  private String sourceDatabaseId;

  private String sourceTableId;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "sourceDatabaseId", Types.StringType.get()),
        Types.NestedField.required(2, "sourceTableId", Types.StringType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord record = GenericRecord.create(getSchema());
    record.setField("sourceDatabaseId", sourceDatabaseId);
    record.setField("sourceTableId", sourceTableId);
    return record;
  }

  @Override
  public Expression getSearchExpression() {
    return Expressions.and(
        Expressions.equal("sourceDatabaseId", sourceDatabaseId),
        Expressions.equal("sourceTableId", sourceTableId));
  }

  @Override
  public IcebergRow buildIcebergRow(Record record) {
    return ReplicationConfigurationStateIcebergRow.builder()
        .sourceDatabaseId((String) record.getField("sourceDatabaseId"))
        .sourceTableId((String) record.getField("sourceTableId"))
        .configured((Boolean) record.getField("configured"))
        .version((String) record.getField("version"))
        .build();
  }
}
