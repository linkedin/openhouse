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
public class ReplicationConfigurationIcebergRowPrimaryKey implements IcebergRowPrimaryKey {
  private String sourceDatabaseId;

  private String sourceTableId;

  private String destinationClusterId;

  private String destinationDatabaseId;

  private String destinationTableId;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "sourceDatabaseId", Types.StringType.get()),
        Types.NestedField.required(2, "sourceTableId", Types.StringType.get()),
        Types.NestedField.optional(3, "destinationClusterId", Types.StringType.get()),
        Types.NestedField.optional(4, "destinationDatabaseId", Types.StringType.get()),
        Types.NestedField.optional(5, "destinationTableId", Types.StringType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord record = GenericRecord.create(getSchema());
    record.setField("sourceDatabaseId", sourceDatabaseId);
    record.setField("sourceTableId", sourceTableId);
    record.setField("destinationClusterId", destinationClusterId);
    record.setField("destinationDatabaseId", destinationDatabaseId);
    record.setField("destinationTableId", destinationTableId);
    return record;
  }

  @Override
  public Expression getSearchExpression() {
    return Expressions.and(
        Expressions.equal("sourceDatabaseId", sourceDatabaseId),
        Expressions.equal("sourceTableId", sourceTableId),
        optionalEquals("destinationClusterId", destinationClusterId),
        optionalEquals("destinationDatabaseId", destinationDatabaseId),
        optionalEquals("destinationTableId", destinationTableId));
  }

  private Expression optionalEquals(String field, String value) {
    return value == null ? Expressions.alwaysTrue() : Expressions.equal(field, value);
  }

  @Override
  public IcebergRow buildIcebergRow(Record record) {
    return ReplicationConfigurationIcebergRow.builder()
        .sourceDatabaseId((String) record.getField("sourceDatabaseId"))
        .sourceTableId((String) record.getField("sourceTableId"))
        .destinationClusterId((String) record.getField("destinationClusterId"))
        .destinationDatabaseId((String) record.getField("destinationDatabaseId"))
        .destinationTableId((String) record.getField("destinationTableId"))
        .replicationInterval((String) record.getField("replicationInterval"))
        .version((String) record.getField("version"))
        .build();
  }
}
