package com.linkedin.openhouse.hts.catalog.model.replication;

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

/** Composite replication-edge key; null destination fields support source-edge lookup. */
@Builder
@Getter
public class ReplicationStateIcebergRowPrimaryKey implements IcebergRowPrimaryKey {
  private String sourceClusterId;
  private String sourceTableUUID;
  private Long sourceCreationTime;
  private String destinationClusterId;
  private String destinationTableUUID;
  private Long destinationCreationTime;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "sourceClusterId", Types.StringType.get()),
        Types.NestedField.required(2, "sourceTableUUID", Types.StringType.get()),
        Types.NestedField.required(3, "sourceCreationTime", Types.LongType.get()),
        Types.NestedField.required(4, "destinationClusterId", Types.StringType.get()),
        Types.NestedField.required(5, "destinationTableUUID", Types.StringType.get()),
        Types.NestedField.required(6, "destinationCreationTime", Types.LongType.get()));
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
    return record;
  }

  @Override
  public Expression getSearchExpression() {
    return Expressions.and(
        Expressions.and(
            Expressions.equal("sourceClusterId", sourceClusterId),
            Expressions.equal("sourceTableUUID", sourceTableUUID),
            Expressions.equal("sourceCreationTime", sourceCreationTime)),
        Expressions.and(
            Expressions.equal("destinationClusterId", destinationClusterId),
            Expressions.equal("destinationTableUUID", destinationTableUUID),
            Expressions.equal("destinationCreationTime", destinationCreationTime)));
  }

  @Override
  public IcebergRow buildIcebergRow(Record record) {
    return ReplicationStateIcebergRow.builder()
        .sourceClusterId((String) record.getField("sourceClusterId"))
        .sourceTableUUID((String) record.getField("sourceTableUUID"))
        .sourceCreationTime((Long) record.getField("sourceCreationTime"))
        .destinationClusterId((String) record.getField("destinationClusterId"))
        .destinationTableUUID((String) record.getField("destinationTableUUID"))
        .destinationCreationTime((Long) record.getField("destinationCreationTime"))
        .payload((String) record.getField("payload"))
        .rowVersion((String) record.getField("rowVersion"))
        .build();
  }
}
