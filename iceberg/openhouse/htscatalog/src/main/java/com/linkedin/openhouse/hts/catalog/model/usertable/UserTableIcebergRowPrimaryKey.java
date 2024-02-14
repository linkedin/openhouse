package com.linkedin.openhouse.hts.catalog.model.usertable;

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
public class UserTableIcebergRowPrimaryKey implements IcebergRowPrimaryKey {

  private String tableId;

  private String databaseId;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "databaseId", Types.StringType.get()),
        Types.NestedField.required(2, "tableId", Types.StringType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord genericRecord = GenericRecord.create(getSchema());
    genericRecord.setField("databaseId", databaseId);
    genericRecord.setField("tableId", tableId);
    return genericRecord;
  }

  @Override
  public Expression getSearchExpression() {
    return Expressions.and(
        Expressions.equal("databaseId", databaseId), Expressions.equal("tableId", tableId));
  }

  @Override
  public IcebergRow buildIcebergRow(Record record) {
    return UserTableIcebergRow.builder()
        .databaseId((String) record.getField("databaseId"))
        .tableId((String) record.getField("tableId"))
        .version((String) record.getField("version"))
        .metadataLocation((String) record.getField("metadataLocation"))
        .build();
  }
}
