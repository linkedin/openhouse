package com.linkedin.openhouse.hts.catalog.mock.model;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

@Builder
public class TestIcebergRowPrimaryKey implements IcebergRowPrimaryKey {

  String stringId;

  Integer intId;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "stringId", Types.StringType.get()),
        Types.NestedField.required(2, "intId", Types.IntegerType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord genericRecord = GenericRecord.create(getSchema());
    genericRecord.setField("stringId", stringId);
    genericRecord.setField("intId", intId);
    return genericRecord;
  }

  @Override
  public Expression getSearchExpression() {
    return Expressions.and(
        Expressions.equal("stringId", stringId), Expressions.equal("intId", intId));
  }

  @Override
  public IcebergRow buildIcebergRow(Record record) {
    GenericRecord complexType2GR = (GenericRecord) record.getField("complexType2");
    TestIcebergRow.NestedStruct complexType2 = null;
    if (complexType2GR != null) {
      complexType2 =
          TestIcebergRow.NestedStruct.builder()
              .key1((String) ((GenericRecord) record.getField("complexType2")).getField("key1"))
              .key2((Long) ((GenericRecord) record.getField("complexType2")).getField("key2"))
              .build();
    }
    return TestIcebergRow.builder()
        .stringId((String) record.getField("stringId"))
        .intId((Integer) record.getField("intId"))
        .data((String) record.getField("data"))
        .version((String) record.getField("version"))
        .complexType1((List<Long>) record.getField("complexType1"))
        .complexType2(complexType2)
        .complexType3((Map<Integer, String>) record.getField("complexType3"))
        .build();
  }
}
