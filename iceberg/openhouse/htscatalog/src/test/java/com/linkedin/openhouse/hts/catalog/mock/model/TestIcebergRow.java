package com.linkedin.openhouse.hts.catalog.mock.model;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;

@Builder(toBuilder = true)
@Getter
public class TestIcebergRow implements IcebergRow {

  String stringId;

  Integer intId;

  String version;

  String data;

  List<Long> complexType1;

  NestedStruct complexType2;

  Map<Integer, String> complexType3;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "stringId", Types.StringType.get()),
        Types.NestedField.required(2, "intId", Types.IntegerType.get()),
        Types.NestedField.required(3, "version", Types.StringType.get()),
        Types.NestedField.required(4, "data", Types.StringType.get()),
        Types.NestedField.optional(
            5, "complexType1", Types.ListType.ofRequired(6, Types.LongType.get())),
        Types.NestedField.optional(
            7,
            "complexType2",
            Types.StructType.of(
                Types.NestedField.optional(8, "key1", Types.StringType.get()),
                Types.NestedField.optional(9, "key2", Types.LongType.get()))),
        Types.NestedField.optional(
            10,
            "complexType3",
            Types.MapType.ofRequired(11, 12, Types.IntegerType.get(), Types.StringType.get())));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord genericRecord = GenericRecord.create(getSchema());
    genericRecord.setField("stringId", stringId);
    genericRecord.setField("intId", intId);
    genericRecord.setField("version", version);
    genericRecord.setField("data", data);
    genericRecord.setField("complexType1", complexType1);
    genericRecord.setField("complexType3", complexType3);
    if (complexType2 != null) {
      GenericRecord complexType2GR =
          GenericRecord.create(
              (Types.StructType)
                  getSchema().select("complexType2").asStruct().field("complexType2").type());
      complexType2GR.setField("key1", complexType2.getKey1());
      complexType2GR.setField("key2", complexType2.getKey2());
      genericRecord.setField("complexType2", complexType2GR);
    }
    return genericRecord;
  }

  @Override
  public String getVersionColumnName() {
    return "version";
  }

  @Override
  public IcebergRowPrimaryKey getIcebergRowPrimaryKey() {
    return TestIcebergRowPrimaryKey.builder().stringId(stringId).intId(intId).build();
  }

  @EqualsAndHashCode
  @Builder
  @Getter
  public static class NestedStruct {
    String key1;
    Long key2;
  }
}
