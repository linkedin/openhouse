package com.linkedin.openhouse.hts.catalog.model.usertable;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import lombok.Builder;
import lombok.Getter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;

/**
 * Iceberg backed implementation for UserTableRow.
 *
 * <p>This class helps in serializing the object into datafiles rows.
 */
@Builder
@Getter
public class UserTableIcebergRow implements IcebergRow {

  private String tableId;

  private String databaseId;

  private String version;

  private String metadataLocation;

  @Override
  public Schema getSchema() {
    return new Schema(
        Types.NestedField.required(1, "databaseId", Types.StringType.get()),
        Types.NestedField.required(2, "tableId", Types.StringType.get()),
        Types.NestedField.required(3, "version", Types.StringType.get()),
        Types.NestedField.required(4, "metadataLocation", Types.StringType.get()));
  }

  @Override
  public GenericRecord getRecord() {
    GenericRecord genericRecord = GenericRecord.create(getSchema());
    genericRecord.setField("databaseId", databaseId);
    genericRecord.setField("tableId", tableId);
    genericRecord.setField("version", version);
    genericRecord.setField("metadataLocation", metadataLocation);
    return genericRecord;
  }

  @Override
  public String getVersionColumnName() {
    return "version";
  }

  @Override
  public IcebergRowPrimaryKey getIcebergRowPrimaryKey() {
    return UserTableIcebergRowPrimaryKey.builder().tableId(tableId).databaseId(databaseId).build();
  }
}
