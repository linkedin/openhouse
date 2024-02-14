package com.linkedin.openhouse.tables.dto.mapper.iceberg;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.*;

import com.linkedin.openhouse.tables.common.TableType;
import java.util.Map;
import org.apache.iceberg.Table;
import org.mapstruct.Mapper;
import org.mapstruct.Named;

/**
 * Mapper to convert tableType from {@link org.apache.iceberg.Table} properties to TableType ENUM
 */
@Mapper(componentModel = "spring")
public class TableTypeMapper {

  @Named("toTableType")
  public TableType toTableType(Table table) {
    Map<String, String> properties = table.properties();
    if (properties.containsKey(getCanonicalFieldName("tableType"))) {
      return TableType.valueOf(properties.get(getCanonicalFieldName("tableType")));
    } else {
      return null;
    }
  }
}
