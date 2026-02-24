package com.linkedin.openhouse.tables.model;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

/** Internal model for table data containing the first N rows of an Iceberg table */
@Builder(toBuilder = true)
@Value
public class TableData {
  String tableId;
  String databaseId;
  String schema;
  List<Map<String, Object>> rows;
  Integer totalRowsFetched;
  Boolean hasMore;
}
