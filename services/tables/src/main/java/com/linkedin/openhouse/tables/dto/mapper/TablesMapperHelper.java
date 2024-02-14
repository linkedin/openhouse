package com.linkedin.openhouse.tables.dto.mapper;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.Table;

/**
 * Utilities classes to simplify java code reference in {@link TablesMapper} with {@link
 * org.mapstruct.ap.shaded.freemarker.core.Expression}.
 */
public final class TablesMapperHelper {
  private TablesMapperHelper() {
    // noop for utilities class constructor
  }

  static List<String> mapSnapshots(Table table) {
    return StreamSupport.stream(table.snapshots().spliterator(), false)
        .map(SnapshotParser::toJson)
        .collect(Collectors.toList());
  }
}
