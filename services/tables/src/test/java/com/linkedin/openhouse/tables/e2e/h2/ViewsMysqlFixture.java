package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.housetables.e2e.HtsMysqlDatabase;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Raw MySQL assertions are independent of HTS response mapping. */
final class ViewsMysqlFixture {
  void seedToggle(TableToggleStatus status) throws SQLException {
    toggle(
        "INSERT INTO table_toggle_rule (feature,database_pattern,table_pattern) VALUES (?,?,?)",
        status);
  }

  void deleteToggle(TableToggleStatus status) throws SQLException {
    toggle(
        "DELETE FROM table_toggle_rule WHERE feature=? AND database_pattern=? AND table_pattern=?",
        status);
  }

  private void toggle(String sql, TableToggleStatus status) throws SQLException {
    try (Connection connection = HtsMysqlDatabase.openConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, status.getFeatureId());
      statement.setString(2, status.getDatabaseId());
      statement.setString(3, status.getTableId());
      statement.executeUpdate();
    }
  }

  Optional<Map<String, Object>> persistedEntity(String databaseId, String tableId)
      throws SQLException {
    try (Connection connection = HtsMysqlDatabase.openConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT database_id,table_id,entity_type,metadata_location,table_version,"
                    + "version,storage_type,creation_time,ETL_TS FROM user_table_row "
                    + "WHERE database_id=? AND table_id=?")) {
      statement.setString(1, databaseId);
      statement.setString(2, tableId);
      try (ResultSet rows = statement.executeQuery()) {
        if (!rows.next()) {
          return Optional.empty();
        }
        Map<String, Object> row = new LinkedHashMap<>();
        for (int column = 1; column <= rows.getMetaData().getColumnCount(); column++) {
          row.put(rows.getMetaData().getColumnLabel(column), rows.getObject(column));
        }
        return Optional.of(row);
      }
    }
  }
}
