package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTablePrimaryKey;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.context.ApplicationContext;
import org.testcontainers.containers.MySQLContainer;

/** Seeds historical rows and toggle rules; ordinary table mutations use the selected repository. */
public class TableE2eFixtures {
  private final MySQLContainer<?> mysql;

  private final ApplicationContext context;

  TableE2eFixtures(MySQLContainer<?> mysql, ApplicationContext context) {
    this.mysql = mysql;
    this.context = context;
  }

  public boolean usesDocker() {
    return mysql != null;
  }

  public void seedToggle(TableToggleStatus status) {
    if (!usesDocker()) {
      context.getBean(ToggleH2StatusesRepository.class).save(status);
      return;
    }

    execute(
        "INSERT INTO table_toggle_rule (feature,database_pattern,table_pattern) VALUES (?,?,?)",
        status.getFeatureId(),
        status.getDatabaseId(),
        status.getTableId());
  }

  public void deleteToggle(TableToggleStatus status) {
    if (!usesDocker()) {
      context.getBean(ToggleH2StatusesRepository.class).delete(status);
      return;
    }
    execute(
        "DELETE FROM table_toggle_rule WHERE feature=? AND database_pattern=? AND table_pattern=?",
        status.getFeatureId(),
        status.getDatabaseId(),
        status.getTableId());
  }

  public void assertBackendWiring() {
    if (usesDocker()) {
      org.junit.jupiter.api.Assertions.assertTrue(
          context.getBeansOfType(HouseTablesH2Repository.class).isEmpty(),
          "Docker mode must not register the H2 HouseTable repository");
      org.junit.jupiter.api.Assertions.assertTrue(
          context.getBeansOfType(ToggleH2StatusesRepository.class).isEmpty(),
          "Docker mode must query feature toggles through HTS too");
      org.junit.jupiter.api.Assertions.assertInstanceOf(
          com.linkedin.openhouse.internal.catalog.repository.HouseTableRepositoryImpl.class,
          context.getBean(
              com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository.class));
    } else {
      org.junit.jupiter.api.Assertions.assertNotNull(
          context.getBean(HouseTablesH2Repository.class));
    }
  }

  public void setEntityType(HouseTable table, String entityType) {
    if (!usesDocker()) {
      context
          .getBean(HouseTablesH2Repository.class)
          .save(table.toBuilder().entityType(entityType).build());
      return;
    }
    execute(
        "UPDATE user_table_row SET entity_type=? WHERE database_id=? AND table_id=?",
        entityType,
        table.getDatabaseId(),
        table.getTableId());
  }

  public String storedEntityType(HouseTablePrimaryKey key) {
    if (!usesDocker()) {
      return context
          .getBean(HouseTablesH2Repository.class)
          .findByDatabaseIdAndTableId(key.getDatabaseId(), key.getTableId())
          .get()
          .getEntityType();
    }
    try (Connection connection = mysql.createConnection("");
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT entity_type FROM user_table_row WHERE database_id=? AND table_id=?")) {
      statement.setString(1, key.getDatabaseId());
      statement.setString(2, key.getTableId());
      try (ResultSet rows = statement.executeQuery()) {
        if (!rows.next()) {
          throw new IllegalStateException("Missing fixture row");
        }
        return rows.getString(1);
      }
    } catch (SQLException exception) {
      throw new IllegalStateException(exception);
    }
  }

  public void seedSoftDeleted(SoftDeletedTablePrimaryKey key, HouseTable table) {
    if (!usesDocker()) {
      HouseTablesH2Repository.softDeletedTables.put(key, table);
      return;
    }
    execute(
        "INSERT INTO soft_deleted_user_table_row "
            + "(database_id,table_id,deleted_at_ms,version,metadata_location,storage_type,"
            + "creation_time,purge_after_ms) VALUES (?,?,?,?,?,?,?,?)",
        key.getDatabaseId(),
        key.getTableId(),
        key.getDeletedAtMs(),
        1L,
        table.getTableLocation(),
        table.getStorageType() == null ? "hdfs" : table.getStorageType(),
        table.getCreationTime(),
        table.getPurgeAfterMs());
  }

  private void execute(String sql, Object... values) {
    try (Connection connection = mysql.createConnection("");
        PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int index = 0; index < values.length; index++) {
        statement.setObject(index + 1, values[index]);
      }
      statement.executeUpdate();
    } catch (SQLException exception) {
      throw new IllegalStateException("Cannot seed historical HTS fixture", exception);
    }
  }
}
