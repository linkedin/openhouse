package com.linkedin.openhouse.housetables.e2e.fixture;

import com.linkedin.openhouse.housetables.model.UserTableRow;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Plants rows whose {@code entity_type} column is SQL NULL, which only rows predating the
 * discriminator may be. JPA cannot express them any more: {@code EntityTypeConverter} rejects a
 * null write so that the legacy population the {@code IS NULL} predicate arm carries cannot grow.
 */
@Component
public class UserTableRawSeeder {

  private static final String INSERT =
      "INSERT INTO user_table_row "
          + "(database_id, table_id, version, metadata_location, storage_type, creation_time, entity_type) "
          + "VALUES (?, ?, ?, ?, ?, ?, NULL)";

  /** Matches the version {@code @Version} stamps on an insert, so rows are indistinguishable. */
  private static final long INSERTED_VERSION = 0L;

  @Autowired private DataSource dataSource;

  public void seedLegacyRow(UserTableRow row) {
    new JdbcTemplate(dataSource)
        .update(
            INSERT,
            row.getDatabaseId(),
            row.getTableId(),
            INSERTED_VERSION,
            row.getMetadataLocation(),
            row.getStorageType(),
            row.getCreationTime());
  }
}
