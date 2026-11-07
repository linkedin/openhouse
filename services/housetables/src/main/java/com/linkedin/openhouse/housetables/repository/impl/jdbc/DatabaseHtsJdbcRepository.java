package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.model.DatabaseRow;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DatabaseHtsJdbcRepository extends JpaRepository<DatabaseRow, String> {
  Optional<DatabaseRow> findByDatabaseNameKey(String databaseNameKey);
}
