package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.model.TableStorageLocationRow;
import com.linkedin.openhouse.housetables.model.TableStorageLocationRowPrimaryKey;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * JDBC-backed Spring Data JPA repository for {@link TableStorageLocationRow}. Picked up by the
 * existing JPA scan in DatabaseConfiguration.
 */
public interface TableStorageLocationJdbcRepository
    extends JpaRepository<TableStorageLocationRow, TableStorageLocationRowPrimaryKey> {

  List<TableStorageLocationRow> findByTableUuid(String tableUuid);
}
