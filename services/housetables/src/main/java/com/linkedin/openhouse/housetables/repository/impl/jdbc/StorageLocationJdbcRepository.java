package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.model.StorageLocationRow;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * JDBC-backed Spring Data JPA repository for {@link StorageLocationRow}. Picked up by the existing
 * JPA scan in DatabaseConfiguration.
 */
public interface StorageLocationJdbcRepository extends JpaRepository<StorageLocationRow, String> {}
