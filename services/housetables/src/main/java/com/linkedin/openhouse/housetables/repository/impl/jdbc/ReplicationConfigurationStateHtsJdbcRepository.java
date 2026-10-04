package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.model.ReplicationConfigurationStateRow;
import com.linkedin.openhouse.housetables.model.ReplicationConfigurationStateRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import java.util.Optional;

public interface ReplicationConfigurationStateHtsJdbcRepository
    extends HtsRepository<
        ReplicationConfigurationStateRow, ReplicationConfigurationStateRowPrimaryKey> {
  Optional<ReplicationConfigurationStateRow>
      findBySourceDatabaseIdIgnoreCaseAndSourceTableIdIgnoreCase(
          String sourceDatabaseId, String sourceTableId);
}
