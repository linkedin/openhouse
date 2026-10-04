package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.model.ReplicationConfigurationRow;
import com.linkedin.openhouse.housetables.model.ReplicationConfigurationRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import java.util.List;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface ReplicationConfigurationHtsJdbcRepository
    extends HtsRepository<ReplicationConfigurationRow, ReplicationConfigurationRowPrimaryKey> {
  List<ReplicationConfigurationRow> findAllBySourceDatabaseIdIgnoreCaseAndSourceTableIdIgnoreCase(
      String sourceDatabaseId, String sourceTableId);

  @Transactional
  @Modifying
  @Query(
      "DELETE FROM ReplicationConfigurationRow r WHERE "
          + "upper(r.sourceDatabaseId) = upper(:sourceDatabaseId) AND "
          + "upper(r.sourceTableId) = upper(:sourceTableId)")
  int deleteAllBySource(
      @Param("sourceDatabaseId") String sourceDatabaseId,
      @Param("sourceTableId") String sourceTableId);
}
