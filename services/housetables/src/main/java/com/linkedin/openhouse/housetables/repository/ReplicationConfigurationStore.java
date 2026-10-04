package com.linkedin.openhouse.housetables.repository;

import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfigurationSet;
import java.util.Optional;

public interface ReplicationConfigurationStore {
  Optional<ReplicationConfigurationSet> findBySource(String sourceDatabaseId, String sourceTableId);

  void replace(ReplicationConfigurationSet replicationConfigurationSet);
}
