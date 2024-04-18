package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.model.TableToggleRule;
import com.linkedin.openhouse.housetables.repository.HtsRepository;

public interface ToggleStatusHtsJdbcRepository extends HtsRepository<TableToggleRule, Long> {
  Iterable<TableToggleRule> findAllByFeature(String feature);
}
