package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.api.spec.model.ToggleStatus;
import com.linkedin.openhouse.housetables.api.spec.model.ToggleStatusEnum;
import com.linkedin.openhouse.housetables.model.TableToggleRule;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.ToggleStatusHtsJdbcRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ToggleStatusesServiceImpl implements ToggleStatusesService {
  @Autowired ToggleStatusHtsJdbcRepository htsRepository;
  @Autowired TableToggleRuleMatcher ruleMatcher;

  @Override
  public ToggleStatus getTableToggleStatus(String featureId, String databaseId, String tableId) {
    for (TableToggleRule tableToggleRule : htsRepository.findAllByFeature(featureId)) {

      if (ruleMatcher.matches(tableToggleRule, tableId, databaseId)) {
        return ToggleStatus.builder().status(ToggleStatusEnum.ACTIVE).build();
      }
    }

    return ToggleStatus.builder().status(ToggleStatusEnum.INACTIVE).build();
  }
}
