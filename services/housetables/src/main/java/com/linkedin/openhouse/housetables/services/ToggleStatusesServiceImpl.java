package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.common.api.spec.ToggleStatusEnum;
import com.linkedin.openhouse.housetables.api.spec.model.ToggleStatus;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.ToggleStatusHtsJdbcRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ToggleStatusesServiceImpl implements ToggleStatusesService {
  @Autowired ToggleStatusHtsJdbcRepository htsRepository;

  @Override
  public ToggleStatus getTableToggleStatus(String featureId, String databaseId, String tableId) {
    return ToggleStatus.builder().status(ToggleStatusEnum.ACTIVE).build();
  }
}
