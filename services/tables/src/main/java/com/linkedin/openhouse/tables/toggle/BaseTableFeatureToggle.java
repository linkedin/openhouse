package com.linkedin.openhouse.tables.toggle;

import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BaseTableFeatureToggle implements TableFeatureToggle {
  @Autowired ToggleStatusesRepository toggleStatusesRepository;

  /**
   * A naive implementation that omits the necessity of rule engine and assumes every {@link
   * TableToggleStatus} is written explicitly without specifying patterns.
   *
   * @param databaseId databaseId
   * @param tableId tableId
   * @return True if the provided pair of databaseId and tableId has the feature "dummy" activated.
   */
  @Override
  public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
    ToggleStatusKey toggleStatusKey =
        ToggleStatusKey.builder()
            .databaseId(databaseId)
            .tableId(tableId)
            .featureId(featureId)
            .build();
    Optional<TableToggleStatus> toggleStatus = toggleStatusesRepository.findById(toggleStatusKey);
    // TODO: Change this once HTS PR is in
    return toggleStatus.isPresent()
        && toggleStatus.get().getToggleStatusEnum().equalsIgnoreCase("active");
  }
}
