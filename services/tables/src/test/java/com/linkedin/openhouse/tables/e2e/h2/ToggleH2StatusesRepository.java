package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.cluster.configs.TblPropsToggleRegistry;
import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import java.util.Optional;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

/**
 * The {@link org.springframework.context.annotation.Bean} injected into /tables e2e tests when
 * communication to the implementation of {@link ToggleStatusesRepository} is not needed. With
 * {@link Primary} annotation, this repository will be the default injection.
 */
@Repository
@Primary
public interface ToggleH2StatusesRepository extends ToggleStatusesRepository {
  String FEATURE_ACTIVATION_TABLE_NAME = "acv_tbl";
  TblPropsToggleRegistry registry = new TblPropsToggleRegistry();

  /**
   * A trick to activate a specific feature for a specific table-id. This is only for testing
   * purpose.
   */
  @Override
  default Optional<TableToggleStatus> findById(ToggleStatusKey toggleStatusKey) {
    registry.initializeKeys();

    if (toggleStatusKey.getTableId().equals(FEATURE_ACTIVATION_TABLE_NAME)
        && registry.isFeatureRegistered(toggleStatusKey.getFeatureId())) {
      return Optional.of(
          TableToggleStatus.builder()
              .tableId(toggleStatusKey.getTableId())
              .databaseId(toggleStatusKey.getDatabaseId())
              .featureId(toggleStatusKey.getFeatureId())
              .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
              .build());
    } else {
      return Optional.empty();
    }
  }
}
