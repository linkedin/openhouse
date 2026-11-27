package com.linkedin.openhouse.tables.toggle;

import com.linkedin.openhouse.common.exception.DependencyUnavailableException;
import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

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
   * @throws DependencyUnavailableException if HouseTables is unreachable or fails (5xx). It answers
   *     every lookup, defaulting to inactive, so a failed call is never a toggle state. A 4xx means
   *     this call is wrong, a bug, so it propagates as itself.
   */
  @Override
  public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
    ToggleStatusKey toggleStatusKey =
        ToggleStatusKey.builder()
            .databaseId(databaseId)
            .tableId(tableId)
            .featureId(featureId)
            .build();
    Optional<TableToggleStatus> toggleStatus;
    try {
      toggleStatus = toggleStatusesRepository.findById(toggleStatusKey);
    } catch (WebClientRequestException e) {
      throw unavailable(toggleStatusKey, e);
    } catch (WebClientResponseException e) {
      if (HttpStatus.Series.resolve(e.getRawStatusCode()) != HttpStatus.Series.SERVER_ERROR) {
        throw e;
      }
      throw unavailable(toggleStatusKey, e);
    }

    return toggleStatus.isPresent()
        && toggleStatus
            .get()
            .getToggleStatusEnum()
            .toString()
            .equalsIgnoreCase(ToggleStatus.StatusEnum.ACTIVE.toString());
  }

  private static DependencyUnavailableException unavailable(
      ToggleStatusKey toggleStatusKey, Exception cause) {
    return new DependencyUnavailableException(
        String.format(
            "HouseTables could not report feature %s for table %s.%s. Retry.",
            toggleStatusKey.getFeatureId(),
            toggleStatusKey.getDatabaseId(),
            toggleStatusKey.getTableId()),
        cause);
  }
}
