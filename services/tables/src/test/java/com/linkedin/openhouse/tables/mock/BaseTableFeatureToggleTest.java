package com.linkedin.openhouse.tables.mock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.tables.toggle.BaseTableFeatureToggle;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BaseTableFeatureToggleTest {
  @InjectMocks private BaseTableFeatureToggle baseFeatureToggle;

  @Mock private ToggleStatusesRepository toggleStatusesRepository;

  @Test
  public void testDummyToggle() {
    TableToggleStatus rule1 =
        TableToggleStatus.builder()
            .featureId("dummy")
            .databaseId("db1")
            .tableId("tbl1")
            .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
            .build();

    TableToggleStatus rule2 =
        TableToggleStatus.builder()
            .featureId("dummy")
            .databaseId("db2")
            .tableId("tbl2")
            .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
            .build();

    TableToggleStatus rule3 =
        TableToggleStatus.builder()
            .featureId("random")
            .databaseId("db1")
            .tableId("tbl3")
            .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
            .build();

    when(toggleStatusesRepository.findById(
            ToggleStatusKey.builder().databaseId("db1").tableId("tbl1").featureId("dummy").build()))
        .thenReturn(Optional.of(rule1));
    when(toggleStatusesRepository.findById(
            ToggleStatusKey.builder().databaseId("db2").tableId("tbl2").featureId("dummy").build()))
        .thenReturn(Optional.of(rule2));
    when(toggleStatusesRepository.findById(
            ToggleStatusKey.builder()
                .databaseId("db1")
                .tableId("tbl3")
                .featureId("random")
                .build()))
        .thenReturn(Optional.of(rule3));
    when(toggleStatusesRepository.findById(
            ToggleStatusKey.builder().databaseId("db1").tableId("tbl3").featureId("dummy").build()))
        .thenReturn(Optional.empty());

    assertTrue(baseFeatureToggle.isFeatureActivated("db1", "tbl1", "dummy"));
    assertTrue(baseFeatureToggle.isFeatureActivated("db2", "tbl2", "dummy"));
    assertTrue(baseFeatureToggle.isFeatureActivated("db1", "tbl3", "random"));
    assertFalse(baseFeatureToggle.isFeatureActivated("db1", "tbl3", "dummy"));

    verify(toggleStatusesRepository, times(1))
        .findById(
            ToggleStatusKey.builder().databaseId("db1").tableId("tbl1").featureId("dummy").build());
    verify(toggleStatusesRepository, times(1))
        .findById(
            ToggleStatusKey.builder().databaseId("db2").tableId("tbl2").featureId("dummy").build());
    verify(toggleStatusesRepository, times(1))
        .findById(
            ToggleStatusKey.builder()
                .databaseId("db1")
                .tableId("tbl3")
                .featureId("random")
                .build());
    verify(toggleStatusesRepository, times(1))
        .findById(
            ToggleStatusKey.builder().databaseId("db1").tableId("tbl3").featureId("dummy").build());
  }
}
