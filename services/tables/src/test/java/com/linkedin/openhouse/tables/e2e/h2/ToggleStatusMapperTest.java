package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.internal.catalog.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.internal.catalog.toggle.model.ToggleStatusKey;
import com.linkedin.openhouse.internal.catalog.toggle.model.ToggleStatusMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ToggleStatusMapperTest {

  @Autowired private ToggleStatusMapper mapper;

  @Test
  void toTableToggleStatus() {
    ToggleStatusKey key =
        ToggleStatusKey.builder()
            .tableId("table1")
            .databaseId("database1")
            .featureId("feature1")
            .build();
    ToggleStatus toggleStatus = new ToggleStatus().status(ToggleStatus.StatusEnum.ACTIVE);

    TableToggleStatus result = mapper.toTableToggleStatus(key, toggleStatus);

    assertNotNull(result);
    assertEquals("feature1", result.getFeatureId());
    assertEquals("table1", result.getTableId());
    assertEquals("database1", result.getDatabaseId());
    assertEquals(ToggleStatus.StatusEnum.ACTIVE, result.getToggleStatusEnum());
  }

  @Test
  void testToTableToggleStatusWithInactiveStatus() {
    ToggleStatusKey key =
        ToggleStatusKey.builder()
            .featureId("feature2")
            .tableId("table2")
            .databaseId("database2")
            .build();
    ToggleStatus toggleStatus = new ToggleStatus().status(ToggleStatus.StatusEnum.INACTIVE);

    TableToggleStatus result = mapper.toTableToggleStatus(key, toggleStatus);

    assertNotNull(result);
    assertEquals("feature2", result.getFeatureId());
    assertEquals("table2", result.getTableId());
    assertEquals("database2", result.getDatabaseId());
    assertEquals(ToggleStatus.StatusEnum.INACTIVE, result.getToggleStatusEnum());
  }

  @Test
  void testToTableToggleStatusWithNullStatus() {
    ToggleStatusKey key =
        ToggleStatusKey.builder()
            .featureId("feature3")
            .tableId("table3")
            .databaseId("database3")
            .build();
    ToggleStatus toggleStatus = new ToggleStatus().status(null);

    TableToggleStatus result = mapper.toTableToggleStatus(key, toggleStatus);

    assertNotNull(result);
    assertEquals("feature3", result.getFeatureId());
    assertEquals("table3", result.getTableId());
    assertEquals("database3", result.getDatabaseId());
    assertNull(result.getToggleStatusEnum());
  }
}
