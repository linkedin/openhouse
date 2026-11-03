package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.model.TableModelConstants.TABLE_DTO;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.tables.config.TblPropsToggleRegistry;
import com.linkedin.openhouse.tables.config.TblPropsToggleRegistryBaseImpl;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import java.util.Collections;
import javax.annotation.PostConstruct;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
@Import(CreateTableFeatureToggleTest.FeatureToggleTestConfig.class)
public class CreateTableFeatureToggleTest {
  private static final String TEST_PROPERTY = "openhouse.testFeatureProperty";
  private static final String TEST_VALUE = "enabled";
  private static final String TABLE_ID = "create_feature_toggle_test";

  @Autowired private OpenHouseInternalRepository openHouseInternalRepository;
  @Autowired private ToggleStatusesRepository toggleStatusesRepository;

  @Test
  void testFeatureToggleAllowsPreservedPropertyDuringCreation() {
    TableDto tableDto =
        TABLE_DTO
            .toBuilder()
            .tableId(TABLE_ID)
            .tableVersion(INITIAL_TABLE_VERSION)
            .tableProperties(Collections.singletonMap(TEST_PROPERTY, TEST_VALUE))
            .build();
    TableDtoPrimaryKey primaryKey =
        TableDtoPrimaryKey.builder()
            .databaseId(tableDto.getDatabaseId())
            .tableId(tableDto.getTableId())
            .build();
    TableToggleStatus toggleStatus =
        TableToggleStatus.builder()
            .featureId(TblPropsToggleRegistryBaseImpl.ENABLE_TBLTYPE)
            .databaseId(tableDto.getDatabaseId())
            .tableId(tableDto.getTableId())
            .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
            .build();

    try {
      toggleStatusesRepository.save(toggleStatus);
      TableDto createdTable = openHouseInternalRepository.save(tableDto);

      assertEquals(TEST_VALUE, createdTable.getTableProperties().get(TEST_PROPERTY));
    } finally {
      if (openHouseInternalRepository.existsById(primaryKey)) {
        openHouseInternalRepository.deleteById(primaryKey);
      }
      toggleStatusesRepository.delete(toggleStatus);
    }
  }

  @TestConfiguration
  static class FeatureToggleTestConfig {
    @Bean
    @Primary
    TblPropsToggleRegistry testTblPropsToggleRegistry() {
      return new TestTblPropsToggleRegistry();
    }
  }

  static class TestTblPropsToggleRegistry extends TblPropsToggleRegistryBaseImpl {
    @PostConstruct
    @Override
    public void initializeKeys() {
      super.initializeKeys();
      featureKeys.put(TEST_PROPERTY, ENABLE_TBLTYPE);
    }
  }
}
