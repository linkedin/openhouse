package com.linkedin.openhouse.tables.mock.service;

import static com.linkedin.openhouse.tables.mock.RequestConstants.*;

import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.services.TablesService;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

@SpringBootTest
public class TablesServiceTest {

  private static final String TEST_TABLE_CREATOR = "test_user";

  @Autowired private ApplicationContext applicationContext;

  @Autowired private TablesService service;

  private OpenHouseInternalRepository mockRepository;

  @BeforeEach
  void setup() {
    mockRepository = applicationContext.getBean(OpenHouseInternalRepository.class);
  }

  @Test
  public void testRetrievingStagedTableThrowsIllegalStateException() {
    final String dbId = TEST_CREATE_TABLE_REQUEST_BODY.getDatabaseId();
    final String tableId = TEST_CREATE_TABLE_REQUEST_BODY.getTableId();
    final TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder().databaseId(dbId).tableId(tableId).build();
    final TableDto tableDto =
        TableDto.builder().databaseId(dbId).tableId(tableId).stageCreate(true).build();
    Mockito.when(mockRepository.findById(key)).thenReturn(Optional.of(tableDto));
    IllegalStateException illegalStateException =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> service.putTable(TEST_CREATE_TABLE_REQUEST_BODY, TEST_TABLE_CREATOR, false));
    Assertions.assertTrue(
        illegalStateException
            .getMessage()
            .contains(String.format("Staged Table %s.%s was illegally persisted", dbId, tableId)));
  }

  @Test
  public void testHouseTableConcurrentUpdateException() {
    final String dbId = TEST_CREATE_TABLE_REQUEST_BODY.getDatabaseId();
    final String tableId = TEST_CREATE_TABLE_REQUEST_BODY.getTableId();
    final TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder().databaseId(dbId).tableId(tableId).build();

    Mockito.when(mockRepository.findById(key)).thenThrow(HouseTableConcurrentUpdateException.class);
    Assertions.assertThrowsExactly(
        HouseTableConcurrentUpdateException.class, () -> service.getTable(dbId, tableId, ""));
  }

  @Test
  public void testHouseTableCallerException() {
    final String dbId = TEST_CREATE_TABLE_REQUEST_BODY.getDatabaseId();
    final String tableId = TEST_CREATE_TABLE_REQUEST_BODY.getTableId();
    final TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder().databaseId(dbId).tableId(tableId).build();

    Mockito.when(mockRepository.findById(key)).thenThrow(HouseTableCallerException.class);
    Assertions.assertThrowsExactly(
        HouseTableCallerException.class, () -> service.getTable(dbId, tableId, ""));
  }
}
