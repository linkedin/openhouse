package com.linkedin.openhouse.tables.mock.service;

import static com.linkedin.openhouse.tables.mock.RequestConstants.*;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapperImpl;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.services.IcebergSnapshotsService;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.data.util.Pair;

@SpringBootTest
public class IcebergSnapshotsServiceTest {

  private static final String TEST_TABLE_CREATOR = "test_user";

  @Autowired private ApplicationContext applicationContext;

  @Autowired private IcebergSnapshotsService service;

  @Autowired private TablesMapper tablesMapper;

  @MockBean private TableUUIDGenerator tableUUIDGenerator;

  private OpenHouseInternalRepository mockRepository;

  @Captor ArgumentCaptor<TableDto> tableDtoArgumentCaptor;

  @BeforeEach
  void setup() {
    mockRepository = applicationContext.getBean(OpenHouseInternalRepository.class);
  }

  @Test
  public void testRepositoryMockWired() {
    Assertions.assertEquals(
        Mockito.mock(OpenHouseInternalRepository.class).getClass(),
        applicationContext.getBean(OpenHouseInternalRepository.class).getClass());
  }

  @Test
  public void testTablesMapperImplWired() {
    Assertions.assertEquals(
        TablesMapperImpl.class, applicationContext.getBean(TablesMapper.class).getClass());
  }

  @Test
  public void testTableCreated() {
    final IcebergSnapshotsRequestBody requestBody =
        TEST_ICEBERG_SNAPSHOTS_INITIAL_VERSION_REQUEST_BODY;
    final String dbId = requestBody.getCreateUpdateTableRequestBody().getDatabaseId();
    final String tableId = requestBody.getCreateUpdateTableRequestBody().getTableId();
    final TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder().databaseId(dbId).tableId(tableId).build();
    final TableDto tableDto = TableDto.builder().databaseId(dbId).tableId(tableId).build();

    Mockito.when(tableUUIDGenerator.generateUUID(Mockito.any(IcebergSnapshotsRequestBody.class)))
        .thenReturn(UUID.randomUUID());
    Mockito.when(mockRepository.findById(key)).thenReturn(Optional.empty());
    Mockito.when(mockRepository.save(tableDtoArgumentCaptor.capture())).thenReturn(tableDto);

    Pair<TableDto, Boolean> result =
        service.putIcebergSnapshots(dbId, tableId, requestBody, TEST_TABLE_CREATOR);
    Assertions.assertEquals(tableDto, result.getFirst(), "Returned DTO must be the mock value");
    Assertions.assertTrue(result.getSecond(), "Table must be created");

    verifyCalls(key, TEST_TABLE_CREATOR, requestBody.getCreateUpdateTableRequestBody());
  }

  @Test
  public void testPutTableExceptionHandling() {
    final IcebergSnapshotsRequestBody requestBody =
        TEST_ICEBERG_SNAPSHOTS_INITIAL_VERSION_REQUEST_BODY;
    final String dbId = requestBody.getCreateUpdateTableRequestBody().getDatabaseId();
    final String tableId = requestBody.getCreateUpdateTableRequestBody().getTableId();
    final TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder().databaseId(dbId).tableId(tableId).build();

    // Mocking exception and ensure it is propogated to the right layer
    Mockito.when(tableUUIDGenerator.generateUUID(Mockito.any(IcebergSnapshotsRequestBody.class)))
        .thenThrow(RequestValidationFailureException.class);

    Mockito.when(mockRepository.findById(key)).thenReturn(Optional.empty());
    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () -> service.putIcebergSnapshots(dbId, tableId, requestBody, TEST_TABLE_CREATOR));

    // Mocking Concurrency failure
    Mockito.when(tableUUIDGenerator.generateUUID(Mockito.any(IcebergSnapshotsRequestBody.class)))
        .thenReturn(UUID.randomUUID());
    Mockito.when(mockRepository.save(Mockito.any(TableDto.class)))
        .thenThrow(CommitFailedException.class);

    Assertions.assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.putIcebergSnapshots(dbId, tableId, requestBody, TEST_TABLE_CREATOR));
  }

  @Test
  public void testPutTableSnapshotsValidationExceptionHandling() {
    final IcebergSnapshotsRequestBody requestBody =
        TEST_ICEBERG_SNAPSHOTS_INITIAL_VERSION_REQUEST_BODY;
    final String dbId = requestBody.getCreateUpdateTableRequestBody().getDatabaseId();
    final String tableId = requestBody.getCreateUpdateTableRequestBody().getTableId();
    final TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder().databaseId(dbId).tableId(tableId).build();

    Mockito.when(mockRepository.findById(key)).thenReturn(Optional.empty());
    Mockito.when(tableUUIDGenerator.generateUUID(Mockito.any(IcebergSnapshotsRequestBody.class)))
        .thenReturn(UUID.randomUUID());
    Mockito.when(mockRepository.save(Mockito.any(TableDto.class)))
        .thenThrow(BadRequestException.class);

    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () -> service.putIcebergSnapshots(dbId, tableId, requestBody, TEST_TABLE_CREATOR));
  }

  @Test
  public void testTableUpdated() {
    final IcebergSnapshotsRequestBody requestBody = TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY;
    final String dbId = requestBody.getCreateUpdateTableRequestBody().getDatabaseId();
    final String tableId = requestBody.getCreateUpdateTableRequestBody().getTableId();
    final TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder().databaseId(dbId).tableId(tableId).build();
    final TableDto tableDto =
        tablesMapper.toTableDto(
            TableDto.builder()
                .clusterId(requestBody.getCreateUpdateTableRequestBody().getClusterId())
                .databaseId(dbId)
                .tableId(tableId)
                .tableLocation(requestBody.getBaseTableVersion())
                .tableCreator(TEST_TABLE_CREATOR)
                .build(),
            requestBody);
    Mockito.when(tableUUIDGenerator.generateUUID(Mockito.any(IcebergSnapshotsRequestBody.class)))
        .thenReturn(UUID.randomUUID());
    Mockito.when(mockRepository.findById(key)).thenReturn(Optional.of(tableDto));
    Mockito.when(mockRepository.save(tableDtoArgumentCaptor.capture())).thenReturn(tableDto);

    Pair<TableDto, Boolean> result = service.putIcebergSnapshots(dbId, tableId, requestBody, null);
    Assertions.assertEquals(tableDto, result.getFirst(), "Returned DTO must be the mock value");
    Assertions.assertFalse(result.getSecond(), "Table must be found in repository");

    verifyCalls(key, TEST_TABLE_CREATOR, requestBody.getCreateUpdateTableRequestBody());
  }

  @Test
  public void testTableUpdatedForLockedTableThrowsException() {
    final IcebergSnapshotsRequestBody requestBody = TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY_FOR_LOCKED;
    final String dbId = requestBody.getCreateUpdateTableRequestBody().getDatabaseId();
    final String tableId = requestBody.getCreateUpdateTableRequestBody().getTableId();
    final TableDtoPrimaryKey key =
        TableDtoPrimaryKey.builder().databaseId(dbId).tableId(tableId).build();
    final TableDto tableDto =
        tablesMapper.toTableDto(
            TableDto.builder()
                .clusterId(requestBody.getCreateUpdateTableRequestBody().getClusterId())
                .databaseId(dbId)
                .tableId(tableId)
                .tableLocation(requestBody.getBaseTableVersion())
                .policies(
                    Policies.builder().lockState(LockState.builder().locked(true).build()).build())
                .tableCreator(TEST_TABLE_CREATOR)
                .build(),
            requestBody);
    Mockito.when(tableUUIDGenerator.generateUUID(Mockito.any(IcebergSnapshotsRequestBody.class)))
        .thenReturn(UUID.randomUUID());
    Mockito.when(mockRepository.findById(key)).thenReturn(Optional.of(tableDto));
    Mockito.when(mockRepository.save(tableDtoArgumentCaptor.capture())).thenReturn(tableDto);

    Assertions.assertThrows(
        UnsupportedClientOperationException.class,
        () -> service.putIcebergSnapshots(dbId, tableId, requestBody, null));
  }

  private void verifyCalls(
      TableDtoPrimaryKey expectedKey,
      String expectedTableCreator,
      CreateUpdateTableRequestBody expectedRequestBody) {
    Mockito.verify(mockRepository, Mockito.times(1)).findById(Mockito.eq(expectedKey));
    Mockito.verify(mockRepository, Mockito.times(1)).save(tableDtoArgumentCaptor.capture());

    TableDto tableDto = tableDtoArgumentCaptor.getValue();
    Assertions.assertEquals(expectedRequestBody.getDatabaseId(), tableDto.getDatabaseId());
    Assertions.assertEquals(expectedRequestBody.getTableId(), tableDto.getTableId());
    Assertions.assertEquals(expectedRequestBody.getClusterId(), tableDto.getClusterId());
    Assertions.assertEquals(expectedTableCreator, tableDto.getTableCreator());
    Assertions.assertEquals(expectedRequestBody.getSchema(), tableDto.getSchema());
    Assertions.assertEquals(expectedRequestBody.getPolicies(), tableDto.getPolicies());
    Assertions.assertEquals(
        expectedRequestBody.getTableProperties(), tableDto.getTableProperties());
  }
}
