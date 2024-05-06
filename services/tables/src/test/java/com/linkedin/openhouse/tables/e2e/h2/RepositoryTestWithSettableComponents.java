package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.model.TableModelConstants.TABLE_DTO;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalTableOperations;
import com.linkedin.openhouse.internal.catalog.SnapshotInspector;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.repository.impl.SettableInternalRepositoryForTest;
import com.linkedin.openhouse.tables.settable.SettableCatalogForTest;
import com.linkedin.openhouse.tables.settable.SettableTestConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
@Import(SettableTestConfig.class)
public class RepositoryTestWithSettableComponents {
  @SpyBean @Autowired HouseTableRepository houseTablesRepository;

  @SpyBean @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired FsStorageProvider fsStorageProvider;

  @Autowired Catalog catalog;

  @Autowired
  @Qualifier("LegacyFileIO")
  FileIO fileIO;

  @Autowired SnapshotInspector snapshotInspector;

  @Autowired HouseTableMapper houseTableMapper;

  @Autowired MeterRegistry meterRegistry;

  /**
   * mocking the behavior of HouseTableRepository to throw exception for triggering retry when
   * needed.
   */
  private HouseTableRepository provideFailedHtsRepoWhenSave() {
    HouseTableRepository htsRepo = Mockito.mock(HouseTableRepository.class);
    doThrow(CommitFailedException.class).when(htsRepo).save(Mockito.any(HouseTable.class));
    HouseTable dummyHt =
        HouseTable.builder()
            .tableId(TABLE_DTO.getTableId())
            .databaseId(TABLE_DTO.getDatabaseId())
            .build();
    doReturn(Optional.of(dummyHt)).when(htsRepo).findById(Mockito.any(HouseTablePrimaryKey.class));
    return htsRepo;
  }

  @Test()
  void testNoRetryInternalRepo() {
    TableIdentifier tableIdentifier =
        TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId());
    HouseTableRepository htsRepo = provideFailedHtsRepoWhenSave();

    // construct a real table object to prepare subsequent client call for table-update (that they
    // will fail)
    OpenHouseInternalTableOperations actualOps =
        new OpenHouseInternalTableOperations(
            houseTablesRepository,
            fileIO,
            snapshotInspector,
            houseTableMapper,
            tableIdentifier,
            new MetricsReporter(this.meterRegistry, "test", Lists.newArrayList()));
    ((SettableCatalogForTest) catalog).setOperation(actualOps);
    TableDto creationDTO = TABLE_DTO.toBuilder().tableVersion(INITIAL_TABLE_VERSION).build();
    creationDTO = openHouseInternalRepository.save(creationDTO);
    // obtain the realTable object for mocking behavior later.
    Table realTable = catalog.loadTable(tableIdentifier);

    // injecting mocked htsRepo within a tableOperation that fails doCommit method.
    // The requirement to trigger htsRepo.save call are: Detectable updates in Transaction itself.
    OpenHouseInternalTableOperations mockOps =
        new OpenHouseInternalTableOperations(
            htsRepo,
            fileIO,
            snapshotInspector,
            houseTableMapper,
            tableIdentifier,
            new MetricsReporter(this.meterRegistry, "test", Lists.newArrayList()));
    OpenHouseInternalTableOperations spyOperations = Mockito.spy(mockOps);
    doReturn(actualOps.current()).when(spyOperations).refresh();
    BaseTable spyOptsMockedTable = Mockito.spy(new BaseTable(spyOperations, realTable.name()));

    Catalog spyCatalog = Mockito.spy(catalog);
    Mockito.doReturn(spyOptsMockedTable).when(spyCatalog).loadTable(tableIdentifier);
    ((SettableInternalRepositoryForTest) openHouseInternalRepository).setCatalog(spyCatalog);
    // the following spy object avoid execution path enter table-creation branch that doesn't
    // contain retry logic.
    OpenHouseInternalRepository spyRepo = Mockito.spy(openHouseInternalRepository);
    Mockito.doReturn(true)
        .when(spyRepo)
        .existsById(
            TableDtoPrimaryKey.builder()
                .tableId(TABLE_DTO.getTableId())
                .databaseId(TABLE_DTO.getDatabaseId())
                .build());

    // step IV execute the table update in internal repository level.
    Map<String, String> props = new HashMap<>(creationDTO.getTableProperties());
    props.put("test", "test");
    creationDTO =
        creationDTO
            .toBuilder()
            .tableVersion(creationDTO.getTableLocation()) /*for sake of versionCheck*/
            .tableProperties(props)
            .build();
    // The following call to update the existing table, as creationDTO contains updated table
    // properties.
    // as htsRepo has been configured to return a retryable exception (CommitFailedException)
    // the following code which serves as a client of Iceberg Catalog would have entered default
    // retry loop
    // declared in org.apache.iceberg.BaseTransaction.commitSimpleTransaction which is default to 4
    // times,
    // if not explicitly disabled.
    try {
      spyRepo.save(creationDTO);
    } catch (CommitFailedException e) {
      verify(htsRepo, times(1)).save(Mockito.any(HouseTable.class));
      ((SettableCatalogForTest) catalog).setOperation(actualOps);
      catalog.dropTable(tableIdentifier);
      return;
    }

    // If there's no exception thrown or another other non-CommitFailedException exception, it is a
    // failed case.
    assert false;
  }

  private HouseTableRepository provideFailedHtsRepoWhenGet(Class c) {
    HouseTableRepository htsRepo = Mockito.mock(HouseTableRepository.class);
    doThrow(c)
        .when(htsRepo)
        .findById(
            HouseTablePrimaryKey.builder()
                .tableId(TABLE_DTO.getTableId())
                .databaseId(TABLE_DTO.getDatabaseId())
                .build());
    return htsRepo;
  }

  @Test()
  void testFailedHtsRepoWhenGet() {
    TableIdentifier tableIdentifier =
        TableIdentifier.of(TABLE_DTO.getDatabaseId(), TABLE_DTO.getTableId());

    HashSet<Class> exs = new HashSet<>();
    exs.add(HouseTableConcurrentUpdateException.class);
    exs.add(HouseTableCallerException.class);

    for (Class c : exs) {
      HouseTableRepository htsRepo = provideFailedHtsRepoWhenGet(c);
      OpenHouseInternalTableOperations mockOps =
          new OpenHouseInternalTableOperations(
              htsRepo,
              fileIO,
              snapshotInspector,
              houseTableMapper,
              tableIdentifier,
              new MetricsReporter(this.meterRegistry, "test", Lists.newArrayList()));
      OpenHouseInternalTableOperations spyOperations = Mockito.spy(mockOps);
      BaseTable spyOptsMockedTable =
          Mockito.spy(
              new BaseTable(
                  spyOperations,
                  "SettableCatelogForTest."
                      + TABLE_DTO.getDatabaseId()
                      + "."
                      + TABLE_DTO.getTableId()));

      Catalog spyCatalog = Mockito.spy(catalog);
      Mockito.doReturn(spyOptsMockedTable).when(spyCatalog).loadTable(tableIdentifier);

      ((SettableInternalRepositoryForTest) openHouseInternalRepository).setCatalog(spyCatalog);
      OpenHouseInternalRepository spyRepo = Mockito.spy(openHouseInternalRepository);
      try {
        spyRepo.findById(
            TableDtoPrimaryKey.builder()
                .tableId(TABLE_DTO.getTableId())
                .databaseId(TABLE_DTO.getDatabaseId())
                .build());
      } catch (Exception e) {
        Assertions.assertEquals(e.getClass(), c);
        continue;
      }
      assert false;
    }
  }
}
