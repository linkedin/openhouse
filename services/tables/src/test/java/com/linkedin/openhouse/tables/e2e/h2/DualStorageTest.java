package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.buildGetTableResponseBodyWithDbTbl;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildTableDto;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.mock.properties.CustomClusterPropertiesInitializer;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
public class DualStorageTest {

  @Autowired HouseTableRepository houseTablesRepository;

  @SpyBean @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired StorageManager storageManager;

  @Autowired Catalog catalog;

  @Autowired TablesMapper tablesMapper;

  @Test
  public void testCreateDropTableDualStorage() {

    // Test create table
    // db.table should be created on hdfs storage
    TableDto hdfsTableDto = buildTableDto(buildGetTableResponseBodyWithDbTbl("db", "table"));
    openHouseInternalRepository.save(hdfsTableDto);
    TableDtoPrimaryKey hdfsDtoPrimaryKey = tablesMapper.toTableDtoPrimaryKey(hdfsTableDto);
    Assertions.assertTrue(openHouseInternalRepository.existsById(hdfsDtoPrimaryKey));
    HouseTablePrimaryKey hdfsHtsPrimaryKey =
        HouseTablePrimaryKey.builder()
            .databaseId(hdfsDtoPrimaryKey.getDatabaseId())
            .tableId(hdfsDtoPrimaryKey.getTableId())
            .build();
    Assertions.assertTrue(houseTablesRepository.existsById(hdfsHtsPrimaryKey));
    HouseTable houseTable = houseTablesRepository.findById(hdfsHtsPrimaryKey).get();
    // storage type hdfs
    Assertions.assertEquals(StorageType.HDFS.getValue(), houseTable.getStorageType());

    // local_db.table should be created on local storage
    TableDto localTableDto = buildTableDto(buildGetTableResponseBodyWithDbTbl("local_db", "table"));
    openHouseInternalRepository.save(localTableDto);
    TableDtoPrimaryKey localDtoPrimaryKey = tablesMapper.toTableDtoPrimaryKey(localTableDto);
    Assertions.assertTrue(openHouseInternalRepository.existsById(localDtoPrimaryKey));
    HouseTablePrimaryKey localHtsPrimaryKey =
        HouseTablePrimaryKey.builder()
            .databaseId(localDtoPrimaryKey.getDatabaseId())
            .tableId(localDtoPrimaryKey.getTableId())
            .build();
    Assertions.assertTrue(houseTablesRepository.existsById(localHtsPrimaryKey));
    houseTable = houseTablesRepository.findById(localHtsPrimaryKey).get();
    // storage type local
    Assertions.assertEquals(StorageType.LOCAL.getValue(), houseTable.getStorageType());

    // Test Drop Table
    openHouseInternalRepository.deleteById(hdfsDtoPrimaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(hdfsDtoPrimaryKey));
    openHouseInternalRepository.deleteById(localDtoPrimaryKey);
    Assertions.assertFalse(openHouseInternalRepository.existsById(localDtoPrimaryKey));
  }

  @AfterAll
  static void unsetSysProp() {
    System.clearProperty("OPENHOUSE_CLUSTER_CONFIG_PATH");
  }
}
