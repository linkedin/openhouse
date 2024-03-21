package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.tables.model.TableModelConstants.TABLE_DTO;
import static org.mockito.Mockito.doReturn;

import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PartitionSpecMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.TableTypeMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.util.ReflectionUtils;

class InternalRepositoryUtilsTest {

  @Test
  void testAlterPropIfNeeded() {
    UpdateProperties mockUpdateProperties = Mockito.mock(UpdateProperties.class);
    doReturn(null).when(mockUpdateProperties).set(Mockito.anyString(), Mockito.anyString());
    doReturn(null).when(mockUpdateProperties).remove(Mockito.anyString());

    Map<String, String> existingTableProps = new HashMap<>();
    Map<String, String> providedTableProps = new HashMap<>();

    // Note that the sequence of following test cases matters.

    // providedTableProps:{}
    // existingTableProps:{}
    Assertions.assertFalse(
        InternalRepositoryUtils.alterPropIfNeeded(
            mockUpdateProperties, existingTableProps, providedTableProps));

    // insert
    // providedTableProps:{a:aa}
    // existingTableProps:{}
    providedTableProps.put("a", "aa");
    Assertions.assertTrue(
        InternalRepositoryUtils.alterPropIfNeeded(
            mockUpdateProperties, existingTableProps, providedTableProps));
    existingTableProps.put("a", "aa");

    // upsert
    // providedTableProps:{a:bb}
    // existingTableProps:{a:aa}
    providedTableProps.put("a", "bb");
    Assertions.assertTrue(
        InternalRepositoryUtils.alterPropIfNeeded(
            mockUpdateProperties, existingTableProps, providedTableProps));
    existingTableProps.put("a", "bb");

    // unset
    // providedTableProps:{}
    // existingTableProps:{a:bb}
    providedTableProps.clear();
    Assertions.assertTrue(
        InternalRepositoryUtils.alterPropIfNeeded(
            mockUpdateProperties, existingTableProps, providedTableProps));
    existingTableProps.remove("a");
  }

  @Test
  void testRemovingHtsField() throws Exception {
    Map<String, String> test = new HashMap<>();
    test.put("openhouse.tableId", "a");
    test.put("openhouse.databaseId", "b");
    test.put("openhouse.clusterId", "c");
    test.put("openhouse.tableUri", "d");
    test.put("openhouse.tableUUID", "e");
    test.put("openhouse.tableLocation", "f");
    test.put("openhouse.tableVersion", "g");
    test.put("openhouse.tableType", "type");
    test.put("policies", "po");
    Map<String, String> result =
        InternalRepositoryUtils.getUserDefinedTblProps(test, new BasePreservedKeyCheckerTest());
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testConvertToTableDto() throws Exception {
    File tmpFile = File.createTempFile("foo", "bar");

    final Schema SCHEMA =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "ts", Types.TimestampType.withZone()));

    Map<String, String> tableProps = new HashMap<>();
    tableProps.put("openhouse.tableId", "a");
    tableProps.put("openhouse.databaseId", "b");
    tableProps.put("openhouse.clusterId", "c");
    tableProps.put("openhouse.tableUri", "d");
    tableProps.put("openhouse.tableUUID", "e");
    tableProps.put("openhouse.tableLocation", tmpFile.getAbsolutePath());
    tableProps.put("openhouse.tableVersion", "g");
    tableProps.put("openhouse.tableCreator", "f");
    tableProps.put("openhouse.lastModifiedTime", "1000");
    tableProps.put("openhouse.creationTime", "1000");
    tableProps.put("openhouse.tableType", TableType.PRIMARY_TABLE.toString());

    Table mockTable = Mockito.mock(Table.class);
    Mockito.doReturn(tableProps).when(mockTable).properties();
    Mockito.doReturn(SCHEMA).when(mockTable).schema();
    FsStorageProvider mockFsStorageProvider = Mockito.mock(FsStorageProvider.class);
    Mockito.doReturn(FileSystem.get(new Configuration()))
        .when(mockFsStorageProvider)
        .storageClient();
    PartitionSpecMapper mockPartitionSpecMapper = Mockito.mock(PartitionSpecMapper.class);
    Mockito.doReturn(null).when(mockPartitionSpecMapper).toPartitionSpec(Mockito.any());
    Mockito.doReturn(TABLE_DTO.getClustering())
        .when(mockPartitionSpecMapper)
        .toClusteringSpec(Mockito.any());
    Mockito.doReturn(TABLE_DTO.getTimePartitioning())
        .when(mockPartitionSpecMapper)
        .toTimePartitionSpec(Mockito.any());
    PoliciesSpecMapper mockPolicyMapper = Mockito.mock(PoliciesSpecMapper.class);
    Mockito.doReturn(TABLE_DTO.getPolicies())
        .when(mockPolicyMapper)
        .toPoliciesObject(Mockito.any());

    TableTypeMapper mockTableTypeMapper = Mockito.mock(TableTypeMapper.class);
    Mockito.doReturn(TableType.PRIMARY_TABLE).when(mockTableTypeMapper).toTableType(Mockito.any());

    TableDto returnDto =
        InternalRepositoryUtils.convertToTableDto(
            mockTable,
            mockFsStorageProvider,
            mockPartitionSpecMapper,
            mockPolicyMapper,
            mockTableTypeMapper);
    Assertions.assertEquals(returnDto.getTableId(), "a");
    Assertions.assertEquals(returnDto.getDatabaseId(), "b");
    Assertions.assertEquals(returnDto.getClusterId(), "c");
    Assertions.assertEquals(returnDto.getTableUri(), "d");
    Assertions.assertEquals(returnDto.getTableUUID(), "e");
    Assertions.assertEquals(
        URI.create(returnDto.getTableLocation()).getPath(), tmpFile.getAbsolutePath());
    Assertions.assertEquals(returnDto.getTableVersion(), "g");
    Assertions.assertEquals(returnDto.getTableCreator(), "f");
    Assertions.assertEquals(returnDto.getCreationTime(), 1000);
    Assertions.assertEquals(returnDto.getLastModifiedTime(), 1000);
    Assertions.assertEquals(returnDto.getTableType(), TableType.valueOf("PRIMARY_TABLE"));

    // All internal fields of TableDTO has to carry non-null value except jsonSnapshots which is
    // intentional.
    // Point of this check is, if something is added into TableDTO but somehow left explicit
    // handling in convertToTableDto
    // this test shall fail.

    Arrays.stream(TableDto.class.getDeclaredFields())
        .filter(
            field ->
                Modifier.isPrivate(field.getModifiers())
                    && !Modifier.isStatic(field.getModifiers())
                    && !field.getName().equals("jsonSnapshots")
                    && !field.getName().equals("snapshotRefs"))
        .map(Field::getName)
        .forEach(name -> ensurePrivateFieldNonNull(name, returnDto));

    tableProps.remove("openhouse.lastModifiedTime");
    tableProps.remove("openhouse.creationTime");
    TableDto returnDto2 =
        InternalRepositoryUtils.convertToTableDto(
            mockTable,
            mockFsStorageProvider,
            mockPartitionSpecMapper,
            mockPolicyMapper,
            mockTableTypeMapper);
    Assertions.assertEquals(returnDto2.getCreationTime(), 0);
    Assertions.assertEquals(returnDto2.getLastModifiedTime(), 0);
  }

  @Test
  void testConstructTablePath() {
    FsStorageProvider fsStorageProvider = Mockito.mock(FsStorageProvider.class);
    String someRoot = "root";
    String dbId = "db";
    String tbId = "tb";
    String uuid = UUID.randomUUID().toString();
    Mockito.when(fsStorageProvider.rootPath()).thenReturn(someRoot);
    Assertions.assertEquals(
        InternalRepositoryUtils.constructTablePath(fsStorageProvider, dbId, tbId, uuid),
        Paths.get(someRoot, dbId, tbId + "-" + uuid));
  }

  private void ensurePrivateFieldNonNull(String fieldName, TableDto tableDto) {
    Field resultField = ReflectionUtils.findField(TableDto.class, fieldName);
    Assertions.assertNotNull(resultField);
    ReflectionUtils.makeAccessible(resultField);
    Assertions.assertNotNull(ReflectionUtils.getField(resultField, tableDto));
  }

  private class BasePreservedKeyCheckerTest extends BasePreservedKeyChecker {
    public BasePreservedKeyCheckerTest() {
      // empty constructor, purely for testing purpose.
    }
  }
}
