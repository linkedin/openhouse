package com.linkedin.openhouse.hts.catalog.mock.data;

import static com.linkedin.openhouse.hts.catalog.model.HtsCatalogConstants.*;
import static com.linkedin.openhouse.hts.catalog.model.HtsCatalogConstants.Helpers.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.hts.catalog.data.GenericIcebergRowReadersWriters;
import com.linkedin.openhouse.hts.catalog.mock.model.TestIcebergRow;
import com.linkedin.openhouse.hts.catalog.mock.model.TestIcebergRowPrimaryKey;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GenericIcebergRowReadersWritersTest {

  private static final GenericIcebergRowReadersWriters<TestIcebergRow, TestIcebergRowPrimaryKey>
      GENERIC_READERS_WRITERS = new GenericIcebergRowReadersWriters();
  private static Table testTable;

  @BeforeAll
  static void setup() {
    testTable =
        TEST_CATALOG.createTable(
            TableIdentifier.of("test", GenericIcebergRowReadersWritersTest.class.getSimpleName()),
            TEST_ICEBERG_ROW.getSchema(),
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));
    testTable.newAppend().commit();
  }

  @Test
  void testCreate() {
    TestIcebergRow row = ir("testCreate", 1, "data1");
    TestIcebergRow putted = GENERIC_READERS_WRITERS.put(testTable, row);

    Assertions.assertEquals(row.getNextVersion(), putted.getCurrentVersion());
    Assertions.assertNotEquals(row.getCurrentVersion(), putted.getCurrentVersion());

    Assertions.assertTrue(row.getSchema().sameSchema(putted.getSchema()));

    Assertions.assertTrue(isRecordEqualWithVersionIgnored(row, putted));

    GENERIC_READERS_WRITERS.delete(testTable, irpk("testCreate", 1));
  }

  @Test
  void testUpdate() {
    TestIcebergRow row1 = ir("testUpdate", 1, "data1");
    TestIcebergRow puttedRow1 = GENERIC_READERS_WRITERS.put(testTable, row1);

    // Test update without specifying correct version
    TestIcebergRow rowFail = ir("testUpdate", 1, "random", "data2");
    Assertions.assertThrows(
        CommitFailedException.class, () -> GENERIC_READERS_WRITERS.put(testTable, rowFail));

    // Test update with correct version
    TestIcebergRow rowSuccess = ir("testUpdate", 1, puttedRow1.getCurrentVersion(), "data3");
    TestIcebergRow puttedRowSuccess =
        Assertions.assertDoesNotThrow(() -> GENERIC_READERS_WRITERS.put(testTable, rowSuccess));

    Assertions.assertTrue(isRecordEqualWithVersionIgnored(rowSuccess, puttedRowSuccess));

    GENERIC_READERS_WRITERS.delete(testTable, irpk("testUpdate", 1));
  }

  @Test
  void testGet() {
    // setup
    GENERIC_READERS_WRITERS.put(testTable, ir("testGet", 1, "data1"));
    GENERIC_READERS_WRITERS.put(testTable, ir("testGet", 2, "data2"));

    // test that we can get accurate data
    TestIcebergRow getRow = GENERIC_READERS_WRITERS.get(testTable, irpk("testGet", 1)).get();
    Assertions.assertEquals("data1", getRow.getData());

    TestIcebergRow getRow2 = GENERIC_READERS_WRITERS.get(testTable, irpk("testGet", 2)).get();
    Assertions.assertEquals("data2", getRow2.getData());

    Optional<TestIcebergRow> getRow3 = GENERIC_READERS_WRITERS.get(testTable, irpk("testGet", -1));
    Assertions.assertFalse(getRow3.isPresent());

    // cleanup
    GENERIC_READERS_WRITERS.delete(testTable, irpk("testGet", 1));
    GENERIC_READERS_WRITERS.delete(testTable, irpk("testGet", 2));
  }

  @Test
  void testDrop() {
    // setup
    GENERIC_READERS_WRITERS.put(testTable, ir("testDrop", 1, "data1"));

    // test that we can get accurate data
    GENERIC_READERS_WRITERS.delete(testTable, irpk("testDrop", 1));

    Assertions.assertFalse(GENERIC_READERS_WRITERS.get(testTable, irpk("testDrop", 1)).isPresent());

    Assertions.assertThrows(
        NotFoundException.class,
        () -> GENERIC_READERS_WRITERS.delete(testTable, irpk("testDrop", 1)));
  }

  @Test
  void testSearchByPartialId() {
    // setup
    GENERIC_READERS_WRITERS.put(testTable, ir("testSearch", 1, "data1"));
    GENERIC_READERS_WRITERS.put(testTable, ir("testSearch", 2, "data2"));
    GENERIC_READERS_WRITERS.put(testTable, ir("testSearch2", 1, "data3"));

    List<TestIcebergRow> rows =
        Lists.newArrayList(
            GENERIC_READERS_WRITERS
                .searchByPartialId(testTable, irpk("testSearch", null))
                .iterator());
    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals(
        rows.stream().map(TestIcebergRow::getData).sorted().collect(Collectors.toList()),
        Arrays.asList("data1", "data2"));

    // test that we can get accurate data
    GENERIC_READERS_WRITERS.delete(testTable, irpk("testSearch", 1));
    GENERIC_READERS_WRITERS.delete(testTable, irpk("testSearch", 2));
    GENERIC_READERS_WRITERS.delete(testTable, irpk("testSearch2", 1));
  }

  @Test
  void testPutComplexType() {
    // setup
    List<Long> complexType1 = ImmutableList.of(1L);
    TestIcebergRow.NestedStruct complexType2 =
        TestIcebergRow.NestedStruct.builder().key1("key1").key2(2L).build();
    ImmutableMap<Integer, String> complexType3 = ImmutableMap.of(1, "value1");
    GENERIC_READERS_WRITERS.put(
        testTable,
        ir("testPutComplexType", 1, "v1", "data1", complexType1, complexType2, complexType3));

    TestIcebergRow getRow =
        GENERIC_READERS_WRITERS.get(testTable, irpk("testPutComplexType", 1)).get();

    Assertions.assertEquals(getRow.getComplexType1(), complexType1);
    Assertions.assertEquals(getRow.getComplexType2(), complexType2);
    Assertions.assertEquals(getRow.getComplexType3(), complexType3);

    // test that we can get accurate data
    GENERIC_READERS_WRITERS.delete(testTable, irpk("testPutComplexType", 1));
  }

  @AfterAll
  static void tearDown() {
    TEST_CATALOG.dropTable(
        TableIdentifier.of("test", GenericIcebergRowReadersWritersTest.class.getSimpleName()));
  }
}
