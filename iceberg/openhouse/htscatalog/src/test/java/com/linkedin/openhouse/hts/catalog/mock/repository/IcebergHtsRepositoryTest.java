package com.linkedin.openhouse.hts.catalog.mock.repository;

import static com.linkedin.openhouse.hts.catalog.model.HtsCatalogConstants.*;
import static com.linkedin.openhouse.hts.catalog.model.HtsCatalogConstants.Helpers.*;

import com.linkedin.openhouse.hts.catalog.mock.model.TestIcebergRow;
import com.linkedin.openhouse.hts.catalog.mock.model.TestIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.repository.IcebergHtsRepository;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IcebergHtsRepositoryTest {

  private static IcebergHtsRepository<TestIcebergRow, TestIcebergRowPrimaryKey> testRepository;

  @BeforeAll
  static void setup() {
    testRepository =
        IcebergHtsRepository.<TestIcebergRow, TestIcebergRowPrimaryKey>builder()
            .htsTableIdentifier(TableIdentifier.of("hts_db", "tables"))
            .catalog(TEST_CATALOG)
            .build();
  }

  @Test
  void testSave() {
    TestIcebergRow ir1 = ir("db1", 1, "data1");
    TestIcebergRow savedIr1 = testRepository.save(ir1);
    Assertions.assertTrue(isRecordEqualWithVersionIgnored(ir1, savedIr1));

    TestIcebergRow irFail = ir("db1", 1, "random", "data2");
    Assertions.assertThrows(RuntimeException.class, () -> testRepository.save(irFail));

    TestIcebergRow ir2 = ir("db1", 1, savedIr1.getCurrentVersion(), "data2");
    TestIcebergRow savedIr2 = Assertions.assertDoesNotThrow(() -> testRepository.save(ir2));
    Assertions.assertTrue(isRecordEqualWithVersionIgnored(ir2, savedIr2));
    Assertions.assertNotEquals(savedIr2.getCurrentVersion(), savedIr1.getCurrentVersion());

    testRepository.delete(savedIr2);
  }

  @Test
  void testFindById() {
    testRepository.save(ir("db1", 1, "data1"));
    testRepository.save(ir("db1", 2, "data2"));
    testRepository.save(ir("db2", 1, "data21"));

    Assertions.assertEquals(testRepository.findById(irpk("db1", 1)).get().getData(), "data1");
    Assertions.assertEquals(testRepository.findById(irpk("db1", 2)).get().getData(), "data2");
    Assertions.assertEquals(testRepository.findById(irpk("db2", 1)).get().getData(), "data21");
    Assertions.assertFalse(testRepository.findById(irpk("dbNotExist", 1)).isPresent());

    testRepository.deleteById(irpk("db1", 1));
    testRepository.deleteById(irpk("db1", 2));
    testRepository.deleteById(irpk("db2", 1));
  }

  @Test
  void testExistsById() {
    testRepository.save(ir("db1", 1, "data1"));
    Assertions.assertTrue(testRepository.existsById(irpk("db1", 1)));
    Assertions.assertFalse(testRepository.existsById(irpk("db2", 1)));
    testRepository.deleteById(irpk("db1", 1));
  }

  @Test
  void testSearchByPartialId() {
    testRepository.save(ir("db1", 1, "data1"));
    testRepository.save(ir("db1", 2, "data2"));
    testRepository.save(ir("db2", 1, "data21"));

    List<TestIcebergRow> rows =
        Lists.newArrayList(testRepository.searchByPartialId(irpk("db1", null)).iterator());
    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals(
        rows.stream().map(TestIcebergRow::getData).sorted().collect(Collectors.toList()),
        Arrays.asList("data1", "data2"));

    testRepository.deleteById(irpk("db1", 1));
    testRepository.deleteById(irpk("db1", 2));
    testRepository.deleteById(irpk("db2", 1));
  }

  @Test
  void testReplaceByPartialIdAtomicallyReplacesOnlyMatchingRows() {
    testRepository.save(ir("db1", 1, "data1"));
    TestIcebergRow saved2 = testRepository.save(ir("db1", 2, "data2"));
    TestIcebergRow savedOtherDatabase = testRepository.save(ir("db2", 1, "other"));

    TestIcebergRow updated2 = ir("db1", 2, saved2.getCurrentVersion(), "updated2");
    TestIcebergRow added3 = ir("db1", 3, "added3");
    testRepository.replaceByPartialId(irpk("db1", null), Arrays.asList(updated2, added3));

    List<TestIcebergRow> database1Rows =
        Lists.newArrayList(testRepository.searchByPartialId(irpk("db1", null)).iterator());
    Assertions.assertEquals(
        Arrays.asList("added3", "updated2"),
        database1Rows.stream().map(TestIcebergRow::getData).sorted().collect(Collectors.toList()));
    Assertions.assertEquals("other", testRepository.findById(irpk("db2", 1)).get().getData());
    Assertions.assertFalse(testRepository.findById(irpk("db1", 1)).isPresent());
    Assertions.assertNotEquals(
        saved2.getCurrentVersion(),
        testRepository.findById(irpk("db1", 2)).get().getCurrentVersion());

    testRepository.deleteById(irpk("db1", 2));
    testRepository.deleteById(irpk("db1", 3));
    testRepository.delete(savedOtherDatabase);
  }

  @AfterAll
  static void tearDown() {
    testRepository.deleteAll();
  }
}
