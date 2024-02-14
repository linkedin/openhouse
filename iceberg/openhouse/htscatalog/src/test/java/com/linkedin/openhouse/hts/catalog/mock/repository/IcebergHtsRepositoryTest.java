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

  private static IcebergHtsRepository<TestIcebergRow, TestIcebergRowPrimaryKey> REPOSITORY;

  @BeforeAll
  static void setup() {
    REPOSITORY =
        IcebergHtsRepository.<TestIcebergRow, TestIcebergRowPrimaryKey>builder()
            .htsTableIdentifier(TableIdentifier.of("hts_db", "tables"))
            .catalog(TEST_CATALOG)
            .build();
  }

  @Test
  void testSave() {
    TestIcebergRow ir1 = ir("db1", 1, "data1");
    TestIcebergRow savedIr1 = REPOSITORY.save(ir1);
    Assertions.assertTrue(isRecordEqualWithVersionIgnored(ir1, savedIr1));

    TestIcebergRow irFail = ir("db1", 1, "random", "data2");
    Assertions.assertThrows(RuntimeException.class, () -> REPOSITORY.save(irFail));

    TestIcebergRow ir2 = ir("db1", 1, savedIr1.getCurrentVersion(), "data2");
    TestIcebergRow savedIr2 = Assertions.assertDoesNotThrow(() -> REPOSITORY.save(ir2));
    Assertions.assertTrue(isRecordEqualWithVersionIgnored(ir2, savedIr2));
    Assertions.assertNotEquals(savedIr2.getCurrentVersion(), savedIr1.getCurrentVersion());

    REPOSITORY.delete(savedIr2);
  }

  @Test
  void testFindById() {
    REPOSITORY.save(ir("db1", 1, "data1"));
    REPOSITORY.save(ir("db1", 2, "data2"));
    REPOSITORY.save(ir("db2", 1, "data21"));

    Assertions.assertEquals(REPOSITORY.findById(irpk("db1", 1)).get().getData(), "data1");
    Assertions.assertEquals(REPOSITORY.findById(irpk("db1", 2)).get().getData(), "data2");
    Assertions.assertEquals(REPOSITORY.findById(irpk("db2", 1)).get().getData(), "data21");
    Assertions.assertFalse(REPOSITORY.findById(irpk("dbNotExist", 1)).isPresent());

    REPOSITORY.deleteById(irpk("db1", 1));
    REPOSITORY.deleteById(irpk("db1", 2));
    REPOSITORY.deleteById(irpk("db2", 1));
  }

  @Test
  void testExistsById() {
    REPOSITORY.save(ir("db1", 1, "data1"));
    Assertions.assertTrue(REPOSITORY.existsById(irpk("db1", 1)));
    Assertions.assertFalse(REPOSITORY.existsById(irpk("db2", 1)));
    REPOSITORY.deleteById(irpk("db1", 1));
  }

  @Test
  void testSearchByPartialId() {
    REPOSITORY.save(ir("db1", 1, "data1"));
    REPOSITORY.save(ir("db1", 2, "data2"));
    REPOSITORY.save(ir("db2", 1, "data21"));

    List<TestIcebergRow> rows =
        Lists.newArrayList(REPOSITORY.searchByPartialId(irpk("db1", null)).iterator());
    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals(
        rows.stream().map(TestIcebergRow::getData).sorted().collect(Collectors.toList()),
        Arrays.asList("data1", "data2"));

    REPOSITORY.deleteById(irpk("db1", 1));
    REPOSITORY.deleteById(irpk("db1", 2));
    REPOSITORY.deleteById(irpk("db2", 1));
  }

  @AfterAll
  static void tearDown() {
    REPOSITORY.deleteAll();
  }
}
