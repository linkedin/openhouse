package com.linkedin.openhouse.hts.catalog.mock.data;

import static com.linkedin.openhouse.hts.catalog.model.HtsCatalogConstants.*;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.data.IcebergDataUtils;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IcebergDataUtilsTest {

  private static Table TEST_TABLE;
  private static IcebergRow TEST_ROW;
  private static IcebergRowPrimaryKey TEST_ROW_PK;

  @BeforeAll
  static void setup() {
    TEST_ROW = TEST_ICEBERG_ROW;
    TEST_ROW_PK = TEST_ROW.getIcebergRowPrimaryKey();
    TEST_TABLE =
        TEST_CATALOG.createTable(
            TableIdentifier.of("test", IcebergDataUtilsTest.class.getSimpleName()),
            TEST_ICEBERG_ROW.getSchema(),
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));
  }

  @Test
  public void testCreateRowDeltaDeleteFile() {
    DeleteFile deleteFile = IcebergDataUtils.createRowDeltaDeleteFile(TEST_TABLE, TEST_ROW_PK);
    Assertions.assertEquals(deleteFile.recordCount(), 1);
    Assertions.assertEquals(deleteFile.format(), FileFormat.AVRO);
    Assertions.assertEquals(deleteFile.content(), FileContent.EQUALITY_DELETES);
    Assertions.assertEquals(
        deleteFile.equalityFieldIds(),
        TEST_ROW.getSchema().select("stringId", "intId").columns().stream()
            .map(Types.NestedField::fieldId)
            .collect(Collectors.toList()));
  }

  @Test
  public void testCreateRowDeltaDataFile() {
    Pair<DataFile, IcebergRow> dataFileAndRow =
        IcebergDataUtils.createRowDeltaDataFileWithNextVersion(TEST_TABLE, TEST_ROW);
    DataFile dataFile = dataFileAndRow.first();
    Assertions.assertEquals(dataFile.recordCount(), 1);
    Assertions.assertEquals(dataFile.format(), FileFormat.AVRO);
    Assertions.assertEquals(dataFile.content(), FileContent.DATA);
  }

  @AfterAll
  static void tearDown() {
    TEST_CATALOG.dropTable(TableIdentifier.of("test", IcebergDataUtilsTest.class.getSimpleName()));
  }
}
