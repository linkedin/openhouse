package com.linkedin.openhouse.housetables.model;

import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import lombok.Getter;

public final class TestHouseTableModelConstants {
  private TestHouseTableModelConstants() {
    // Utility class, constructor does nothing
  }

  // constants for straightforward tests where there's singleton instance of a user table object
  // needed.
  private static final TestTuple TUPLE_0 = new TestTuple(0);
  public static final String TEST_TABLE_ID = TUPLE_0.getTableId();
  public static final String TEST_DB_ID = TUPLE_0.getDatabaseId();
  public static final String TEST_TBL_VERSION = TUPLE_0.getVer();
  public static final String TEST_TBL_META_LOC = TUPLE_0.getTableLoc();

  public static final String TEST_DEFAULT_STORAGE_TYPE = "hdfs";

  public static final long TEST_CREATION_TIME = 123;

  public static final UserTableDto TEST_USER_TABLE_DTO = TUPLE_0.get_userTableDto();
  public static final UserTable TEST_USER_TABLE = TUPLE_0.get_userTable();

  // Model constants used for testing relatively more complicated cases.
  // table1, db0
  public static final TestTuple TEST_TUPLE_1_0 = new TestHouseTableModelConstants.TestTuple(1);
  // table2, db0
  public static final TestTuple TEST_TUPLE_2_0 = new TestHouseTableModelConstants.TestTuple(2);
  // table3, db0
  public static final TestTuple TEST_TUPLE_3_0 = new TestHouseTableModelConstants.TestTuple(3);
  // table4, db0
  public static final TestTuple TEST_TUPLE_4_0 = new TestHouseTableModelConstants.TestTuple(4);
  // table1, db1
  public static final TestTuple TEST_TUPLE_1_1 = new TestHouseTableModelConstants.TestTuple(1, 1);
  // table2, db1
  public static final TestTuple TEST_TUPLE_2_1 = new TestHouseTableModelConstants.TestTuple(2, 1);
  // table3, db1
  public static final TestTuple TEST_TUPLE_3_1 = new TestHouseTableModelConstants.TestTuple(3, 1);
  // table1, db2
  public static final TestTuple TEST_TUPLE_1_2 = new TestHouseTableModelConstants.TestTuple(1, 2);
  // table2, db2
  public static final TestTuple TEST_TUPLE_2_2 = new TestHouseTableModelConstants.TestTuple(2, 2);

  @Getter
  public static class TestTuple {
    private static final String LOC_TEMPLATE =
        "/openhouse/$test_db/$test_table/$version_metadata.json";
    private final UserTable _userTable;
    private final UserTableRow _userTableRow;
    private final UserTableDto _userTableDto;
    private final String ver;
    private final String tableId;
    private final String databaseId;
    private final String tableLoc;
    private final String storageType;
    private final long creationTime;

    public TestTuple(int tbSeq) {
      this(tbSeq, 0);
    }

    public TestTuple(int tbSeq, int dbSeq) {
      this.tableId = "test_table" + tbSeq;
      this.databaseId = "test_db" + dbSeq;
      this.ver = ValidatorConstants.INITIAL_TABLE_VERSION;
      this.tableLoc =
          LOC_TEMPLATE
              .replace("$test_db", databaseId)
              .replace("$test_table", tableId)
              .replace("$version", "v0");
      this.storageType = TEST_DEFAULT_STORAGE_TYPE;
      this.creationTime = TEST_CREATION_TIME;
      this._userTable =
          UserTable.builder()
              .tableId(tableId)
              .databaseId(databaseId)
              .tableVersion(ver)
              .metadataLocation(tableLoc)
              .storageType(storageType)
              .creationTime(TEST_CREATION_TIME)
              .build();

      this._userTableDto =
          UserTableDto.builder()
              .tableId(tableId)
              .databaseId(databaseId)
              .tableVersion(ver)
              .metadataLocation(tableLoc)
              .storageType(storageType)
              .creationTime(TEST_CREATION_TIME)
              .build();

      this._userTableRow =
          UserTableRow.builder()
              .tableId(tableId)
              .databaseId(databaseId)
              .version(null)
              .metadataLocation(tableLoc)
              .storageType(storageType)
              .creationTime(TEST_CREATION_TIME)
              .build();
    }
  }
}
