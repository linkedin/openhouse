package com.linkedin.openhouse.housetables.model;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.request.CreateUpdateEntityRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;

public final class TestHtsApiConstants {
  private TestHtsApiConstants() {
    // Do nothing in utilities class's constructor
  }

  // DB/Table name that should never exist.
  // If accidentally creating table/db names like these, it leads to test failures.
  public static final String NON_EXISTED_TABLE = "non_existed_table";
  public static final String NON_EXISTED_DB = "non_existed_db";

  /**
   * Copy of {@link
   * com.linkedin.openhouse.common.exception.NoSuchUserTableException.ERROR_MSG_TEMPLATE}
   */
  public static final String NOT_FOUND_ERROR_MSG_TEMPLATE = "User table $db.$tbl cannot be found";

  public static final EntityResponseBody<UserTable> TEST_GET_USER_TABLE_RESPONSE_BODY =
      EntityResponseBody.<UserTable>builder()
          .entity(
              UserTable.builder()
                  .tableId(TEST_TABLE_ID)
                  .databaseId(TEST_DB_ID)
                  .tableVersion(TEST_TBL_VERSION)
                  .metadataLocation(TEST_TBL_META_LOC)
                  .storageType(TEST_DEFAULT_STORAGE_TYPE)
                  .build())
          .build();

  // A request to put a user table
  public static final CreateUpdateEntityRequestBody<UserTable> PUT_USER_TABLE_REQUEST_BODY =
      CreateUpdateEntityRequestBody.<UserTable>builder().entity(TEST_USER_TABLE).build();
}
