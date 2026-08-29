package com.linkedin.openhouse.housetables.model;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.request.CreateUpdateEntityRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import java.util.Collections;

public final class TestHtsApiConstants {
  private TestHtsApiConstants() {
    // Do nothing in utilities class's constructor
  }

  // DB/Table name that should never exist.
  // If accidentally creating table/db names like these, it leads to test failures.
  public static final String NON_EXISTED_TABLE = "non_existed_table";
  public static final String NON_EXISTED_DB = "non_existed_db";

  /** Distinct identifiers so a route wired to the wrong handler method is visible, not silent. */
  public static final String TEST_VIEW_ID = "test_view0";

  public static final String TEST_NEUTRAL_ID = "test_neutral0";

  /**
   * Copy of {@link
   * com.linkedin.openhouse.common.exception.NoSuchUserTableException.ERROR_MSG_TEMPLATE}
   */
  public static final String NOT_FOUND_ERROR_MSG_TEMPLATE = "User table $db.$tbl cannot be found";

  /**
   * Copy of {@link com.linkedin.openhouse.common.exception.NoSuchEntityException}'s template, which
   * the neutral and view point reads use rather than the table-specific one above.
   */
  public static final String NO_SUCH_ENTITY_ERROR_MSG_TEMPLATE = "$ent $id cannot be found";

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

  /** The view mirror of the table response body, used by the view point read and view PUT. */
  public static final EntityResponseBody<UserTable> TEST_GET_USER_VIEW_RESPONSE_BODY =
      EntityResponseBody.<UserTable>builder()
          .entity(
              UserTable.builder()
                  .tableId(TEST_VIEW_ID)
                  .databaseId(TEST_DB_ID)
                  .tableVersion(TEST_TBL_VERSION)
                  .metadataLocation(TEST_TBL_META_LOC)
                  .storageType(TEST_DEFAULT_STORAGE_TYPE)
                  .entityType("VIEW")
                  .build())
          .build();

  /**
   * The neutral read always names a canonical, non-null type. Its identifier differs from both
   * typed fixtures so a route wired to the table or view handler method fails the body assertion
   * rather than passing by coincidence.
   */
  public static final EntityResponseBody<UserTable> TEST_GET_NEUTRAL_ENTITY_RESPONSE_BODY =
      EntityResponseBody.<UserTable>builder()
          .entity(
              UserTable.builder()
                  .tableId(TEST_NEUTRAL_ID)
                  .databaseId(TEST_DB_ID)
                  .tableVersion(TEST_TBL_VERSION)
                  .metadataLocation(TEST_TBL_META_LOC)
                  .storageType(TEST_DEFAULT_STORAGE_TYPE)
                  .entityType("VIEW")
                  .build())
          .build();

  public static final GetAllEntityResponseBody<UserTable> TEST_GET_ALL_USER_VIEWS_RESPONSE_BODY =
      GetAllEntityResponseBody.<UserTable>builder()
          .results(Collections.singletonList(TEST_GET_USER_VIEW_RESPONSE_BODY.getEntity()))
          .build();

  // A request to put a user table
  public static final CreateUpdateEntityRequestBody<UserTable> PUT_USER_TABLE_REQUEST_BODY =
      CreateUpdateEntityRequestBody.<UserTable>builder().entity(TEST_USER_TABLE).build();

  /** A view PUT payload that omits the discriminator, as a rolling old client would send it. */
  public static final CreateUpdateEntityRequestBody<UserTable> PUT_USER_VIEW_REQUEST_BODY =
      CreateUpdateEntityRequestBody.<UserTable>builder()
          .entity(TEST_USER_TABLE.toBuilder().tableId(TEST_VIEW_ID).entityType(null).build())
          .build();
}
