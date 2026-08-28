package com.linkedin.openhouse.housetables.mock.mapper;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.dto.model.UserViewQuery;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class UserTablesMapperTest {
  private static final long TEST_DELETED_AT_MS = 1751907524L;

  private static final long TEST_PURGE_RETENTION_MS = 1000000L;

  @Autowired UserTablesMapper userTablesMapper;

  @Test
  void toUserTableDto() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    UserTableDto dtoAfterMapping = userTablesMapper.toUserTableDto(testUserTableRow);
    // Assert objects are equal ignoring versions
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE_DTO.toBuilder().tableVersion("").build(),
        dtoAfterMapping.toBuilder().tableVersion("").build());
    // Assert After Mapping version is same as the source's metadataLocation
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE_DTO.getMetadataLocation(),
        dtoAfterMapping.getTableVersion());
  }

  @Test
  void toUserTable() {
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE,
        userTablesMapper.toUserTable(TestHouseTableModelConstants.TEST_USER_TABLE_DTO));
  }

  @Test
  void toUserTableRowNullStorageType() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    Assertions.assertEquals(
        testUserTableRow.toBuilder().entityType(EntityType.TABLE).build(),
        userTablesMapper.toUserTableRow(
            TestHouseTableModelConstants.TEST_USER_TABLE
                .toBuilder()
                .storageType(null)
                .entityType(EntityType.TABLE.name())
                .build(),
            Optional.empty()));
  }

  @Test
  void toUserTableRowCustomStorageType() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    Assertions.assertEquals(
        testUserTableRow.toBuilder().storageType("blobfs").entityType(EntityType.TABLE).build(),
        userTablesMapper.toUserTableRow(
            TestHouseTableModelConstants.TEST_USER_TABLE
                .toBuilder()
                .storageType("blobfs")
                .entityType(EntityType.TABLE.name())
                .build(),
            Optional.empty()));
  }

  @Test
  void fromUserTable() {
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE_DTO,
        userTablesMapper.fromUserTable(TestHouseTableModelConstants.TEST_USER_TABLE));
  }

  /**
   * The entityType discriminator must survive every hop of the HTS mapping chain (API -> JPA row ->
   * DTO -> API). A mapper that silently drops it would let a view pointer be persisted as a legacy
   * table row.
   */
  @Test
  void entityTypeRoundTripsAcrossUserTableRowDtoAndApi() {
    UserTable viewUserTable =
        TestHouseTableModelConstants.TEST_USER_TABLE.toBuilder().entityType("VIEW").build();

    UserTableRow row = userTablesMapper.toUserTableRow(viewUserTable, Optional.empty());
    Assertions.assertEquals(EntityType.VIEW, row.getEntityType());

    UserTableDto dto = userTablesMapper.toUserTableDto(row);
    Assertions.assertEquals(EntityType.VIEW, dto.getEntityType());

    UserTable roundTripped = userTablesMapper.toUserTable(dto);
    Assertions.assertEquals("VIEW", roundTripped.getEntityType());

    // Every other field is untouched by the new discriminator.
    Assertions.assertEquals(viewUserTable.getTableId(), roundTripped.getTableId());
    Assertions.assertEquals(viewUserTable.getDatabaseId(), roundTripped.getDatabaseId());
    Assertions.assertEquals(
        viewUserTable.getMetadataLocation(), roundTripped.getMetadataLocation());
    Assertions.assertEquals(viewUserTable.getStorageType(), roundTripped.getStorageType());
    Assertions.assertEquals(viewUserTable.getCreationTime(), roundTripped.getCreationTime());

    // fromUserTable is the other API -> DTO direction and must carry it too.
    Assertions.assertEquals(
        EntityType.VIEW, userTablesMapper.fromUserTable(viewUserTable).getEntityType());
  }

  /**
   * The transport model validates the discriminator case-insensitively, so the enum boundary must
   * resolve every spelling it lets through. The canonical constant name is what reaches storage and
   * the wire.
   */
  @Test
  void entityTypeSpellingsNormalizeToTheCanonicalConstant() {
    Map<String, EntityType> spellings = new HashMap<>();
    spellings.put("VIEW", EntityType.VIEW);
    spellings.put("view", EntityType.VIEW);
    spellings.put("ViEw", EntityType.VIEW);
    spellings.put("TABLE", EntityType.TABLE);
    spellings.put("table", EntityType.TABLE);
    spellings.put("TaBlE", EntityType.TABLE);

    for (Map.Entry<String, EntityType> spelling : spellings.entrySet()) {
      UserTable userTable =
          TestHouseTableModelConstants.TEST_USER_TABLE
              .toBuilder()
              .entityType(spelling.getKey())
              .build();

      UserTableRow row = userTablesMapper.toUserTableRow(userTable, Optional.empty());
      Assertions.assertEquals(spelling.getValue(), row.getEntityType(), spelling.getKey());
      Assertions.assertEquals(
          spelling.getValue().name(),
          userTablesMapper.toUserTable(userTablesMapper.toUserTableDto(row)).getEntityType(),
          spelling.getKey());
    }
  }

  /**
   * An unrecognized spelling must stay a client error at every entry point. Bean Validation rejects
   * it first on the PUT path; this pins that a caller reaching the mapper directly still gets a
   * request failure rather than the raw {@link IllegalArgumentException} MapStruct's implicit
   * conversion would raise.
   */
  @Test
  void unknownEntityTypeIsARequestFailureNotAnInternalError() {
    UserTable garbage =
        TestHouseTableModelConstants.TEST_USER_TABLE.toBuilder().entityType("UNKNOWN").build();

    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesMapper.toUserTableRow(garbage, Optional.empty()));
    Assertions.assertThrows(
        RequestValidationFailureException.class, () -> userTablesMapper.fromUserTable(garbage));
    Assertions.assertThrows(
        RequestValidationFailureException.class, () -> userTablesMapper.toEntityType("VIEWS"));
  }

  /**
   * Because the controller stamps at ingress, the mapper cannot legitimately receive a null, so
   * tolerating one would only hide a missed ingress path. Resolving a null <em>column</em> is the
   * read converter's job, and stays pinned there.
   */
  @Test
  void nullEntityTypeIsRejectedOnEveryWirePath() {
    UserTable untyped =
        TestHouseTableModelConstants.TEST_USER_TABLE.toBuilder().entityType(null).build();
    Assertions.assertNull(untyped.getEntityType());

    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesMapper.toUserTableRow(untyped, Optional.empty()));
    Assertions.assertThrows(
        RequestValidationFailureException.class, () -> userTablesMapper.fromUserTable(untyped));
    Assertions.assertThrows(
        RequestValidationFailureException.class, () -> userTablesMapper.toEntityType(null));
  }

  /** Reconstructed as a table by construction, so restore cannot reintroduce SQL NULLs. */
  @Test
  void softDeletedRowIsRestoredAsATable() {
    SoftDeletedUserTableRow softDeleted =
        SoftDeletedUserTableRow.builder()
            .tableId(TestHouseTableModelConstants.TEST_TABLE_ID)
            .databaseId(TestHouseTableModelConstants.TEST_DB_ID)
            .deletedAtMs(TEST_DELETED_AT_MS)
            .version(0L)
            .metadataLocation(TestHouseTableModelConstants.TEST_TBL_META_LOC)
            .storageType(TestHouseTableModelConstants.TEST_DEFAULT_STORAGE_TYPE)
            .creationTime(TestHouseTableModelConstants.TEST_CREATION_TIME)
            .purgeAfterMs(TEST_DELETED_AT_MS + TEST_PURGE_RETENTION_MS)
            .build();

    UserTableRow restored = userTablesMapper.toUserTableRow(softDeleted);

    Assertions.assertEquals(EntityType.TABLE, restored.getEntityType());
    Assertions.assertEquals(TestHouseTableModelConstants.TEST_TABLE_ID, restored.getTableId());
    Assertions.assertEquals(TestHouseTableModelConstants.TEST_DB_ID, restored.getDatabaseId());
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_TBL_META_LOC, restored.getMetadataLocation());
  }

  /**
   * {@code mapToUserTable} still binds an {@code entityType} request parameter onto the model, but
   * the query endpoint is table-scoped by path so nothing consumes it. This pins where the value
   * stops: bound here, then dropped by {@code toUserViewQuery}, never reaching a predicate.
   */
  @Test
  void mapToUserTableBindsButDoesNotConsumeEntityType() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("databaseId", "test_db0");
    parameters.put("entityType", "VIEW");

    UserTable mapped = userTablesMapper.mapToUserTable(parameters);

    Assertions.assertEquals("test_db0", mapped.getDatabaseId());
    Assertions.assertEquals("VIEW", mapped.getEntityType());
    Assertions.assertNull(mapped.getTableId());

    // The view query dispatches on which fields are null, so the hop must default nothing.
    UserViewQuery query = userTablesMapper.toUserViewQuery(mapped);

    Assertions.assertEquals("test_db0", query.getDatabaseId());
    Assertions.assertNull(query.getTableId());
    Assertions.assertNull(query.getTableVersion());
    Assertions.assertNull(query.getMetadataLocation());
    Assertions.assertNull(query.getStorageType());
    Assertions.assertNull(query.getCreationTime());

    // Conversely, every field that path reads survives the hop verbatim.
    UserViewQuery populated =
        userTablesMapper.toUserViewQuery(
            UserTable.builder()
                .databaseId("test_db0")
                .tableId("test_table0")
                .tableVersion("v1")
                .metadataLocation("/loc/v1.metadata.json")
                .storageType("objectstore")
                .creationTime(TEST_DELETED_AT_MS)
                .entityType(EntityType.VIEW.name())
                .deletedAtMs(TEST_DELETED_AT_MS)
                .purgeAfterMs(TEST_PURGE_RETENTION_MS)
                .build());

    Assertions.assertEquals("test_db0", populated.getDatabaseId());
    Assertions.assertEquals("test_table0", populated.getTableId());
    Assertions.assertEquals("v1", populated.getTableVersion());
    Assertions.assertEquals("/loc/v1.metadata.json", populated.getMetadataLocation());
    Assertions.assertEquals("objectstore", populated.getStorageType());
    Assertions.assertEquals(TEST_DELETED_AT_MS, populated.getCreationTime());
  }
}
