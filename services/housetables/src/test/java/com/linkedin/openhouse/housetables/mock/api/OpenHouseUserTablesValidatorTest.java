package com.linkedin.openhouse.housetables.mock.api;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.validator.HouseTablesApiValidator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class OpenHouseUserTablesValidatorTest {

  @Autowired private HouseTablesApiValidator<UserTableKey, UserTable> userTablesHtsApiValidator;

  @Test
  public void validateGetEntitySuccess() {
    UserTableKey userTableKey = UserTableKey.builder().tableId("tbl1").databaseId("db1").build();
    assertDoesNotThrow(() -> userTablesHtsApiValidator.validateGetEntity(userTableKey));
  }

  @Test
  public void validateInValidGetEntity() {
    UserTableKey userTableKey = UserTableKey.builder().tableId("tb??1").databaseId("").build();

    // Invalid tableId and empty databaseId
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateGetEntity(userTableKey));
  }

  @Test
  public void validateGetAllEntitiesSuccessEmptyParams() {
    UserTable userTable = UserTable.builder().build();
    assertDoesNotThrow(() -> userTablesHtsApiValidator.validateGetEntities(userTable));
  }

  @Test
  public void validateGetAllEntitiesSuccessTablePattern() {
    UserTable userTable = UserTable.builder().tableId("%tb%").databaseId("db1").build();
    assertDoesNotThrow(() -> userTablesHtsApiValidator.validateGetEntities(userTable));
  }

  @Test
  public void validateInValidGetAllEntitiesBadDatabaseId() {
    UserTable userTable = UserTable.builder().databaseId("db%").build();

    // Invalid databaseId
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateGetEntities(userTable));
  }

  @Test
  public void validateInValidGetAllEntitiesTableIdWithoutDatabaseId() {
    UserTable userTable = UserTable.builder().tableId("tb").build();

    // Provide tableId without databaseId
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateGetEntities(userTable));
  }

  @Test
  public void validateInValidGetAllEntitiesUnsupportedField() {
    UserTable userTable = UserTable.builder().creationTime(1L).build();

    // Search by creationTime not supported.
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateGetEntities(userTable));
  }

  @Test
  public void validatePutEntitySuccess() {
    UserTable userTable =
        UserTable.builder()
            .tableId("tb1")
            .databaseId("db1")
            .tableVersion("/tmp/test/opt/metadata.json")
            .metadataLocation("INITIAL_VERSION")
            .build();

    assertDoesNotThrow(() -> userTablesHtsApiValidator.validatePutEntity(userTable));
  }

  @Test
  public void validateInvalidPutEntityRequest() {
    UserTable userTable = UserTable.builder().tableId("tb??").databaseId("db1").build();

    // Inadmissible values for tableID
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validatePutEntity(userTable));
  }

  @Test
  public void validateDeleteEntitySuccess() {
    UserTableKey userTableKey = UserTableKey.builder().tableId("tbl1").databaseId("db1").build();
    assertDoesNotThrow(() -> userTablesHtsApiValidator.validateDeleteEntity(userTableKey));
  }

  @Test
  public void validateInvalidDeleteEntity() {
    UserTableKey userTableKey = UserTableKey.builder().tableId("tb??").databaseId("db??").build();

    // Inadmissible values for tableId and databaseId
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateDeleteEntity(userTableKey));
  }

  @Test
  public void validateRenameEntityCaseInsensitiveSameName() {
    UserTableKey fromKey = UserTableKey.builder().tableId("testTable").databaseId("testDB").build();
    UserTableKey toKey = UserTableKey.builder().tableId("TESTTABLE").databaseId("TESTDB").build();

    // Should throw because it's the same table name (case-insensitive)
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateRenameEntity(fromKey, toKey));
  }

  @Test
  public void validateRenameEntityCaseInsensitiveCrossDatabase() {
    UserTableKey fromKey = UserTableKey.builder().tableId("testTable").databaseId("testDB").build();
    UserTableKey toKey =
        UserTableKey.builder().tableId("testTable").databaseId("DIFFERENTDB").build();

    // Should throw because cross-database rename is not supported
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateRenameEntity(fromKey, toKey));
  }

  @Test
  public void validateRenameEntityInvalidInput() {
    UserTableKey fromKey =
        UserTableKey.builder().tableId("invalid!table").databaseId("testDB").build();
    UserTableKey toKey = UserTableKey.builder().tableId("testTable").databaseId("testDB").build();

    // Should throw because of invalid table name format
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateRenameEntity(fromKey, toKey));
  }

  /**
   * A type-qualified query must reach the repository. NOTE: {@code validateUserTable} only rejects
   * non-null tableVersion/metadataLocation/storageType/creationTime, so this case passes whether or
   * not entityType validation exists. It guards against a future change that adds entityType to
   * that unsupported-field list; the load-bearing assertions for entityType validation live in
   * {@link #validateEntityTypeQueryRejectsGarbage} and {@link
   * #validatePutEntityTypeCaseInsensitivelyAndRejectsGarbage}.
   */
  @Test
  public void validateEntityTypeOnlyQueriesCaseInsensitively() {
    for (String entityType : new String[] {"VIEW", "view", "ViEw", "TABLE", "table", "TaBlE"}) {
      UserTable userTable = UserTable.builder().databaseId("db1").entityType(entityType).build();

      assertDoesNotThrow(
          () -> userTablesHtsApiValidator.validateGetEntities(userTable),
          "entityType=" + entityType + " should be an accepted unpaged query filter");
      assertDoesNotThrow(
          () -> userTablesHtsApiValidator.validateGetEntities(userTable, 0, 2, "tableId"),
          "entityType=" + entityType + " should be an accepted paged query filter");
    }
  }

  /**
   * Load-bearing: an unknown discriminator must be rejected before it ever reaches the repository,
   * so callers get a validation error rather than a silently empty result set.
   */
  @Test
  public void validateEntityTypeQueryRejectsGarbage() {
    UserTable garbage = UserTable.builder().databaseId("db1").entityType("UNKNOWN").build();

    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateGetEntities(garbage));
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateGetEntities(garbage, 0, 2, "tableId"));

    // The pre-existing unsupported-field rejection must not be weakened by adding entityType as
    // a permitted filter.
    UserTable unsupportedField = UserTable.builder().creationTime(1L).build();
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateGetEntities(unsupportedField));
    UserTable unsupportedFieldWithEntityType =
        UserTable.builder().databaseId("db1").entityType("VIEW").creationTime(1L).build();
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validateGetEntities(unsupportedFieldWithEntityType));
  }

  /**
   * Load-bearing: the PUT path relies on Bean Validation on the transport model, so the pattern on
   * {@code UserTable#entityType} must accept every TABLE/VIEW spelling and reject anything else.
   */
  @Test
  public void validatePutEntityTypeCaseInsensitivelyAndRejectsGarbage() {
    for (String entityType : new String[] {"VIEW", "view", "ViEw", "TABLE", "table", "TaBlE"}) {
      UserTable userTable =
          UserTable.builder()
              .tableId("tb1")
              .databaseId("db1")
              .tableVersion("/tmp/test/opt/metadata.json")
              .metadataLocation("INITIAL_VERSION")
              .entityType(entityType)
              .build();

      assertDoesNotThrow(
          () -> userTablesHtsApiValidator.validatePutEntity(userTable),
          "PUT with entityType=" + entityType + " should validate");
    }

    // Omitting the field entirely stays valid (legacy table writers).
    assertDoesNotThrow(
        () ->
            userTablesHtsApiValidator.validatePutEntity(
                UserTable.builder()
                    .tableId("tb1")
                    .databaseId("db1")
                    .tableVersion("/tmp/test/opt/metadata.json")
                    .metadataLocation("INITIAL_VERSION")
                    .build()));

    UserTable garbage =
        UserTable.builder()
            .tableId("tb1")
            .databaseId("db1")
            .tableVersion("/tmp/test/opt/metadata.json")
            .metadataLocation("INITIAL_VERSION")
            .entityType("UNKNOWN")
            .build();
    assertThrows(
        RequestValidationFailureException.class,
        () -> userTablesHtsApiValidator.validatePutEntity(garbage));
  }
}
