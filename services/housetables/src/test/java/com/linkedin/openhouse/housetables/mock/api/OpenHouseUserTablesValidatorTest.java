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

  /**
   * Transport-level nullability is load-bearing for rolling deploys: HTS and the tables service are
   * separate deployables, so an un-upgraded client will send no discriminator at all. Validation
   * must not be what rejects that — the controller resolves it to the type its route serves before
   * validation ever runs, so by the time the shared validator sees the payload it is already typed.
   *
   * <p>Preserved-behaviour regression test: it passes both before and after this change. It guards
   * the wire field staying nullable while the downstream mapper is tightened to reject null.
   */
  @Test
  public void validatePutEntityAcceptsATransportNullBeforeNormalization() {
    UserTable untyped =
        UserTable.builder()
            .tableId("tb1")
            .databaseId("db1")
            .tableVersion("/tmp/test/opt/metadata.json")
            .metadataLocation("INITIAL_VERSION")
            .entityType(null)
            .build();

    assertDoesNotThrow(() -> userTablesHtsApiValidator.validatePutEntity(untyped));

    // And the normalized form the controller hands on is equally valid, for both routes.
    assertDoesNotThrow(
        () ->
            userTablesHtsApiValidator.validatePutEntity(
                untyped.toBuilder().entityType("TABLE").build()));
    assertDoesNotThrow(
        () ->
            userTablesHtsApiValidator.validatePutEntity(
                untyped.toBuilder().entityType("VIEW").build()));
  }

  /**
   * The query endpoints are type-scoped by path, so an {@code entityType} filter is tolerated and
   * ignored rather than rejected. Rejecting it would be a separate, deliberate choice; this pins
   * that it has not been made by accident.
   *
   * <p>Preserved-behaviour regression test: it passes both before and after this change, because
   * the validator itself is deliberately untouched by this ticket.
   */
  @Test
  public void validateGetEntitiesToleratesAndIgnoresEntityType() {
    for (String entityType : new String[] {"VIEW", "view", "TABLE", "TaBlE", null}) {
      UserTable byDatabase = UserTable.builder().databaseId("db1").entityType(entityType).build();
      assertDoesNotThrow(
          () -> userTablesHtsApiValidator.validateGetEntities(byDatabase),
          "query with entityType=" + entityType + " should validate");
      assertDoesNotThrow(
          () -> userTablesHtsApiValidator.validateGetEntities(byDatabase, 0, 50, "tableId"));
    }

    // Even an unrecognized value is inert here: it is never a filter, so it is never validated.
    UserTable garbageFilter =
        UserTable.builder().databaseId("db1").tableId("tb%").entityType("UNKNOWN").build();
    assertDoesNotThrow(() -> userTablesHtsApiValidator.validateGetEntities(garbageFilter));
  }
}
