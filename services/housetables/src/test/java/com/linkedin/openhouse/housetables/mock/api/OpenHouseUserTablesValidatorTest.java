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
}
