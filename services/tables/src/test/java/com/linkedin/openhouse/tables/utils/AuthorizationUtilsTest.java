package com.linkedin.openhouse.tables.utils;

import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.model.TableDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.security.access.AccessDeniedException;

public class AuthorizationUtilsTest {

  private static final String ACTING_PRINCIPAL = "test-user";
  private static final TableDto TABLE =
      TableDto.builder()
          .databaseId("database")
          .tableId("table")
          .tableCreator("table-creator")
          .build();

  private AuthorizationHandler authorizationHandler;
  private AuthorizationUtils authorizationUtils;

  @BeforeEach
  public void setUp() {
    authorizationHandler = Mockito.mock(AuthorizationHandler.class);
    authorizationUtils = new AuthorizationUtils();
    authorizationUtils.authorizationHandler = authorizationHandler;
  }

  @Test
  public void testLegacyTableWritePathAllowsUpdateMetadataPrivilege() {
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                ACTING_PRINCIPAL, TABLE, Privileges.UPDATE_TABLE_METADATA))
        .thenReturn(true);

    Assertions.assertDoesNotThrow(
        () ->
            authorizationUtils.checkTableWritePathPrivileges(
                TABLE, ACTING_PRINCIPAL, Privileges.UPDATE_TABLE_METADATA));
    Mockito.verify(authorizationHandler)
        .checkAccessDecision(ACTING_PRINCIPAL, TABLE, Privileges.UPDATE_TABLE_METADATA);
  }

  @Test
  public void testLegacyTableWritePathRejectsMissingUpdateMetadataPrivilege() {
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                ACTING_PRINCIPAL, TABLE, Privileges.UPDATE_TABLE_METADATA))
        .thenReturn(false);

    Assertions.assertThrows(
        AccessDeniedException.class,
        () ->
            authorizationUtils.checkTableWritePathPrivileges(
                TABLE, ACTING_PRINCIPAL, Privileges.UPDATE_TABLE_METADATA));
  }
}
