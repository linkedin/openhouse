package com.linkedin.openhouse.tables.utils;

import com.linkedin.openhouse.tables.model.TableDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.security.access.AccessDeniedException;

public class AuthorizationUtilsTest {

  private static final String TABLE_CREATOR =
      "urn:li:gridUser:test-user/urn:li:sparksvc:sparksvc_oh_2";

  private final AuthorizationUtils authorizationUtils = new AuthorizationUtils();

  @Test
  public void testReplaceTablePrivilegeAllowsSameGridUser() {
    TableDto tableDto =
        TableDto.builder()
            .databaseId("database")
            .tableId("table")
            .tableCreator(TABLE_CREATOR)
            .build();

    Assertions.assertDoesNotThrow(
        () ->
            authorizationUtils.checkReplaceTablePrivilege(
                tableDto, "urn:li:gridUser:test-user/urn:li:servicePrincipal:jobs-service"));
  }

  @Test
  public void testReplaceTablePrivilegeRejectsDifferentGridUser() {
    TableDto tableDto =
        TableDto.builder()
            .databaseId("database")
            .tableId("table")
            .tableCreator(TABLE_CREATOR)
            .build();

    Assertions.assertThrows(
        AccessDeniedException.class,
        () ->
            authorizationUtils.checkReplaceTablePrivilege(
                tableDto, "urn:li:gridUser:another-user/urn:li:servicePrincipal:jobs-service"));
  }
}
