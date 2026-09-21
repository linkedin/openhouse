package com.linkedin.openhouse.common.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

class ActionTypeContextTest {
  @AfterEach
  void clearRequest() {
    RequestContextHolder.resetRequestAttributes();
  }

  @ParameterizedTest
  @CsvSource(
      value = {"NULL,false", "SYSTEM,true", "system,true", "SyStEm,true"},
      nullValues = "NULL")
  void evaluatesActionTypeWithoutChangingRawValue(String value, boolean systemAction) {
    request(value);
    assertEquals(value, ActionTypeContext.getDeclaration());
    assertEquals(systemAction, ActionTypeContext.isSystemAction());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"", " ", "yes", "1", "true", "false", " SYSTEM", "SYSTEM ", "USER", "UsEr"})
  void invalidSuppliedValuesRemainReadableButCannotEnableAccess(String value) {
    request(value);
    assertEquals(value, ActionTypeContext.getDeclaration());
    assertThrows(RequestValidationFailureException.class, ActionTypeContext::isSystemAction);
  }

  @Test
  void absentAndNonServletContextsAreNotSystemActions() {
    RequestContextHolder.resetRequestAttributes();
    assertNull(ActionTypeContext.getDeclaration());
    assertFalse(ActionTypeContext.isSystemAction());
    RequestContextHolder.setRequestAttributes(mock(RequestAttributes.class));
    assertNull(ActionTypeContext.getDeclaration());
    assertFalse(ActionTypeContext.isSystemAction());
  }

  private void request(String value) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader(ActionTypeContext.HTTP_HEADER_ACTION_TYPE)).thenReturn(value);
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
  }
}
