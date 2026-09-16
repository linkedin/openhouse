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

class SystemActionContextTest {
  @AfterEach
  void clearRequest() {
    RequestContextHolder.resetRequestAttributes();
  }

  @ParameterizedTest
  @CsvSource(
      value = {"NULL,false", "true,true", "TRUE,true", "TrUe,true", "false,false", "FaLsE,false"},
      nullValues = "NULL")
  void evaluatesBooleanDeclarationWithoutChangingRawValue(String value, boolean enabled) {
    request(value);
    assertEquals(value, SystemActionContext.getDeclaration());
    assertEquals(enabled, SystemActionContext.isEnabled());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "yes", "1", " true", "false "})
  void invalidSuppliedValuesRemainReadableButCannotEnableAccess(String value) {
    request(value);
    assertEquals(value, SystemActionContext.getDeclaration());
    assertThrows(RequestValidationFailureException.class, SystemActionContext::isEnabled);
  }

  @Test
  void absentAndNonServletContextsDefaultToDisabled() {
    RequestContextHolder.resetRequestAttributes();
    assertNull(SystemActionContext.getDeclaration());
    assertFalse(SystemActionContext.isEnabled());
    RequestContextHolder.setRequestAttributes(mock(RequestAttributes.class));
    assertNull(SystemActionContext.getDeclaration());
    assertFalse(SystemActionContext.isEnabled());
  }

  private void request(String value) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader(SystemActionContext.HTTP_HEADER_SYSTEM_ACTION)).thenReturn(value);
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
  }
}
