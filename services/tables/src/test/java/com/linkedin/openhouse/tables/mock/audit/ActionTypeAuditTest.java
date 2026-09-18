package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.model.BaseAuditEvent;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

@SpringBootTest
@AutoConfigureMockMvc
@WithMockUser(username = "testUser")
class ActionTypeAuditTest {
  private static final String HEADER = "X-OpenHouse-Action-Type";

  @Autowired private MockMvc mvc;
  @MockBean private AuditHandler<ServiceAuditEvent> serviceAuditHandler;
  @MockBean private AuditHandler<TableAuditEvent> tableAuditHandler;

  @ParameterizedTest
  @CsvSource(
      value = {
        "SYSTEM,d200,200",
        "USER,d200,200",
        "NULL,d200,200",
        "SyStEm,d200,200",
        "SYSTEM,d404,404",
        "USER,d404,404",
        "NULL,d404,404",
        "invalid,d404,404",
        "invalid,d200,200",
        "true,d200,200",
        "false,d404,404"
      },
      nullValues = "NULL")
  void recordsDeclarationWithCallerAndTableContext(
      String declaration, String database, int expectedStatus) throws Exception {
    String uri = "/v1/databases/" + database + "/tables/tb1";
    MockHttpServletRequestBuilder request = get(uri);
    if (declaration != null) {
      request.header(HEADER, declaration);
    }
    mvc.perform(request).andExpect(status().is(expectedStatus));

    ArgumentCaptor<ServiceAuditEvent> serviceEvent =
        ArgumentCaptor.forClass(ServiceAuditEvent.class);
    verify(serviceAuditHandler).audit(serviceEvent.capture());
    assertDeclaration(serviceEvent.getValue(), declaration);
    assertEquals("testUser", serviceEvent.getValue().getUser());
    assertEquals(uri, serviceEvent.getValue().getUri());
    assertEquals(expectedStatus, serviceEvent.getValue().getStatusCode());

    ArgumentCaptor<TableAuditEvent> tableEvent = ArgumentCaptor.forClass(TableAuditEvent.class);
    verify(tableAuditHandler).audit(tableEvent.capture());
    assertDeclaration(tableEvent.getValue(), declaration);
    assertEquals("testUser", tableEvent.getValue().getUser());
    assertEquals(database, tableEvent.getValue().getDatabaseName());
    assertEquals("tb1", tableEvent.getValue().getTableName());
    assertEquals(
        expectedStatus == 200 ? OperationStatus.SUCCESS : OperationStatus.FAILED,
        tableEvent.getValue().getOperationStatus());
  }

  @Test
  void requestAuditFailureDoesNotChangeSuccessfulResponse() throws Exception {
    doThrow(new IllegalStateException("audit unavailable")).when(serviceAuditHandler).audit(any());
    mvc.perform(get("/v1/databases/d200/tables/tb1").header(HEADER, "SYSTEM"))
        .andExpect(status().isOk());
  }

  private static void assertDeclaration(BaseAuditEvent event, String expected) {
    JsonObject json = JsonParser.parseString(event.toJson()).getAsJsonObject();
    JsonElement declaration = json.get("actionType");
    assertNotNull(declaration);
    assertFalse(json.has("systemAction"));
    if (expected == null) {
      assertTrue(declaration.isJsonNull());
    } else {
      assertTrue(declaration.getAsJsonPrimitive().isString());
      assertEquals(expected, declaration.getAsString());
    }
  }
}
