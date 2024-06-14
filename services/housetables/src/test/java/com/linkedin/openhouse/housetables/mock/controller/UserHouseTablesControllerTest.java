package com.linkedin.openhouse.housetables.mock.controller;

import static com.linkedin.openhouse.housetables.model.ServiceAuditModelConstants.*;
import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static com.linkedin.openhouse.housetables.model.TestHtsApiConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.housetables.api.handler.UserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.mock.MockUserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.model.TestHtsApiConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@AutoConfigureMockMvc
public class UserHouseTablesControllerTest {

  @Autowired private MockMvc mvc;

  @Autowired private ApplicationContext applicationContext;

  @MockBean private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  @Captor private ArgumentCaptor<ServiceAuditEvent> argCaptor;

  @Test
  public void testApiHandler() {
    Assertions.assertEquals(
        applicationContext.getBean(UserTableHtsApiHandler.class).getClass(),
        MockUserTableHtsApiHandler.class);
  }

  /** @throws Exception */
  @Test
  public void testGetTableRow() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/tables")
                .param("databaseId", TEST_TABLE_ID)
                .param("tableId", TEST_DB_ID)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(TestHtsApiConstants.TEST_GET_USER_TABLE_RESPONSE_BODY.toJson()));
  }

  @Test
  public void testPutTableRow() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(PUT_USER_TABLE_REQUEST_BODY.toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(TEST_GET_USER_TABLE_RESPONSE_BODY.toJson()));
  }

  @Test
  public void testServiceAuditSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put("/hts/tables")
            .contentType(MediaType.APPLICATION_JSON)
            .content(PUT_USER_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(serviceAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    ServiceAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(SERVICE_AUDIT_EVENT_PUT_TABLE_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }
}
