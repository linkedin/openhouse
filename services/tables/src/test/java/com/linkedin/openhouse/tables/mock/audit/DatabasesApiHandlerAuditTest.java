package com.linkedin.openhouse.tables.mock.audit;

import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.*;
import static com.linkedin.openhouse.tables.model.TableAuditModelConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import com.linkedin.openhouse.tables.mock.RequestConstants;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration
@WithMockUser(username = "testUser")
public class DatabasesApiHandlerAuditTest {
  @Autowired private MockMvc mvc;

  @MockBean private AuditHandler<TableAuditEvent> tableAuditHandler;

  @Captor private ArgumentCaptor<TableAuditEvent> argCaptor;

  @Test
  public void testGetAllDatabasesSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases")
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_GET_ALL_DATABASES_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testGetDatabaseAclPoliciesSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/aclPolicies")
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_GET_DATABASES_ACL_POLICIES_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testUpdateDatabaseAclPoliciesSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.patch(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/aclPolicies")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_UPDATE_ACL_POLICIES_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(
                TABLE_AUDIT_EVENT_UPDATE_DATABASES_ACL_POLICIES_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }
}
