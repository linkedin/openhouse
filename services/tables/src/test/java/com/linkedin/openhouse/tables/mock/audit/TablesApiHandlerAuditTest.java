package com.linkedin.openhouse.tables.mock.audit;

import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX;
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
public class TablesApiHandlerAuditTest {
  @Autowired private MockMvc mvc;

  @MockBean private AuditHandler<TableAuditEvent> tableAuditHandler;

  @Captor private ArgumentCaptor<TableAuditEvent> argCaptor;

  @Test
  public void testGetTableSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables/tb1")
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_GET_TABLE_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testGetTableFailedPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d404/tables/tb1")
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_GET_TABLE_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testCreateTableSuccessfulPath() throws Exception {
    // test create table using POST api
    mvc.perform(
        MockMvcRequestBuilders.post(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_CREATE_TABLE_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));

    // test create table using PUT api
    mvc.perform(
        MockMvcRequestBuilders.put(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables/tb1")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent2 = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_CREATE_TABLE_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent2));
  }

  @Test
  public void testCreateTableFailedPath() throws Exception {
    // test create table using POST api
    mvc.perform(
        MockMvcRequestBuilders.post(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d409/tables")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_CREATE_TABLE_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));

    // test create table using PUT api
    mvc.perform(
        MockMvcRequestBuilders.post(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d409/tables/tb1")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent2 = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_CREATE_TABLE_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent2));
  }

  @Test
  public void testStagedCreateSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.post(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_STAGE_CREATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_STAGED_CREATE_TABLE_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testStagedCreateFailurePath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.post(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d404/tables")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_STAGE_CREATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_STAGED_CREATE_TABLE_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testUpdateTableSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables/tb1")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_UPDATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_UPDATE_TABLE_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testUpdateTableFailedPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d404/tables/tb1")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_UPDATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_UPDATE_TABLE_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testDeleteTableSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.delete("/v2/databases/d200/tables/tb1?purge=true")
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_DELETE_TABLE_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testDeleteTableFailedPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.delete("/v2/databases/d400/tables/tb1?purge=true")
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_DELETE_TABLE_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testGetAclPoliciesSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(
                CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables/tb1/aclPolicies")
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_GET_ACL_POLICIES_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testGetAclPoliciesFailedPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(
                CURRENT_MAJOR_VERSION_PREFIX + "/databases/d400/tables/tb1/aclPolicies")
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_GET_ACL_POLICIES_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testUpdateAclPoliciesSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.patch(
                CURRENT_MAJOR_VERSION_PREFIX + "/databases/d204/tables/tb1/aclPolicies")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_UPDATE_ACL_POLICIES_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_UPDATE_ACL_POLICIES_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testUpdateAclPoliciesFailedPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.patch(
                CURRENT_MAJOR_VERSION_PREFIX + "/databases/d400/tables/tb1/aclPolicies")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_UPDATE_ACL_POLICIES_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_UPDATE_ACL_POLICIES_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }
}
