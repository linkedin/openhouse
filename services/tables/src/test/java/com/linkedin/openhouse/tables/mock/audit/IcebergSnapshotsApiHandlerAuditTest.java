package com.linkedin.openhouse.tables.mock.audit;

import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.*;
import static com.linkedin.openhouse.tables.model.TableAuditModelConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import com.linkedin.openhouse.tables.mock.RequestConstants;
import lombok.extern.slf4j.Slf4j;
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
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration
@WithMockUser(username = "testUser")
@Slf4j
public class IcebergSnapshotsApiHandlerAuditTest {
  @Autowired private MockMvc mvc;

  @MockBean private AuditHandler<TableAuditEvent> tableAuditHandler;

  @Captor private ArgumentCaptor<TableAuditEvent> argCaptor;

  @Autowired private ApplicationContext appContext;

  @Test
  public void tmp() {
    try {
      Object bean = appContext.getBean("storageManager");
      log.info("bean: {}", bean);
    } catch (Exception e) {
      log.error("ignore bean");
    }
  }

  @Test
  public void testPutIcebergSnapshotsSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_PUT_ICEBERG_SNAPSHOTS_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testPutIcebergSnapshotsFailedPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d400/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_PUT_ICEBERG_SNAPSHOTS_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testCTASCommitPhase() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(
                RequestConstants.TEST_ICEBERG_SNAPSHOTS_INITIAL_VERSION_REQUEST_BODY.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_PUT_ICEBERG_SNAPSHOTS_CTAS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }
}
