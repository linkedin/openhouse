package com.linkedin.openhouse.jobs.mock;

import static com.linkedin.openhouse.jobs.mock.ServiceAuditModelConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.CachingRequestBodyFilter;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.jobs.api.handler.JobsApiHandler;
import com.linkedin.openhouse.jobs.controller.JobsController;
import com.linkedin.openhouse.jobs.services.HouseJobsCoordinator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@SpringBootTest
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
@ComponentScan(
    basePackages = {
      "com.linkedin.openhouse.common.audit",
      "com.linkedin.openhouse.common.exception"
    })
public class JobsControllerTest {
  private MockMvc mvc;

  @Autowired private ApplicationContext applicationContext;

  @Autowired private JobsController controller;

  @MockBean private HouseJobsCoordinator jobsCoordinator;

  @Autowired private OpenHouseExceptionHandler openHouseExceptionHandler;

  @MockBean private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  @Captor private ArgumentCaptor<ServiceAuditEvent> argCaptor;

  @BeforeEach
  public void setup() {
    mvc =
        MockMvcBuilders.standaloneSetup(controller)
            .setControllerAdvice(openHouseExceptionHandler)
            .addFilter(new CachingRequestBodyFilter())
            .build();
  }

  @Test
  public void testApiHandler() {
    Assertions.assertEquals(
        MockJobsApiHandler.class, applicationContext.getBean(JobsApiHandler.class).getClass());
  }

  @Test
  public void getJobById2xx() throws Exception {
    mvc.perform(MockMvcRequestBuilders.get("/jobs/my_job_2xx").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  @Test
  public void getJobById4xx() throws Exception {
    mvc.perform(MockMvcRequestBuilders.get("/jobs/my_job_4xx").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void cancelJobById2xx() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/jobs/my_job_2xx/cancel")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNoContent());
  }

  @Test
  public void cancelJobById4xx() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.put("/jobs/my_job_404/cancel")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
    mvc.perform(
            MockMvcRequestBuilders.put("/jobs/my_job_409/cancel")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isConflict());
  }

  @Test
  public void createJob2xx() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post("/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(RequestConstants.TEST_CREATE_JOB_REQUEST_BODY.toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    RequestConstants.TEST_GET_JOB_RESPONSE_BODY_BUILDER
                        .jobId("my_job_id")
                        .build()
                        .toJson()));
  }

  @Test
  public void createJob4xx() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post("/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(RequestConstants.TEST_CREATE_INVALID_JOB_REQUEST_BODY.toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testServiceAuditSuccessfulPath() throws Exception {
    mvc.perform(MockMvcRequestBuilders.get("/jobs/my_job_2xx").accept(MediaType.APPLICATION_JSON));
    Mockito.verify(serviceAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    ServiceAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(SERVICE_AUDIT_EVENT_GET_JOB_SUCCESS, excludeFields)
            .matches(actualEvent));
  }

  @Test
  public void testServiceAuditFailedPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(String.format("/jobs/my_job_4xx"))
            .accept(MediaType.APPLICATION_JSON));
    Mockito.verify(serviceAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    ServiceAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(SERVICE_AUDIT_EVENT_GET_JOB_FAILED, excludeFields)
            .matches(actualEvent));
  }
}
