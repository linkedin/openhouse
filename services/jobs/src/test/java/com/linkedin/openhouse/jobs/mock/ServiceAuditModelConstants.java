package com.linkedin.openhouse.jobs.mock;

import com.google.gson.JsonNull;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.audit.model.ServiceName;
import org.springframework.http.HttpMethod;

public class ServiceAuditModelConstants {
  private static final String CLUSTER_NAME = "local-cluster";
  private static final String USER = "undefined";
  private static final String ERROR_MESSAGE = "Job %s cannot be found";

  public static final String[] EXCLUDE_FIELDS =
      new String[] {"startTimestamp", "endTimestamp", "stacktrace", "cause"};

  public static final ServiceAuditEvent SERVICE_AUDIT_EVENT_GET_JOB_SUCCESS =
      ServiceAuditEvent.builder()
          .serviceName(ServiceName.JOBS_SERVICE)
          .statusCode(200)
          .clusterName(CLUSTER_NAME)
          .user(USER)
          .uri("/jobs/my_job_2xx")
          .method(HttpMethod.GET)
          .requestPayload(JsonNull.INSTANCE)
          .build();

  public static final ServiceAuditEvent SERVICE_AUDIT_EVENT_GET_JOB_FAILED =
      ServiceAuditEvent.builder()
          .serviceName(ServiceName.JOBS_SERVICE)
          .statusCode(404)
          .clusterName(CLUSTER_NAME)
          .user(USER)
          .uri("/jobs/my_job_4xx")
          .method(HttpMethod.GET)
          .requestPayload(JsonNull.INSTANCE)
          .responseErrorMessage(String.format(ERROR_MESSAGE, "my_job_4xx"))
          .build();
}
