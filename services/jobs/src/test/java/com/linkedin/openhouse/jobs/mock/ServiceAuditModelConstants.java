package com.linkedin.openhouse.jobs.mock;

import com.google.gson.JsonNull;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.audit.model.ServiceName;
import org.springframework.http.HttpMethod;

public class ServiceAuditModelConstants {
  private static final String clusterName = "local-cluster";
  private static final String user = "undefined";
  private static final String errorMessage = "Job %s cannot be found";

  public static final String[] excludeFields =
      new String[] {"startTimestamp", "endTimestamp", "stacktrace", "cause"};

  public static final ServiceAuditEvent SERVICE_AUDIT_EVENT_GET_JOB_SUCCESS =
      ServiceAuditEvent.builder()
          .serviceName(ServiceName.JOBS_SERVICE)
          .statusCode(200)
          .clusterName(clusterName)
          .user(user)
          .uri("/jobs/my_job_2xx")
          .method(HttpMethod.GET)
          .requestPayload(JsonNull.INSTANCE)
          .build();

  public static final ServiceAuditEvent SERVICE_AUDIT_EVENT_GET_JOB_FAILED =
      ServiceAuditEvent.builder()
          .serviceName(ServiceName.JOBS_SERVICE)
          .statusCode(404)
          .clusterName(clusterName)
          .user(user)
          .uri("/jobs/my_job_4xx")
          .method(HttpMethod.GET)
          .requestPayload(JsonNull.INSTANCE)
          .responseErrorMessage(String.format(errorMessage, "my_job_4xx"))
          .build();
}
