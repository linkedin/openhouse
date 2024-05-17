package com.linkedin.openhouse.tables.model;

import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX;

import com.google.gson.JsonNull;
import com.google.gson.JsonParser;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.audit.model.ServiceName;
import com.linkedin.openhouse.tables.mock.RequestConstants;
import org.springframework.http.HttpMethod;

public class ServiceAuditModelConstants {
  private static final String CLUSTER_NAME = "local-cluster";
  private static final String MOCK_USER = "DUMMY_ANONYMOUS_USER";
  private static final String END_TO_END_USER = "testUser";
  private static final String ERROR_MESSAGE = "User table %s.%s cannot be found";

  public static final String[] EXCLUDE_FIELDS =
      new String[] {"startTimestamp", "endTimestamp", "stacktrace", "cause"};

  public static final ServiceAuditEvent SERVICE_AUDIT_EVENT_CREATE_TABLE_SUCCESS =
      ServiceAuditEvent.builder()
          .serviceName(ServiceName.TABLES_SERVICE)
          .statusCode(201)
          .clusterName(CLUSTER_NAME)
          .user(MOCK_USER)
          .uri(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables")
          .method(HttpMethod.POST)
          .requestPayload(
              JsonParser.parseString(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson()))
          .build();

  public static final ServiceAuditEvent SERVICE_AUDIT_EVENT_CREATE_TABLE_FAILED =
      ServiceAuditEvent.builder()
          .serviceName(ServiceName.TABLES_SERVICE)
          .statusCode(404)
          .clusterName(CLUSTER_NAME)
          .user(MOCK_USER)
          .uri(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d404/tables")
          .method(HttpMethod.POST)
          .requestPayload(
              JsonParser.parseString(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson()))
          .responseErrorMessage(String.format(ERROR_MESSAGE, "d404", "tb1"))
          .build();

  public static final ServiceAuditEvent SERVICE_AUDIT_EVENT_RUNTIME_EXCEPTION =
      ServiceAuditEvent.builder()
          .serviceName(ServiceName.TABLES_SERVICE)
          .statusCode(500)
          .clusterName(CLUSTER_NAME)
          .user(MOCK_USER)
          .uri(CURRENT_MAJOR_VERSION_PREFIX + "/databases/dnullpointer/tables/t1")
          .method(HttpMethod.GET)
          .requestPayload(JsonNull.INSTANCE)
          .responseErrorMessage((new NullPointerException()).toString())
          .build();

  public static final ServiceAuditEvent SERVICE_AUDIT_EVENT_END_TO_END =
      ServiceAuditEvent.builder()
          .serviceName(ServiceName.TABLES_SERVICE)
          .statusCode(200)
          .clusterName(CLUSTER_NAME)
          .user(END_TO_END_USER)
          .uri(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d1/tables/t1")
          .method(HttpMethod.GET)
          .requestPayload(JsonNull.INSTANCE)
          .build();
}
