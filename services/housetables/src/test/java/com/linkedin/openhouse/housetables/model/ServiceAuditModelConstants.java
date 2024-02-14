package com.linkedin.openhouse.housetables.model;

import static com.linkedin.openhouse.housetables.model.TestHtsApiConstants.*;

import com.google.gson.JsonParser;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.audit.model.ServiceName;
import org.springframework.http.HttpMethod;

public class ServiceAuditModelConstants {
  private static final String clusterName = "local-cluster";
  private static final String user = "undefined";

  public static final String[] excludeFields = new String[] {"startTimestamp", "endTimestamp"};

  public static final ServiceAuditEvent SERVICE_AUDIT_EVENT_PUT_TABLE_SUCCESS =
      ServiceAuditEvent.builder()
          .serviceName(ServiceName.HOUSETABLES_SERVICE)
          .statusCode(200)
          .clusterName(clusterName)
          .user(user)
          .uri("/hts/tables")
          .method(HttpMethod.PUT)
          .requestPayload(JsonParser.parseString(PUT_USER_TABLE_REQUEST_BODY.toJson()))
          .build();
}
