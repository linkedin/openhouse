package com.linkedin.openhouse.common.audit.model;

import com.google.gson.JsonElement;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpMethod;

/** Data Model for auditing http requests to each service. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class ServiceAuditEvent extends BaseAuditEvent {

  private Instant startTimestamp;

  private Instant endTimestamp;

  private String sessionId;

  private String clusterName;

  private String user;

  private ServiceName serviceName;

  private HttpMethod method;

  private String uri;

  private JsonElement requestPayload;

  private int statusCode;

  private String responseErrorMessage;

  private String stacktrace;

  private String cause;
}
