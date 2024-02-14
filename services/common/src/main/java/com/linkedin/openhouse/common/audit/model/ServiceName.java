package com.linkedin.openhouse.common.audit.model;

/** The service that handles the REST request */
public enum ServiceName {
  TABLES_SERVICE("tables_service"),
  JOBS_SERVICE("jobs_service"),
  HOUSETABLES_SERVICE("housetables_service"),
  UNRECOGNIZED("unrecognized");

  private final String serviceName;

  ServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getServiceName() {
    return serviceName;
  }
}
