package com.linkedin.openhouse.common.metrics;

public final class MetricsConstant {

  private MetricsConstant() {}

  public static final String REPO_TABLE_CREATED_CTR = "repo_table_created";
  public static final String REPO_TABLE_CREATED_CTR_STAGED = "repo_table_created_staged";
  public static final String REPO_TABLE_CREATED_WITH_DATA = "repo_table_created_with_data";
  public static final String REPO_TABLE_UPDATED_CTR = "repo_table_updated";
  public static final String REPO_TABLE_INVALID_SCHEMA_EVOLUTION =
      "repo_table_invalid_schema_evolution";
  public static final String REPO_TABLE_UNSUPPORTED_PARTITIONSPEC_EVOLUTION =
      "repo_table_unsupported_partitionspec_evolution";
  public static final String REPO_TABLE_ACCEPT_SNAPSHOT_CTR = "repo_table_snapshot_accepted";

  // Components sections
  public static final String JOBS_SERVICE = "jobs";
  public static final String SERVICE_AUDIT = "service_audit";

  //  Common metric constants section
  public static final String ACTION_TAG = "action";
  public static final String CANCEL = "cancel";
  public static final String GET = "get";
  public static final String SUBMIT = "submit";
  public static final String CREATE = "create";
  public static final String REQUEST_COUNT = "request_count";
  public static final String REQUEST = "request";

  // Metrics for auditing
  public static final String FAILED_SERVICE_AUDIT = "failed_service_audit";
  public static final String FAILED_PARSING_REQUEST_PAYLOAD = "failed_parsing_request_payload";
}
