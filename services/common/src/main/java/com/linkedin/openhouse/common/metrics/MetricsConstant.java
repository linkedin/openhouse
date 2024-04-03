package com.linkedin.openhouse.common.metrics;

public final class MetricsConstant {

  private MetricsConstant() {}

  public static final String REPO_TABLE_CREATED_CTR = "repo_table_created";
  public static final String REPO_TABLE_CREATED_CTR_STAGED = "repo_table_created_staged";
  public static final String REPO_TABLE_CREATED_WITH_DATA_CTR = "repo_table_created_with_data";
  public static final String REPO_TABLE_UPDATED_CTR = "repo_table_updated";
  public static final String REPO_TABLE_INVALID_SCHEMA_EVOLUTION =
      "repo_table_invalid_schema_evolution";
  public static final String REPO_TABLE_UNSUPPORTED_PARTITIONSPEC_EVOLUTION =
      "repo_table_unsupported_partitionspec_evolution";
  public static final String REPO_TABLE_ACCEPT_SNAPSHOT_CTR = "repo_table_snapshot_accepted";

  // Components sections
  public static final String JOBS_SERVICE = "jobs";
  public static final String SERVICE_AUDIT = "service_audit";

  // Common metric constants section
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

  // Performance metrics
  public static final String REPO_TABLE_SAVE_TIME = "repo_table_save_time";
  public static final String REPO_TABLE_CREATE_TIME = "repo_table_create_time";
  public static final String REPO_TABLE_UPDATE_TIME = "repo_table_update_time";
  public static final String REPO_TABLE_FIND_TIME = "repo_table_find_time";
  public static final String REPO_TABLE_EXISTS_TIME = "repo_table_exists_time";
  public static final String REPO_TABLE_DELETE_TIME = "repo_table_delete_time";
  public static final String REPO_TABLES_FIND_BY_DATABASE_TIME =
      "repo_tables_find_by_database_time";
  public static final String REPO_TABLES_SEARCH_BY_DATABASE_TIME =
      "repo_tables_search_by_database_time";
  public static final String REPO_TABLE_IDS_FIND_ALL_TIME = "repo_table_ids_find_all_time";
  public static final String REPO_TABLES_FIND_ALL_TIME = "repo_tables_find_all_time";
}
