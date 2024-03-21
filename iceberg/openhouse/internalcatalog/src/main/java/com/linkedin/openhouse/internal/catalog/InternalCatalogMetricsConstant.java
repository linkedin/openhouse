package com.linkedin.openhouse.internal.catalog;

public final class InternalCatalogMetricsConstant {
  private InternalCatalogMetricsConstant() {}

  static final String METRICS_PREFIX = "catalog";

  static final String COMMIT_FAILED_CTR = "commit_failed";
  static final String COMMIT_STATE_UNKNOWN = "commit_state_unknown";
  static final String NO_TABLE_WHEN_REFRESH = "table_not_found_during_refresh";

  static final String MISSING_COMMIT_KEY = "commit_key_missing";

  static final String SNAPSHOTS_ADDED_CTR = "snapshots_added";
  static final String SNAPSHOTS_STAGED_CTR = "snapshots_staged";
  static final String SNAPSHOTS_CHERRY_PICKED_CTR = "snapshots_cherry_picked";
  static final String SNAPSHOTS_DELETED_CTR = "snapshots_deleted";

  static final String METADATA_UPDATE_LATENCY = "metadata_update_latency";
  static final String METADATA_RETRIEVAL_LATENCY = "metadata_retrieval_latency";
}
