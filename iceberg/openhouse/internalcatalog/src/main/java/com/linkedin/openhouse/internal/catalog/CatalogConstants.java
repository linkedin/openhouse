package com.linkedin.openhouse.internal.catalog;

/** Constants used across service and catalog layer. */
public final class CatalogConstants {
  public static final String SNAPSHOTS_JSON_KEY = "snapshotsJsonToBePut";
  public static final String SNAPSHOTS_REFS_KEY = "snapshotsRefs";
  public static final String IS_STAGE_CREATE_KEY = "isStageCreate";
  public static final String OPENHOUSE_UUID_KEY = "openhouse.tableUUID";
  public static final String INITIAL_VERSION = "INITIAL_VERSION";
  public static final String APPENDED_SNAPSHOTS = "appended_snapshots";
  public static final String STAGED_SNAPSHOTS = "staged_snapshots";
  public static final String CHERRY_PICKED_SNAPSHOTS = "cherry_picked_snapshots";
  public static final String DELETED_SNAPSHOTS = "deleted_snapshots";

  /** Used to uniquely identify an update towards a table from user side. */
  public static final String COMMIT_KEY = "commitKey";

  public static final String EVOLVED_SCHEMA_KEY = "evolved.table.schema";

  static final String FEATURE_TOGGLE_STOP_CREATE = "stop_create";

  static final String CLIENT_TABLE_SCHEMA = "client.table.schema";

  private CatalogConstants() {
    // Noop
  }
}
