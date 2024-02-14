package com.linkedin.openhouse.internal.catalog;

/** Constants used across service and catalog layer. */
public final class CatalogConstants {
  public static final String SNAPSHOTS_JSON_KEY = "snapshotsJsonToBePut";
  public static final String IS_STAGE_CREATE_KEY = "isStageCreate";
  public static final String OPENHOUSE_UUID_KEY = "openhouse.tableUUID";

  /** Used to uniquely identify an update towards a table from user side. */
  public static final String COMMIT_KEY = "commitKey";

  private CatalogConstants() {
    // Noop
  }
}
