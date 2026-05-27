package com.linkedin.openhouse.tables.rest.config;

/** URL roots for the Iceberg REST Catalog adapter. */
public final class IcebergRestPaths {
  private IcebergRestPaths() {}

  public static final String BASE = "/iceberg";
  public static final String V1 = BASE + "/v1";
  public static final String CONFIG = V1 + "/config";
}
