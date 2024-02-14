package com.linkedin.openhouse.tables.model;

import com.linkedin.openhouse.tables.api.spec.v0.response.GetDatabaseResponseBody;

public class DatabaseModelConstants {
  private static final String clusterName = "local-cluster";

  public static final GetDatabaseResponseBody GET_DATABASE_RESPONSE_BODY =
      GetDatabaseResponseBody.builder().databaseId("d1").clusterId(clusterName).build();

  public static final GetDatabaseResponseBody GET_DATABASE_RESPONSE_BODY_DIFF_DB =
      GetDatabaseResponseBody.builder().databaseId("d2").clusterId(clusterName).build();

  public static final DatabaseDto DATABASE_DTO =
      DatabaseModelConstants.buildDatabaseDto(GET_DATABASE_RESPONSE_BODY);

  public static DatabaseDto buildDatabaseDto(GetDatabaseResponseBody getDatabaseResponseBody) {
    return DatabaseDto.builder()
        .databaseId(getDatabaseResponseBody.getDatabaseId())
        .clusterId(getDatabaseResponseBody.getClusterId())
        .build();
  }
}
