package com.linkedin.openhouse.tables.mock;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

public class RequestConstants {
  public static final GetTableResponseBody TEST_GET_TABLE_RESPONSE_BODY =
      GetTableResponseBody.builder()
          .databaseId("db1")
          .tableId("tb1")
          .clusterId("cl1")
          .tableUri("cl1.db1.tb1")
          .tableUUID(UUID.randomUUID().toString())
          .tableLocation("hdfs://namenode01/data/openhouse/")
          .tableVersion(String.valueOf(new Random().nextLong()))
          .tableCreator("DUMMY_ANONYMOUS_USER")
          .build();

  public static final GetAllTablesResponseBody TEST_GET_ALL_TABLES_RESPONSE_BODY =
      GetAllTablesResponseBody.builder()
          .results(Collections.singletonList(TEST_GET_TABLE_RESPONSE_BODY))
          .build();

  public static final CreateUpdateTableRequestBody TEST_CREATE_TABLE_REQUEST_BODY =
      CreateUpdateTableRequestBody.builder()
          .databaseId("db1")
          .tableId("tb1")
          .clusterId("cl1")
          .baseTableVersion("INITIAL_VERSION")
          .build();

  public static final CreateUpdateTableRequestBody TEST_STAGE_CREATE_TABLE_REQUEST_BODY =
      CreateUpdateTableRequestBody.builder()
          .databaseId("db1")
          .tableId("tb1")
          .clusterId("cl1")
          .baseTableVersion("INITIAL_VERSION")
          .stageCreate(true)
          .build();

  public static final CreateUpdateTableRequestBody TEST_UPDATE_TABLE_REQUEST_BODY =
      CreateUpdateTableRequestBody.builder()
          .databaseId("db1")
          .tableId("tb1")
          .clusterId("cl1")
          .baseTableVersion("SOME_VERSION")
          .build();

  public static final GetDatabaseResponseBody TEST_GET_DATABASE_RESPONSE_BODY =
      GetDatabaseResponseBody.builder().databaseId("db1").clusterId("cl1").build();

  public static final GetAllDatabasesResponseBody TEST_GET_ALL_DATABASES_RESPONSE_BODY =
      GetAllDatabasesResponseBody.builder()
          .results(Collections.singletonList(TEST_GET_DATABASE_RESPONSE_BODY))
          .build();

  public static final String TEST_ICEBERG_SNAPSHOT_JSON =
      "{\n"
          + "\"snapshot-id\" : 2151407017102313398,\n"
          + "\"timestamp-ms\" : 1669126937912,\n"
          + "\"summary\" : {\n"
          + "\"operation\" : \"append\",\n"
          + "\"spark.app.id\" : \"local-1669126906634\",\n"
          + "\"added-data-files\" : \"1\",\n"
          + "\"added-records\" : \"1\",\n"
          + "\"added-files-size\" : \"673\",\n"
          + "\"changed-partition-count\" : \"1\",\n"
          + "\"total-records\" : \"1\",\n"
          + "\"total-files-size\" : \"673\",\n"
          + "\"total-data-files\" : \"1\",\n"
          + "\"total-delete-files\" : \"0\",\n"
          + "\"total-position-deletes\" : \"0\",\n"
          + "\"total-equality-deletes\" : \"0\"\n"
          + "},\n"
          + "\"manifest-list\" : \"/data/openhouse/db/test_ctas_src-../metadata/snap-2151407017102313398-1-...avro\",\n"
          + "\"schema-id\" : 0\n"
          + "}\n";

  public static final IcebergSnapshotsRequestBody TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY =
      IcebergSnapshotsRequestBody.builder()
          .baseTableVersion("v1")
          .jsonSnapshots(Collections.singletonList(TEST_ICEBERG_SNAPSHOT_JSON))
          .createUpdateTableRequestBody(TEST_CREATE_TABLE_REQUEST_BODY)
          .build();

  public static final IcebergSnapshotsRequestBody
      TEST_ICEBERG_SNAPSHOTS_INITIAL_VERSION_REQUEST_BODY =
          IcebergSnapshotsRequestBody.builder()
              .baseTableVersion("INITIAL_VERSION")
              .jsonSnapshots(Collections.singletonList(TEST_ICEBERG_SNAPSHOT_JSON))
              .createUpdateTableRequestBody(TEST_CREATE_TABLE_REQUEST_BODY)
              .build();

  public static final UpdateAclPoliciesRequestBody TEST_UPDATE_ACL_POLICIES_REQUEST_BODY =
      UpdateAclPoliciesRequestBody.builder()
          .principal("DUMMY_ANONYMOUS_USER")
          .role("AclEditor")
          .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
          .build();

  public static final AclPolicy ACL_POLICY =
      AclPolicy.builder().principal("TEST_USER").role("TABLE_ADMIN").build();
  public static final GetAclPoliciesResponseBody TEST_GET_ACL_POLICIES_RESPONSE_BODY =
      GetAclPoliciesResponseBody.builder().results(Collections.singletonList(ACL_POLICY)).build();
}
