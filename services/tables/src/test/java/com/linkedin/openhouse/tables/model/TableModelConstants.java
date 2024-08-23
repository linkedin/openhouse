package com.linkedin.openhouse.tables.model;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;

import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.common.test.schema.ResourceIoHelper;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.RetentionColumnPattern;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Transform;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.attribute.ClusteringSpecConverter;
import com.linkedin.openhouse.tables.dto.mapper.attribute.PoliciesSpecConverter;
import com.linkedin.openhouse.tables.dto.mapper.attribute.TimePartitionSpecConverter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.TableProperties;
import org.springframework.test.web.servlet.MvcResult;

public class TableModelConstants {
  public static String HEALTH_SCHEMA_LITERAL;
  public static String UNHEALTHY_CLUSTER_SCHEMA_LITERAL;

  // Evolution I: Adding a new field with default value,
  // this is evolved on top of HEALTH_SCHEMA_LITERAL.
  public static String ADD_OPTIONAL_FIELD;

  public static RetentionColumnPattern COL_PAT;
  public static Retention RETENTION_POLICY;

  public static Retention RETENTION_POLICY_WITH_PATTERN;
  public static Retention RETENTION_POLICY_WITH_EMPTY_PATTERN;
  public static Policies TABLE_POLICIES_WITH_EMPTY_PATTERN;
  public static Policies TABLE_POLICIES;

  public static Policies TABLE_POLICIES_COMPLEX;
  public static Policies SHARED_TABLE_POLICIES;
  public static String TEST_USER;

  public static String TEST_USER_PRINCIPAL;
  public static String CLUSTER_NAME;

  static {
    COL_PAT = RetentionColumnPattern.builder().columnName("name").pattern("yyyy").build();

    RETENTION_POLICY =
        Retention.builder().count(3).granularity(TimePartitionSpec.Granularity.HOUR).build();

    RETENTION_POLICY_WITH_PATTERN =
        Retention.builder()
            .count(3)
            .granularity(TimePartitionSpec.Granularity.HOUR)
            .columnPattern(COL_PAT)
            .build();

    RETENTION_POLICY_WITH_EMPTY_PATTERN =
        RETENTION_POLICY_WITH_PATTERN.toBuilder()
            .columnPattern(COL_PAT.toBuilder().pattern("").build())
            .build();

    TABLE_POLICIES = Policies.builder().retention(RETENTION_POLICY).build();
    TABLE_POLICIES_COMPLEX = Policies.builder().retention(RETENTION_POLICY_WITH_PATTERN).build();
    SHARED_TABLE_POLICIES =
        Policies.builder().retention(RETENTION_POLICY).sharingEnabled(true).build();
    TABLE_POLICIES_WITH_EMPTY_PATTERN =
        Policies.builder().retention(RETENTION_POLICY_WITH_EMPTY_PATTERN).build();
    TEST_USER = "testUser";
    TEST_USER_PRINCIPAL = "testUserPrincipal";
    CLUSTER_NAME = "local-cluster";
    try {
      HEALTH_SCHEMA_LITERAL =
          ResourceIoHelper.getSchemaJsonFromResource("dummy_healthy_schema.json");
      ADD_OPTIONAL_FIELD =
          ResourceIoHelper.getSchemaJsonFromResource("evolved_dummy_healthy_schema.json");
      UNHEALTHY_CLUSTER_SCHEMA_LITERAL =
          ResourceIoHelper.getSchemaJsonFromResource("dummy_unhealthy_cluster_schema.json");
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to initialize constant: Healthy Schema literal");
    }
  }

  public static final Map<String, String> TABLE_PROPS =
      new HashMap<String, String>() {
        {
          put("user.a", "b");
          put("user.c", "d");
        }
      };

  public static final Map<String, String> TABLE_PROPS_RESERVED =
      new HashMap<String, String>() {
        {
          put("user.a", "b");
          put("openhouse.random", "random");
        }
      };

  // Essentially a clone of TableDto with schema field being decorated.
  public static TableDto decorateSchemaEvolution(TableDto base, String evolvedSchema) {
    return TableDto.builder()
        .tableId(base.getTableId())
        .databaseId(base.getDatabaseId())
        .clusterId(base.getClusterId())
        .tableUri(base.getTableUri())
        .tableLocation(base.getTableLocation())
        .tableVersion(base.getTableVersion())
        .tableUUID(base.getTableUUID())
        .tableProperties(base.getTableProperties())
        .schema(evolvedSchema)
        .timePartitioning(base.getTimePartitioning())
        .clustering(base.getClustering())
        .build();
  }

  // TODO: Replace all these decorate method with a better pattern to avoid code duplication
  public static TableDto decorateTimePartitionSpec(
      TableDto base, TimePartitionSpec timePartitionSpec) {
    return TableDto.builder()
        .tableId(base.getTableId())
        .databaseId(base.getDatabaseId())
        .clusterId(base.getClusterId())
        .tableUri(base.getTableUri())
        .tableLocation(base.getTableLocation())
        .tableVersion(base.getTableVersion())
        .tableUUID(base.getTableUUID())
        .tableProperties(base.getTableProperties())
        .schema(base.getSchema())
        .timePartitioning(timePartitionSpec)
        .clustering(null)
        .tableProperties(base.getTableProperties())
        .policies(base.getPolicies())
        .build();
  }

  // TODO: Clean this method
  public static TableDto evolveDummySchema(TableDto baseWithDummySchema) {
    return decorateSchemaEvolution(baseWithDummySchema, ADD_OPTIONAL_FIELD);
  }

  public static GetTableResponseBody evolveDummySchema(MvcResult baseMvcResult)
      throws UnsupportedEncodingException {
    GetTableResponseBody container =
        GetTableResponseBody.builder().schema(ADD_OPTIONAL_FIELD).build();
    return buildGetTableResponseBody(baseMvcResult, container);
  }

  public static final GetTableResponseBody GET_TABLE_RESPONSE_BODY =
      GetTableResponseBody.builder()
          .tableId("t1")
          .databaseId("d1")
          .clusterId(CLUSTER_NAME)
          .tableUri(CLUSTER_NAME + ".d1.t1")
          .tableLocation(INITIAL_TABLE_VERSION)
          .tableVersion(INITIAL_TABLE_VERSION)
          .tableUUID(UUID.randomUUID().toString())
          .tableProperties(buildTableProperties(TABLE_PROPS))
          .tableCreator(TEST_USER)
          .schema(HEALTH_SCHEMA_LITERAL)
          .policies(TABLE_POLICIES)
          .tableType(TableType.PRIMARY_TABLE)
          .timePartitioning(
              TimePartitionSpec.builder()
                  .columnName("timestampCol")
                  .granularity(TimePartitionSpec.Granularity.HOUR)
                  .build())
          .clustering(
              Arrays.asList(
                  ClusteringColumn.builder().columnName("id").build(),
                  ClusteringColumn.builder()
                      .columnName("name")
                      .transform(
                          Transform.builder()
                              .transformType(Transform.TransformType.TRUNCATE)
                              .transformParams(Arrays.asList("10"))
                              .build())
                      .build()))
          .build();

  public static final GetTableResponseBody GET_TABLE_RESPONSE_BODY_RESERVED_PROP =
      GetTableResponseBody.builder()
          .tableId("t1")
          .databaseId("d1")
          .clusterId(CLUSTER_NAME)
          .tableUri(CLUSTER_NAME + ".d1.t1")
          .tableLocation("loc1")
          .tableVersion(String.valueOf(new Random().nextLong()))
          .tableUUID(UUID.randomUUID().toString())
          .tableProperties(buildTableProperties(TABLE_PROPS_RESERVED))
          .tableCreator(TEST_USER)
          .schema(HEALTH_SCHEMA_LITERAL)
          .policies(TABLE_POLICIES)
          .timePartitioning(
              TimePartitionSpec.builder()
                  .columnName("timestampCol")
                  .granularity(TimePartitionSpec.Granularity.HOUR)
                  .build())
          .clustering(
              Arrays.asList(
                  ClusteringColumn.builder().columnName("name").build(),
                  ClusteringColumn.builder().columnName("id").build()))
          .build();

  public static final GetTableResponseBody GET_TABLE_RESPONSE_BODY_NULL_PROP =
      GetTableResponseBody.builder()
          .tableId("t1")
          .databaseId("d1")
          .clusterId(CLUSTER_NAME)
          .tableUri(CLUSTER_NAME + ".d1.t1")
          .tableLocation("loc1")
          .tableVersion(String.valueOf(new Random().nextLong()))
          .tableCreator(TEST_USER)
          .tableUUID(UUID.randomUUID().toString())
          .tableProperties(null)
          .schema(HEALTH_SCHEMA_LITERAL)
          .timePartitioning(null)
          .clustering(null)
          .policies(TABLE_POLICIES)
          .build();

  public static final GetTableResponseBody GET_TABLE_RESPONSE_BODY_WITH_TABLE_TYPE =
      GetTableResponseBody.builder()
          .tableId("t1")
          .databaseId("d1")
          .clusterId(CLUSTER_NAME)
          .tableUri(CLUSTER_NAME + ".d1.t1")
          .tableLocation(INITIAL_TABLE_VERSION)
          .tableVersion(INITIAL_TABLE_VERSION)
          .tableUUID(UUID.randomUUID().toString())
          .tableProperties(buildTableProperties(TABLE_PROPS))
          .tableCreator(TEST_USER)
          .schema(HEALTH_SCHEMA_LITERAL)
          .policies(TABLE_POLICIES)
          .tableType(TableType.PRIMARY_TABLE)
          .timePartitioning(
              TimePartitionSpec.builder()
                  .columnName("timestampCol")
                  .granularity(TimePartitionSpec.Granularity.HOUR)
                  .build())
          .clustering(
              Arrays.asList(
                  ClusteringColumn.builder().columnName("id").build(),
                  ClusteringColumn.builder().columnName("name").build()))
          .build();

  public static final TableDto TABLE_DTO =
      TableModelConstants.buildTableDto(GET_TABLE_RESPONSE_BODY);

  public static final TableDto SHARED_TABLE_DTO =
      TABLE_DTO.toBuilder().policies(SHARED_TABLE_POLICIES).build();

  public static final TableDto STAGED_TABLE_DTO =
      TableDto.builder()
          .tableId(TABLE_DTO.getTableId())
          .databaseId(TABLE_DTO.getDatabaseId())
          .clusterId(TABLE_DTO.getClusterId())
          .tableUri(TABLE_DTO.getTableUri())
          .schema(TABLE_DTO.getSchema())
          .tableLocation(TABLE_DTO.getTableLocation())
          .tableVersion(INITIAL_TABLE_VERSION)
          .tableCreator(TABLE_DTO.getTableCreator())
          .timePartitioning(TABLE_DTO.getTimePartitioning())
          .tableProperties(TABLE_DTO.getTableProperties())
          .policies(TABLE_DTO.getPolicies())
          .stageCreate(true)
          .tableType(TableType.PRIMARY_TABLE)
          .build();

  public static final GetTableResponseBody GET_TABLE_RESPONSE_BODY_SAME_DB =
      GetTableResponseBody.builder()
          .tableId("t2")
          .databaseId("d1")
          .clusterId(CLUSTER_NAME)
          .tableUri(CLUSTER_NAME + ".d1.t2")
          .tableLocation(INITIAL_TABLE_VERSION)
          .tableVersion(String.valueOf(new Random().nextLong()))
          .tableCreator(TEST_USER)
          .tableUUID(UUID.randomUUID().toString())
          .schema(HEALTH_SCHEMA_LITERAL)
          .timePartitioning(
              TimePartitionSpec.builder()
                  .columnName("timestampCol")
                  .granularity(TimePartitionSpec.Granularity.HOUR)
                  .build())
          .clustering(
              Arrays.asList(
                  ClusteringColumn.builder().columnName("name").build(),
                  ClusteringColumn.builder().columnName("id").build()))
          .tableProperties(buildTableProperties(TABLE_PROPS))
          .policies(TABLE_POLICIES)
          .build();
  public static final TableDto TABLE_DTO_SAME_DB =
      TableModelConstants.buildTableDto(GET_TABLE_RESPONSE_BODY_SAME_DB);

  public static final GetTableResponseBody GET_TABLE_RESPONSE_BODY_DIFF_DB =
      GetTableResponseBody.builder()
          .tableId("t1")
          .databaseId("d2")
          .clusterId(CLUSTER_NAME)
          .tableUri(CLUSTER_NAME + ".d2.t1")
          .tableLocation(INITIAL_TABLE_VERSION)
          .tableVersion(String.valueOf(new Random().nextLong()))
          .tableCreator(TEST_USER)
          .tableUUID(UUID.randomUUID().toString())
          .schema(HEALTH_SCHEMA_LITERAL)
          .tableProperties(buildTableProperties(TABLE_PROPS))
          .policies(TABLE_POLICIES)
          .tableType(TableType.PRIMARY_TABLE)
          .timePartitioning(
              TimePartitionSpec.builder()
                  .columnName("timestampCol")
                  .granularity(TimePartitionSpec.Granularity.HOUR)
                  .build())
          .clustering(
              Arrays.asList(
                  ClusteringColumn.builder().columnName("name").build(),
                  ClusteringColumn.builder().columnName("id").build()))
          .build();
  public static final TableDto TABLE_DTO_DIFF_DB =
      TableModelConstants.buildTableDto(GET_TABLE_RESPONSE_BODY_DIFF_DB);

  public static final GetTableResponseBody GET_TABLE_RESPONSE_BODY_IDENTIFIER =
      GetTableResponseBody.builder().tableId("t1").databaseId("d1").build();

  public static final GetTableResponseBody GET_TABLE_RESPONSE_BODY_SAME_DB_IDENTIFIER =
      GetTableResponseBody.builder().tableId("t2").databaseId("d1").build();

  public static final CreateUpdateTableRequestBody
      CREATE_TABLE_REQUEST_BODY_WITHIN_SNAPSHOTS_REQUEST =
          CreateUpdateTableRequestBody.builder()
              .tableId("t1_snapshots")
              .databaseId("db1_snapshots")
              .clusterId(CLUSTER_NAME)
              .schema(HEALTH_SCHEMA_LITERAL)
              .tableProperties(buildTableProperties(TABLE_PROPS))
              .tableType(TableType.REPLICA_TABLE)
              .timePartitioning(
                  TimePartitionSpec.builder()
                      .columnName("ts")
                      .granularity(TimePartitionSpec.Granularity.DAY)
                      .build())
              .clustering(
                  Arrays.asList(
                      ClusteringColumn.builder().columnName("name").build(),
                      ClusteringColumn.builder().columnName("id").build()))
              .build();

  public static final CreateUpdateTableRequestBody CREATE_TABLE_REQUEST_BODY =
      CreateUpdateTableRequestBody.builder()
          .tableId("t1")
          .databaseId("create")
          .clusterId(CLUSTER_NAME)
          .schema(HEALTH_SCHEMA_LITERAL)
          .tableProperties(buildTableProperties(TABLE_PROPS))
          .policies(TABLE_POLICIES)
          .baseTableVersion(INITIAL_TABLE_VERSION)
          .build();

  public static final IcebergSnapshotsRequestBody ICEBERG_SNAPSHOTS_REQUEST_BODY =
      IcebergSnapshotsRequestBody.builder()
          .baseTableVersion("v1")
          .createUpdateTableRequestBody(CREATE_TABLE_REQUEST_BODY_WITHIN_SNAPSHOTS_REQUEST)
          .jsonSnapshots(Collections.singletonList("dummy"))
          .build();

  public static TableDto buildTableDto(GetTableResponseBody getTableResponseBody) {
    return TableDto.builder()
        .tableId(getTableResponseBody.getTableId())
        .databaseId(getTableResponseBody.getDatabaseId())
        .clusterId(getTableResponseBody.getClusterId())
        .tableUri(getTableResponseBody.getTableUri())
        .tableLocation(getTableResponseBody.getTableLocation())
        .tableVersion(getTableResponseBody.getTableLocation())
        .tableCreator(getTableResponseBody.getTableCreator())
        .tableUUID(getTableResponseBody.getTableUUID())
        .schema(getTableResponseBody.getSchema())
        .tableProperties(getTableResponseBody.getTableProperties())
        .timePartitioning(getTableResponseBody.getTimePartitioning())
        .clustering(getTableResponseBody.getClustering())
        .policies(getTableResponseBody.getPolicies())
        .tableType(getTableResponseBody.getTableType())
        .build();
  }

  public static CreateUpdateTableRequestBody buildCreateUpdateTableRequestBody(
      GetTableResponseBody getTableResponseBody) {
    return buildCreateUpdateTableRequestBody(getTableResponseBody, false);
  }

  public static CreateUpdateTableRequestBody buildCreateUpdateTableRequestBody(
      GetTableResponseBody getTableResponseBody, boolean stageCreate) {
    return CreateUpdateTableRequestBody.builder()
        .tableId(getTableResponseBody.getTableId())
        .databaseId(getTableResponseBody.getDatabaseId())
        .clusterId(getTableResponseBody.getClusterId())
        .schema(getTableResponseBody.getSchema())
        .timePartitioning(getTableResponseBody.getTimePartitioning())
        .clustering(getTableResponseBody.getClustering())
        .tableProperties(getTableResponseBody.getTableProperties())
        .policies(getTableResponseBody.getPolicies())
        .baseTableVersion(getTableResponseBody.getTableLocation())
        .stageCreate(stageCreate)
        .tableType(getTableResponseBody.getTableType())
        .build();
  }

  /** Reconstruct the {@link CreateUpdateTableRequestBody} from raw {@link MvcResult}'s string. */
  public static CreateUpdateTableRequestBody buildCreateUpdateTableRequestBody(
      MvcResult mvcResult, boolean stageCreate) throws UnsupportedEncodingException {
    String content = mvcResult.getResponse().getContentAsString();
    return CreateUpdateTableRequestBody.builder()
        .tableId(JsonPath.read(content, "$.tableId"))
        .databaseId(JsonPath.read(content, "$.databaseId"))
        .clusterId(JsonPath.read(content, "$.clusterId"))
        .schema(JsonPath.read(content, "$.schema"))
        .tableProperties(JsonPath.read(content, "$.tableProperties"))
        .timePartitioning(
            new TimePartitionSpecConverter()
                .convertToEntityAttribute(JsonPath.read(content, "$.timePartitioning").toString()))
        .policies(
            new PoliciesSpecConverter()
                .convertToEntityAttribute(JsonPath.read(content, "$.policies").toString()))
        .clustering(
            new ClusteringSpecConverter()
                .convertToEntityAttribute(JsonPath.read(content, "$.clustering").toString()))
        .stageCreate(stageCreate)
        .baseTableVersion(JsonPath.read(content, "$.tableLocation"))
        .build();
  }

  public static CreateUpdateTableRequestBody buildCreateUpdateTableRequestBody(MvcResult mvcResult)
      throws UnsupportedEncodingException {
    return buildCreateUpdateTableRequestBody(mvcResult, false);
  }

  /** Re-construct the {@link GetTableResponseBody} from raw {@link MvcResult}'s string */
  public static GetTableResponseBody buildGetTableResponseBody(MvcResult mvcResult)
      throws UnsupportedEncodingException {
    String content = mvcResult.getResponse().getContentAsString();
    return GetTableResponseBody.builder()
        .tableId(JsonPath.read(content, "$.tableId"))
        .databaseId(JsonPath.read(content, "$.databaseId"))
        .tableUri(JsonPath.read(content, "$.tableUri"))
        .tableUUID(JsonPath.read(content, "$.tableUUID"))
        .clusterId(JsonPath.read(content, "$.clusterId"))
        .schema(JsonPath.read(content, "$.schema"))
        .tableLocation(JsonPath.read(content, "$.tableLocation"))
        .tableVersion(JsonPath.read(content, "$.tableVersion"))
        .tableCreator(JsonPath.read(content, "$.tableCreator"))
        .tableProperties(JsonPath.read(content, "$.tableProperties"))
        .lastModifiedTime(JsonPath.read(content, "$.lastModifiedTime"))
        .creationTime(JsonPath.read(content, "$.creationTime"))
        .timePartitioning(
            new TimePartitionSpecConverter()
                .convertToEntityAttribute(JsonPath.read(content, "$.timePartitioning").toString()))
        .clustering(
            new ClusteringSpecConverter()
                .convertToEntityAttribute(JsonPath.read(content, "$.clustering").toString()))
        .policies(
            new PoliciesSpecConverter()
                .convertToEntityAttribute(JsonPath.read(content, "$.policies").toString()))
        .build();
  }

  /**
   * Re-construct the {@link GetTableResponseBody} from raw {@link MvcResult}'s string and {@link
   * GetTableResponseBody} as a way to set field as wanted
   */
  public static GetTableResponseBody buildGetTableResponseBody(
      MvcResult mvcResult, GetTableResponseBody getTableResponseBody)
      throws UnsupportedEncodingException {
    String content = mvcResult.getResponse().getContentAsString();
    return GetTableResponseBody.builder()
        .tableId(
            getTableResponseBody.getTableId() == null
                ? JsonPath.read(content, "$.tableId")
                : getTableResponseBody.getTableId())
        .databaseId(
            getTableResponseBody.getDatabaseId() == null
                ? JsonPath.read(content, "$.databaseId")
                : getTableResponseBody.getDatabaseId())
        .tableUri(
            getTableResponseBody.getTableUri() == null
                ? JsonPath.read(content, "$.tableUri")
                : getTableResponseBody.getTableUri())
        .tableUUID(
            getTableResponseBody.getTableUUID() == null
                ? JsonPath.read(content, "$.tableUUID")
                : getTableResponseBody.getTableUUID())
        .clusterId(
            getTableResponseBody.getClusterId() == null
                ? JsonPath.read(content, "$.clusterId")
                : getTableResponseBody.getClusterId())
        .schema(
            getTableResponseBody.getSchema() == null
                ? JsonPath.read(content, "$.schema")
                : getTableResponseBody.getSchema())
        .tableLocation(
            getTableResponseBody.getTableLocation() == null
                ? JsonPath.read(content, "$.tableLocation")
                : getTableResponseBody.getTableLocation())
        .tableVersion(
            getTableResponseBody.getTableVersion() == null
                ? JsonPath.read(content, "$.tableVersion")
                : getTableResponseBody.getTableVersion())
        .tableCreator(
            getTableResponseBody.getTableVersion() == null
                ? JsonPath.read(content, "$.tableCreator")
                : getTableResponseBody.getTableCreator())
        .lastModifiedTime(
            getTableResponseBody.getLastModifiedTime() == 0
                ? JsonPath.read(content, "$.lastModifiedTime")
                : getTableResponseBody.getLastModifiedTime())
        .creationTime(
            getTableResponseBody.getCreationTime() == 0
                ? JsonPath.read(content, "$.creationTime")
                : getTableResponseBody.getCreationTime())
        .tableProperties(
            getTableResponseBody.getTableProperties() == null
                ? JsonPath.read(content, "$.tableProperties")
                : getTableResponseBody.getTableProperties())
        .timePartitioning(
            getTableResponseBody.getTimePartitioning() == null
                    && JsonPath.read(content, "$.timePartitioning") != null
                ? new Gson()
                    .fromJson(
                        JsonPath.read(content, "$.timePartitioning").toString(),
                        TimePartitionSpec.class)
                : getTableResponseBody.getTimePartitioning())
        .clustering(
            getTableResponseBody.getClustering() == null
                ? Arrays.asList(
                    new Gson()
                        .fromJson(
                            JsonPath.read(content, "$.clustering").toString(),
                            ClusteringColumn[].class))
                : getTableResponseBody.getClustering())
        .policies(
            getTableResponseBody.getPolicies() == null
                ? new Gson()
                    .fromJson(JsonPath.read(content, "$.policies").toString(), Policies.class)
                : getTableResponseBody.getPolicies())
        .build();
  }

  public static CreateUpdateTableRequestBody buildCreateUpdateTableRequestBody(TableDto tableDto) {
    CreateUpdateTableRequestBody.CreateUpdateTableRequestBodyBuilder
        createUpdateTableRequestBodyBuilder =
            CreateUpdateTableRequestBody.builder()
                .tableId(tableDto.getTableId())
                .databaseId(tableDto.getDatabaseId())
                .clusterId(tableDto.getClusterId())
                .schema(tableDto.getSchema())
                .tableProperties(tableDto.getTableProperties())
                .timePartitioning(tableDto.getTimePartitioning())
                .clustering(tableDto.getClustering())
                .baseTableVersion(tableDto.getTableLocation())
                .tableType(tableDto.getTableType())
                .policies(tableDto.getPolicies());
    if (tableDto.isStageCreate()) {
      createUpdateTableRequestBodyBuilder.stageCreate(true);
    }
    return createUpdateTableRequestBodyBuilder.build();
  }

  public static GetTableResponseBody buildGetTableResponseBodyWithPolicy(
      GetTableResponseBody getTableResponseBody, Policies policies) {
    return GetTableResponseBody.builder()
        .tableId(getTableResponseBody.getTableId())
        .databaseId(getTableResponseBody.getDatabaseId())
        .tableUri(getTableResponseBody.getTableUri())
        .tableUUID(getTableResponseBody.getTableUri())
        .schema(getTableResponseBody.getSchema())
        .tableLocation(getTableResponseBody.getTableLocation())
        .tableVersion(getTableResponseBody.getTableVersion())
        .tableCreator(getTableResponseBody.getTableCreator())
        .clusterId(getTableResponseBody.getClusterId())
        .timePartitioning(getTableResponseBody.getTimePartitioning())
        .clustering(getTableResponseBody.getClustering())
        .policies(policies)
        .tableProperties(getTableResponseBody.getTableProperties())
        .build();
  }

  private static Map<String, String> buildTableProperties(Map<String, String> tblprops) {
    tblprops.put(TableProperties.DEFAULT_FILE_FORMAT, "orc");
    tblprops.put(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "false");
    tblprops.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "100");
    return tblprops;
  }
}
