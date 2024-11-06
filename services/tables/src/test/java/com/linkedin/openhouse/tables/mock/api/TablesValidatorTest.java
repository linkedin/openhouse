package com.linkedin.openhouse.tables.mock.api;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static com.sun.org.apache.xalan.internal.xsltc.compiler.Constants.CHARACTERS;
import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.RetentionColumnPattern;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Transform;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.common.TableType;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class TablesValidatorTest {

  @Autowired private TablesApiValidator tablesApiValidator;

  @Test
  public void validateGetTableSuccess() {
    assertDoesNotThrow(() -> tablesApiValidator.validateGetTable("d", "t"));
  }

  private String generateId(int length) {
    final Random random = new SecureRandom();
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }

  @Test
  public void validateGetTableSpecialCharacter() {
    assertThrows(
        RequestValidationFailureException.class,
        () -> tablesApiValidator.validateGetTable("%%", "t"));

    assertThrows(
        RequestValidationFailureException.class,
        () -> tablesApiValidator.validateGetTable("d", ";;"));

    assertThrows(
        RequestValidationFailureException.class,
        () -> tablesApiValidator.validateGetTable("test-table", ";;"));
  }

  @Test
  public void validateGetTableWithEmptyString() {
    assertThrows(
        RequestValidationFailureException.class,
        () -> tablesApiValidator.validateGetTable("", "t"));
    assertThrows(
        RequestValidationFailureException.class,
        () -> tablesApiValidator.validateGetTable("d", ""));
  }

  @Test
  public void validateCreateTableSuccess() {
    assertDoesNotThrow(
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "d",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateTooLongTableOrDbId() {
    String tooLongDbId = generateId(130);
    String tooLongTblId = generateId(140);

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                tooLongDbId,
                CreateUpdateTableRequestBody.builder()
                    .databaseId(tooLongDbId)
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "db",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d2")
                    .tableId(tooLongTblId)
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  // TODO: Add a case for schema validation error.

  @Test
  public void validateCreateTableRequestParamNotMatchingRequestBody() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "d1",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d2")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateIllegalTableId() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "d1",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d2")
                    .tableId("t-9") /* hyphen not allowed */
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateCreateTableRequestParamWithValidPoliciesJson() {
    assertDoesNotThrow(
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "d",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .timePartitioning(
                        TimePartitionSpec.builder()
                            .columnName("timestamp")
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .baseTableVersion("base")
                    .policies(Policies.builder().retention(RETENTION_POLICY).build())
                    .build()));
  }

  @Test
  public void validateCreateTableRequestParamWithMissingDaysInPoliciesObject() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "d",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .timePartitioning(
                        TimePartitionSpec.builder()
                            .columnName("timestamp")
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .policies(Policies.builder().retention(Retention.builder().build()).build())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateCreateTableRequestParamWithMissingColNamesInRetentionObject() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "d",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .timePartitioning(
                        TimePartitionSpec.builder()
                            .columnName("timestamp")
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .policies(
                        Policies.builder()
                            .retention(
                                Retention.builder()
                                    .columnPattern(
                                        RetentionColumnPattern.builder().pattern("yyyy").build())
                                    .build())
                            .build())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateCreateTableRequestParamWithInvalidDaysInPoliciesObject() {
    List<Integer> daysValues = new ArrayList<Integer>() {};
    /*
    negative day values should not be accepted in retention days
     */
    daysValues.add(-10);
    daysValues.add(0);
    for (int d : daysValues) {
      assertThrows(
          RequestValidationFailureException.class,
          () ->
              tablesApiValidator.validateCreateTable(
                  "c",
                  "d",
                  CreateUpdateTableRequestBody.builder()
                      .databaseId("d")
                      .tableId("t")
                      .clusterId("c")
                      .schema(HEALTH_SCHEMA_LITERAL)
                      .tableProperties(ImmutableMap.of())
                      .timePartitioning(
                          TimePartitionSpec.builder()
                              .granularity(TimePartitionSpec.Granularity.DAY)
                              .columnName("timestamp")
                              .build())
                      .policies(
                          Policies.builder()
                              .retention(
                                  Retention.builder()
                                      .count(d)
                                      .granularity(TimePartitionSpec.Granularity.DAY)
                                      .build())
                              .build())
                      .baseTableVersion("base")
                      .build()));
    }
  }

  @Test
  public void validateCreateTableSpecialCharacterAndEmpty() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "%%",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("%%")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "d1",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d1")
                    .tableId("^`")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                ":'",
                "d1",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d1")
                    .tableId("t")
                    .clusterId(":'")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "",
                "d1",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("")
                    .tableId("")
                    .clusterId("")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "",
                "",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("")
                    .tableId("")
                    .clusterId("")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateUpdateTableSuccess() {
    tablesApiValidator.validateUpdateTable(
        "c",
        "d",
        "t",
        CreateUpdateTableRequestBody.builder()
            .databaseId("d")
            .tableId("t")
            .clusterId("c")
            .schema(HEALTH_SCHEMA_LITERAL)
            .baseTableVersion("base")
            .tableProperties(ImmutableMap.of())
            .build());
  }

  @Test
  public void validateCreateTableWithEmptyTblProps() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateTable(
                "c",
                "d1",
                "t",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d2")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(null)
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateCreateTableForStagedTable() {
    assertDoesNotThrow(
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "d1",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d1")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .stageCreate(true)
                    .build()));
  }

  @Test
  public void validateCreateTableRequestMoreThanMaxColumns() {
    List<ClusteringColumn> clusteringColumns =
        Arrays.asList(
            ClusteringColumn.builder().columnName("id").build(),
            ClusteringColumn.builder().columnName("name").build(),
            ClusteringColumn.builder().columnName("country").build(),
            ClusteringColumn.builder().columnName("city").build(),
            ClusteringColumn.builder().columnName("county").build());

    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateCreateTable(
                    "c",
                    "dC",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("dC")
                        .tableId("t")
                        .clusterId("c")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .baseTableVersion("base")
                        .clustering(clusteringColumns)
                        .tableProperties(ImmutableMap.of())
                        .build()));
    Assertions.assertEquals(
        String.format(
            "table dC.t has %s clustering columns specified, max clustering columns supported is %s",
            clusteringColumns.size(), MAX_ALLOWED_CLUSTERING_COLUMNS),
        requestValidationFailureException.getMessage());

    // No exception for {MAX_ALLOWED_CLUSTERING_COLUMNS} number of columns
    List<ClusteringColumn> boderlineList =
        clusteringColumns.subList(0, MAX_ALLOWED_CLUSTERING_COLUMNS - 1);
    assertDoesNotThrow(
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "dC",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("dC")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .baseTableVersion("base")
                    .clustering(boderlineList)
                    .tableProperties(ImmutableMap.of())
                    .build()));
  }

  @Test
  public void validateCreateTableRequestOneColNull() {
    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateCreateTable(
                    "c",
                    "dC",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("dC")
                        .tableId("t")
                        .clusterId("c")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .clustering(
                            Arrays.asList(
                                ClusteringColumn.builder().columnName("id").build(),
                                ClusteringColumn.builder().columnName(null).build()))
                        .tableProperties(ImmutableMap.of())
                        .baseTableVersion("base")
                        .build()));
    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains(
                "CreateUpdateTableRequestBody.clustering[1].columnName : columnName cannot be null"));
    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains(
                "CreateUpdateTableRequestBody.clustering[1].columnName : columnName cannot be empty"));
  }

  @Test
  public void validateCreateTableRequestNullCol() {
    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateCreateTable(
                    "c",
                    "dC",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("dC")
                        .tableId("t")
                        .clusterId("c")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .clustering(
                            Arrays.asList(
                                ClusteringColumn.builder().columnName("name").build(), null))
                        .tableProperties(ImmutableMap.of())
                        .baseTableVersion("base")
                        .build()));
    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains("table dC.t clustering[1] : cannot be null"));
  }

  @Test
  public void validateCreateTableRequestNullTransformType() {
    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateCreateTable(
                    "c",
                    "dC",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("dC")
                        .tableId("t")
                        .clusterId("c")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .clustering(
                            Arrays.asList(
                                ClusteringColumn.builder()
                                    .columnName("name")
                                    .transform(Transform.builder().build())
                                    .build()))
                        .tableProperties(ImmutableMap.of())
                        .baseTableVersion("base")
                        .build()));
    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains(
                "CreateUpdateTableRequestBody.clustering[0].transform.transformType : transformType cannot be null"));
  }

  @Test
  public void validateCreateTableRequestTransformEmptyParams() {
    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateCreateTable(
                    "c",
                    "dC",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("dC")
                        .tableId("t")
                        .clusterId("c")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .clustering(
                            Arrays.asList(
                                ClusteringColumn.builder()
                                    .columnName("name")
                                    .transform(
                                        Transform.builder()
                                            .transformType(Transform.TransformType.TRUNCATE)
                                            .build())
                                    .build()))
                        .tableProperties(ImmutableMap.of())
                        .baseTableVersion("base")
                        .build()));
    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains("TRUNCATE transform: parameters can not be empty"));
  }

  @Test
  public void validateCreateTableRequestTransformMoreThanOneParams() {
    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateCreateTable(
                    "c",
                    "dC",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("dC")
                        .tableId("t")
                        .clusterId("c")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .clustering(
                            Arrays.asList(
                                ClusteringColumn.builder()
                                    .columnName("name")
                                    .transform(
                                        Transform.builder()
                                            .transformType(Transform.TransformType.TRUNCATE)
                                            .transformParams(Arrays.asList("1", "2"))
                                            .build())
                                    .build()))
                        .tableProperties(ImmutableMap.of())
                        .baseTableVersion("base")
                        .build()));
    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains("TRUNCATE transform: cannot have more than one parameter"));
  }

  @Test
  public void validateCreateTableRequestTransformNonNumericParams() {
    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateCreateTable(
                    "c",
                    "dC",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("dC")
                        .tableId("t")
                        .clusterId("c")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .clustering(
                            Arrays.asList(
                                ClusteringColumn.builder()
                                    .columnName("name")
                                    .transform(
                                        Transform.builder()
                                            .transformType(Transform.TransformType.TRUNCATE)
                                            .transformParams(Arrays.asList("x"))
                                            .build())
                                    .build()))
                        .tableProperties(ImmutableMap.of())
                        .baseTableVersion("base")
                        .build()));
    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains("TRUNCATE transform: parameters must be numeric string"));
  }

  @Test
  public void validateUpdateTableRequestParamNotMatchingRequestBody() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateTable(
                "c",
                "d1",
                "t",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d2")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateTable(
                "c",
                "d1",
                "t1",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d1")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateUpdateTableSpecialCharacterAndEmpty() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateTable(
                "c",
                "%%",
                "t",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("%%")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateTable(
                "c",
                "d1",
                "^`",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d1")
                    .tableId("^`")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateTable(
                "c",
                "d1",
                "t",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d1")
                    .tableId("t")
                    .clusterId(":'")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateTable(
                "c",
                "d1",
                "t",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("")
                    .tableId("")
                    .clusterId("")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateUpdateTableForStagedCreate() {
    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateUpdateTable(
                    "c",
                    "d1",
                    "t",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("d1")
                        .tableId("t")
                        .clusterId("c")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .tableProperties(ImmutableMap.of())
                        .stageCreate(true)
                        .baseTableVersion("base")
                        .build()));
    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains(
                "Staged table d1.t cannot be created with PUT request, please use the POST apis"));
  }

  @Test
  public void validateDeleteTableSuccess() {
    tablesApiValidator.validateDeleteTable("d", "t");
  }

  @Test
  public void validateDeleteTableSpecialCharacter() {
    assertThrows(
        RequestValidationFailureException.class,
        () -> tablesApiValidator.validateDeleteTable("%%", "t"));

    assertThrows(
        RequestValidationFailureException.class,
        () -> tablesApiValidator.validateDeleteTable("d", ";;"));
  }

  @Test
  public void validateCreateTableRequestParamWithValidReplicationInPoliciesObject() {
    assertDoesNotThrow(
        () ->
            tablesApiValidator.validateCreateTable(
                "c",
                "d",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .timePartitioning(
                        TimePartitionSpec.builder()
                            .columnName("timestamp")
                            .granularity(TimePartitionSpec.Granularity.HOUR)
                            .build())
                    .policies(
                        Policies.builder()
                            .replication(
                                Replication.builder()
                                    .config(
                                        Arrays.asList(
                                            ReplicationConfig.builder()
                                                .destination("z")
                                                .interval("12H")
                                                .build()))
                                    .build())
                            .build())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateUpdateAclPoliciesSpecialCharacter() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateAclPolicies(
                "%%",
                "t",
                UpdateAclPoliciesRequestBody.builder()
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .principal("DUMMY_USER")
                    .role("AclEditor")
                    .build()));
  }

  @Test
  public void validateUpdateAclPoliciesEmptyValues() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateAclPolicies(
                "d",
                "t1",
                UpdateAclPoliciesRequestBody.builder()
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .principal("")
                    .role("AclEditor")
                    .build()));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateAclPolicies(
                "d",
                "t1",
                UpdateAclPoliciesRequestBody.builder()
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .principal("DUMMY_USER")
                    .role("")
                    .build()));
  }

  @Test
  public void validateCreateTableClusterIdMismatch() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "local-cluster",
                "d",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateUpdateTableClusterIdMismatch() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateTable(
                "local-cluster",
                "d",
                "t",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .clusterId("c")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateCreateTableEmptySchema() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateCreateTable(
                "local-cluster",
                "d",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .clusterId("local-cluster")
                    .schema("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateUpdateTableEmptySchema() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            tablesApiValidator.validateUpdateTable(
                "local-cluster",
                "d",
                "t",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .clusterId("local-cluster")
                    .schema("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
                    .tableProperties(ImmutableMap.of())
                    .baseTableVersion("base")
                    .build()));
  }

  @Test
  public void validateCreateTableWithReplicaTableType() {
    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateCreateTable(
                    "local-cluster",
                    "d",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("d")
                        .tableId("t")
                        .clusterId("local-cluster")
                        .baseTableVersion("INITIAL_VERSION")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .tableProperties(ImmutableMap.of())
                        .tableType(TableType.REPLICA_TABLE)
                        .build()));

    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains(
                String.format(
                    "tableProperties should contain a valid %s property for tableType: %s",
                    CatalogConstants.OPENHOUSE_UUID_KEY, TableType.REPLICA_TABLE)));
  }

  @Test
  public void validateCreateTableWithReplicaTableTypeInvalidUUID() {
    String invalidUUID = "5a6bfe92-0ed6-4717-8cd6-c01a4ec87dabb-we";
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(CatalogConstants.OPENHOUSE_UUID_KEY, invalidUUID);
    RequestValidationFailureException requestValidationFailureException =
        assertThrows(
            RequestValidationFailureException.class,
            () ->
                tablesApiValidator.validateCreateTable(
                    "local-cluster",
                    "d",
                    CreateUpdateTableRequestBody.builder()
                        .databaseId("d")
                        .tableId("t")
                        .clusterId("local-cluster")
                        .baseTableVersion("INITIAL_VERSION")
                        .schema(HEALTH_SCHEMA_LITERAL)
                        .tableProperties(tableProperties)
                        .tableType(TableType.REPLICA_TABLE)
                        .build()));

    Assertions.assertTrue(
        requestValidationFailureException
            .getMessage()
            .contains(String.format("UUID: provided %s is an invalid UUID string", invalidUUID)));
  }

  @Test
  public void validateCreateTableWithPrimaryTableType() {
    assertDoesNotThrow(
        () ->
            tablesApiValidator.validateCreateTable(
                "local-cluster",
                "d",
                CreateUpdateTableRequestBody.builder()
                    .databaseId("d")
                    .tableId("t")
                    .baseTableVersion("INITIAL_VERSION")
                    .clusterId("local-cluster")
                    .schema(HEALTH_SCHEMA_LITERAL)
                    .tableProperties(ImmutableMap.of())
                    .tableType(TableType.PRIMARY_TABLE)
                    .build()));
  }
}
