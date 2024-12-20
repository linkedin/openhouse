package com.linkedin.openhouse.tables.mock.mapper;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;
import static com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec.Granularity.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Transform;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PartitionSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableModelConstants;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.FileSystemUtils;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PartitionSpecMapperTest {

  @Autowired protected PartitionSpecMapper tablesMapper;

  private Path tempDir;

  private Schema schema;

  private String timePartitioningColumn;

  private List<String> clusteringColumns;

  @BeforeAll
  public void setup() throws IOException {
    tempDir = Files.createTempDirectory(this.getClass().getSimpleName());
    schema = getSchemaFromSchemaJson(HEALTH_SCHEMA_LITERAL);
    timePartitioningColumn =
        schema.columns().stream()
            .filter(x -> x.type().typeId().equals(Type.TypeID.TIMESTAMP))
            .findFirst()
            .map(Types.NestedField::name)
            .get();
    clusteringColumns =
        schema.columns().stream()
            .filter(
                x ->
                    x.type().typeId().equals(Type.TypeID.STRING)
                        || x.type().typeId().equals(Type.TypeID.INTEGER)
                        || x.type().typeId().equals(Type.TypeID.LONG))
            .map(Types.NestedField::name)
            .collect(Collectors.toList());
  }

  @AfterAll
  public void tearDown() throws IOException {
    FileSystemUtils.deleteRecursively(tempDir);
  }

  @AfterEach
  private void recreateTempDir() throws IOException {
    FileSystemUtils.deleteRecursively(tempDir);
    tempDir = Files.createTempDirectory(this.getClass().getSimpleName());
  }

  @Test
  public void testToTimePartitionSpec() {
    for (String transform : ImmutableList.of("year", "month", "day", "hour")) {
      TimePartitionSpec timePartitionSpec =
          tablesMapper.toTimePartitionSpec(
              createDummyIcebergTable(timePartitioningColumn, transform));
      Assertions.assertEquals(timePartitionSpec.getGranularity(), valueOf(transform.toUpperCase()));
      Assertions.assertEquals(timePartitionSpec.getColumnName(), timePartitioningColumn);
    }

    for (String transform : ImmutableList.of("bucket[10]", "identity", "void")) {
      Assertions.assertThrows(
          IllegalStateException.class,
          () ->
              tablesMapper.toTimePartitionSpec(
                  createDummyIcebergTable(timePartitioningColumn, transform)));
    }
  }

  @Test
  public void testToClusteringSpec() throws IOException {
    for (String transform : ImmutableList.of("identity", "truncate[10]")) {
      Map<String, String> colTransformMap = new HashMap<>();
      colTransformMap.put(timePartitioningColumn, "day");
      clusteringColumns.stream().forEach(x -> colTransformMap.put(x, transform));
      List<ClusteringColumn> clusteringSpecs =
          tablesMapper.toClusteringSpec(createDummyIcebergTable(colTransformMap));
      assertThat(
          clusteringSpecs.stream().map(x -> x.getColumnName()).collect(Collectors.toList()),
          containsInAnyOrder(clusteringColumns.toArray()));
    }
    recreateTempDir();
    for (String transform : ImmutableList.of("bucket[10]", "void")) {
      Map<String, String> colTransformMap = new HashMap<>();
      colTransformMap.put(timePartitioningColumn, "day");
      clusteringColumns.stream().forEach(x -> colTransformMap.put(x, transform));
      Assertions.assertThrows(
          IllegalStateException.class,
          () -> tablesMapper.toClusteringSpec(createDummyIcebergTable(colTransformMap)));
    }
  }

  @Test
  public void testToPartitionSpecClusteringMoreThanMax() {
    Schema tmpSchema = getSchemaFromSchemaJson(UNHEALTHY_CLUSTER_SCHEMA_LITERAL);
    List<String> tmpClustering =
        tmpSchema.columns().stream()
            .filter(x -> x.type().typeId().equals(Type.TypeID.STRING))
            .map(Types.NestedField::name)
            .collect(Collectors.toList());
    TableDto tableDto =
        TableModelConstants.buildTableDto(
            GET_TABLE_RESPONSE_BODY
                .toBuilder()
                .clustering(
                    tmpClustering.stream()
                        .map(x -> ClusteringColumn.builder().columnName(x).build())
                        .collect(Collectors.toList()))
                .build());
    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () -> tablesMapper.toPartitionSpec(tableDto),
        "Max allowed clustering columns supported are 3, specified are 4");
  }

  @Test
  public void testToPartitionSpecClusteringColNotInSchema() {
    TableDto tableDto =
        TableModelConstants.buildTableDto(
            GET_TABLE_RESPONSE_BODY
                .toBuilder()
                .clustering(
                    Arrays.asList(ClusteringColumn.builder().columnName("notInSchema").build()))
                .build());
    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () -> tablesMapper.toPartitionSpec(tableDto),
        "Adding partition spec failed:Clustering column notInSchema not found in the schema");
  }

  @Test
  public void testToPartitionSpecClusteringColNotAllowedType() {
    TableDto tableDto =
        TableModelConstants.buildTableDto(
            GET_TABLE_RESPONSE_BODY
                .toBuilder()
                .clustering(Arrays.asList(ClusteringColumn.builder().columnName("stats").build()))
                .build());
    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () -> tablesMapper.toPartitionSpec(tableDto),
        "Adding partition spec failed:Column name stats is type DOUBLE is not supported clustering type");
  }

  @Test
  public void testToPartitionSpecClusteringOnly() {
    TableDto tableDto =
        TableModelConstants.buildTableDto(
            GET_TABLE_RESPONSE_BODY
                .toBuilder()
                .clustering(
                    clusteringColumns.stream()
                        .map(x -> ClusteringColumn.builder().columnName(x).build())
                        .collect(Collectors.toList()))
                .timePartitioning(null)
                .build());

    PartitionSpec partitionSpec = tablesMapper.toPartitionSpec(tableDto);
    Assertions.assertEquals(4, partitionSpec.fields().size());
    // Make sure only clustering columns have been captured in the Iceberg
    // partition spec.
    assertThat(
        partitionSpec.fields().stream()
            .map(x -> schema.findField(x.sourceId()).name())
            .collect(Collectors.toList()),
        containsInAnyOrder(clusteringColumns.toArray()));
    // Make sure clustering columns have appropriate transforms.
    assertThat(
        partitionSpec.fields().stream()
            .map(
                x -> {
                  Type.TypeID typeID = schema.findField(x.sourceId()).type().typeId();
                  switch (typeID) {
                    case STRING:
                    case INTEGER:
                    case LONG:
                      return "identity".equals(x.transform().toString());
                    default:
                      return false;
                  }
                })
            .collect(Collectors.toList()),
        everyItem(is(oneOf(true))));
  }

  @Test
  public void testToPartitionSpecTimePartitioningAndClustering() {
    for (TimePartitionSpec.Granularity granularity : ImmutableList.of(DAY, MONTH, YEAR, HOUR)) {
      TableDto tableDto =
          TableModelConstants.buildTableDto(
              GET_TABLE_RESPONSE_BODY
                  .toBuilder()
                  .timePartitioning(
                      TimePartitionSpec.builder()
                          .columnName(timePartitioningColumn)
                          .granularity(granularity)
                          .build())
                  .clustering(
                      clusteringColumns.stream()
                          .map(x -> ClusteringColumn.builder().columnName(x).build())
                          .collect(Collectors.toList()))
                  .build());
      PartitionSpec partitionSpec = tablesMapper.toPartitionSpec(tableDto);
      Assertions.assertEquals(5, partitionSpec.fields().size());
      // Make sure both partitioning and clustering columns have been captured in the Iceberg
      // partition spec.
      assertThat(
          partitionSpec.fields().stream()
              .map(x -> schema.findField(x.sourceId()).name())
              .collect(Collectors.toList()),
          containsInAnyOrder(
              Stream.concat(
                      Arrays.asList(timePartitioningColumn).stream(), clusteringColumns.stream())
                  .collect(Collectors.toList())
                  .toArray()));
      // Make sure both partitioning and clustering columns have appropriate transforms.
      assertThat(
          partitionSpec.fields().stream()
              .map(
                  x -> {
                    Type.TypeID typeID = schema.findField(x.sourceId()).type().typeId();
                    switch (typeID) {
                      case TIMESTAMP:
                        return x.transform().toString().equals(granularity.name().toLowerCase());
                      case STRING:
                      case INTEGER:
                      case LONG:
                        return "identity".equals(x.transform().toString());
                      default:
                        return false;
                    }
                  })
              .collect(Collectors.toList()),
          everyItem(is(oneOf(true))));
    }

    PartitionSpec partitionSpec =
        tablesMapper.toPartitionSpec(
            TableModelConstants.buildTableDto(
                GET_TABLE_RESPONSE_BODY
                    .toBuilder()
                    .timePartitioning(null)
                    .clustering(null)
                    .build()));
    Assertions.assertTrue(partitionSpec.fields().isEmpty());
    Assertions.assertTrue(partitionSpec.isUnpartitioned());
  }

  @Test
  public void testToPartitionSpecTransform() {
    TableDto tableDto =
        TableModelConstants.buildTableDto(
            GET_TABLE_RESPONSE_BODY
                .toBuilder()
                .clustering(
                    clusteringColumns.stream()
                        .map(
                            x ->
                                ClusteringColumn.builder()
                                    .columnName(x)
                                    .transform(
                                        Transform.builder()
                                            .transformType(Transform.TransformType.TRUNCATE)
                                            .transformParams(Arrays.asList("10"))
                                            .build())
                                    .build())
                        .collect(Collectors.toList()))
                .timePartitioning(null)
                .build());

    PartitionSpec partitionSpec = tablesMapper.toPartitionSpec(tableDto);
    // Make sure clustering columns have appropriate transforms.
    assertThat(
        partitionSpec.fields().stream()
            .map(
                x -> {
                  Type.TypeID typeID = schema.findField(x.sourceId()).type().typeId();
                  switch (typeID) {
                    case STRING:
                    case INTEGER:
                    case LONG:
                      return "truncate[10]".equals(x.transform().toString());
                    default:
                      return false;
                  }
                })
            .collect(Collectors.toList()),
        everyItem(is(oneOf(true))));
  }

  @Test
  public void testToClusteringSpecTransform() throws IOException {
    Map<String, String> colTransformMap = new HashMap<>();
    clusteringColumns.stream().forEach(x -> colTransformMap.put(x, "identity"));
    List<ClusteringColumn> clusteringSpecsIdentity =
        tablesMapper.toClusteringSpec(createDummyIcebergTable(colTransformMap));
    // Make sure identity transform can be converted appropriately
    Assertions.assertEquals(
        4,
        clusteringSpecsIdentity.stream()
            .filter(x -> x.getTransform() == null)
            .collect(Collectors.toList())
            .size());

    clusteringColumns.stream().forEach(x -> colTransformMap.put(x, "truncate[10]"));
    List<ClusteringColumn> clusteringSpecsTruncate =
        tablesMapper.toClusteringSpec(createDummyIcebergTable(colTransformMap));
    // Make sure truncate transform can be converted appropriately
    Assertions.assertEquals(
        4,
        clusteringSpecsTruncate.stream()
            .filter(x -> x.getTransform().getTransformType() == Transform.TransformType.TRUNCATE)
            .collect(Collectors.toList())
            .size());
    Assertions.assertEquals(
        4,
        clusteringSpecsTruncate.stream()
            .filter(x -> x.getTransform().getTransformParams().get(0).equals("10"))
            .collect(Collectors.toList())
            .size());
  }

  private Table createDummyIcebergTable(Map<String, String> columnTransformMap) {
    PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
    columnTransformMap.entrySet().stream()
        .forEach(x -> createDummyIcebergTable(x.getKey(), x.getValue(), partitionSpecBuilder));
    return new HadoopTables()
        .create(
            schema,
            partitionSpecBuilder.build(),
            tempDir.resolve(partitionSpecBuilder.build().toString()).toString());
  }

  private Table createDummyIcebergTable(String columnName, String transform) {
    PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
    createDummyIcebergTable(columnName, transform, partitionSpecBuilder);
    return new HadoopTables()
        .create(
            schema,
            partitionSpecBuilder.build(),
            tempDir.resolve(partitionSpecBuilder.build().toString()).toString());
  }

  private void createDummyIcebergTable(
      String columnName, String transform, PartitionSpec.Builder partitionSpecBuilder) {
    Pattern hasWidth = Pattern.compile("(\\w+)\\[(\\d+)\\]");
    Matcher widthMatcher = hasWidth.matcher(transform);
    if (widthMatcher.matches()) {
      String name = widthMatcher.group(1);
      int parsedWidth = Integer.parseInt(widthMatcher.group(2));
      if ("truncate".equals(name)) {
        partitionSpecBuilder.truncate(columnName, parsedWidth);
        return;
      } else if ("bucket".equals(name)) {
        partitionSpecBuilder.bucket(columnName, parsedWidth);
        return;
      }
    }
    switch (transform) {
      case "identity":
        partitionSpecBuilder.identity(columnName);
        break;
      case "day":
        partitionSpecBuilder.day(columnName);
        break;
      case "hour":
        partitionSpecBuilder.hour(columnName);
        break;
      case "month":
        partitionSpecBuilder.month(columnName);
        break;
      case "year":
        partitionSpecBuilder.year(columnName);
        break;
      case "unpartitioned":
        break;
      default:
        throw new IllegalStateException();
    }
  }
}
