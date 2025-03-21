package com.linkedin.openhouse.tables.model;

import static com.linkedin.openhouse.tables.model.TableModelConstants.HEALTH_SCHEMA_LITERAL;
import static com.linkedin.openhouse.tables.model.TableModelConstants.TABLE_POLICIES;

import com.google.common.collect.Sets;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.common.TableType;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableDtoMappingTest {

  private static final Set<String> FIELDS_INTO_SPEC =
      ImmutableSet.of("schema", "timePartitioning", "tableProperties", "clustering");
  private static final Set<String> FIELDS_UNMAPPABLE =
      ImmutableSet.of(
          "stageCreate", "jsonSnapshots", "snapshotRefs", "policies", "tableType", "locked");

  /** Making all fields making it to map is expected, and all expected field are making it there. */
  @Test
  public void testTableDtoToMap() {
    Map<String, String> map = ImmutableMap.of();
    List<ClusteringColumn> clusteringColumns = ImmutableList.of();
    List<String> snapshots = ImmutableList.of();
    Map<String, String> snapshotRefs = ImmutableMap.of();

    TableDto tableDto =
        TableDto.builder()
            .tableId("a")
            .databaseId("b")
            .clusterId("c")
            .tableUri("d")
            .tableCreator("e")
            .tableUUID("f")
            .tableProperties(map)
            .tableLocation("g")
            .tableVersion("h")
            .lastModifiedTime(1000L)
            .clustering(clusteringColumns)
            .jsonSnapshots(snapshots)
            .snapshotRefs(snapshotRefs)
            .schema(HEALTH_SCHEMA_LITERAL)
            .policies(TABLE_POLICIES)
            .locked(false)
            .tableType(TableType.PRIMARY_TABLE)
            .build();

    Map<String, String> convertedMap = tableDto.convertToMap();

    Set<String> dtoFieldNames =
        Arrays.stream(TableDto.class.getDeclaredFields())
            .filter(
                field ->
                    Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isStatic(field.getModifiers()))
            .map(Field::getName)
            .collect(Collectors.toSet());

    Set<String> combinedNonMappedKeys = new HashSet<>(FIELDS_INTO_SPEC);
    combinedNonMappedKeys.addAll(FIELDS_UNMAPPABLE);

    // Mapped keySet is mutually exclusive while complementary with non-mapped keys.
    Assertions.assertEquals(
        Sets.intersection(combinedNonMappedKeys, convertedMap.keySet()).size(), 0);
    Assertions.assertEquals(
        Sets.union(combinedNonMappedKeys, convertedMap.keySet()), dtoFieldNames);
  }
}
