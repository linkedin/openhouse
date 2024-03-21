package com.linkedin.openhouse.tables.dto.mapper;

import com.linkedin.openhouse.common.schema.IcebergSchemaHelper;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.common.DefaultColumnPattern;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PartitionSpecMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import java.util.HashMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

/** Mapper class to transform between DTO and Data Model objects. */
@Mapper(
    componentModel = "spring",
    imports = {
      Table.class,
      IcebergSchemaHelper.class,
      TablesMapperHelper.class,
      PoliciesSpecMapper.class,
      HashMap.class,
      TableType.class,
      DefaultColumnPattern.class
    },
    uses = {PartitionSpecMapper.class, PoliciesSpecMapper.class})
public interface TablesMapper {

  /**
   * Update elements in {@link TableDto} based on {@link
   * com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody} Note that
   * return of this method is for saving, thus read-only field like creationTime is omitted
   *
   * @param tableDto Destination {@link TableDto} that will be updated.
   * @param requestBody Source {@link
   *     com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody}
   * @return A new immutable {@link TableDto} with updated elements.
   */
  @Mappings({
    @Mapping(source = "requestBody.tableId", target = "tableId"),
    @Mapping(source = "requestBody.databaseId", target = "databaseId"),
    @Mapping(source = "requestBody.clusterId", target = "clusterId"),
    @Mapping(source = "requestBody.schema", target = "schema"),
    @Mapping(source = "requestBody.timePartitioning", target = "timePartitioning"),
    @Mapping(source = "requestBody.clustering", target = "clustering"),
    @Mapping(source = "requestBody.tableProperties", target = "tableProperties"),
    @Mapping(source = "requestBody.policies", target = "policies", qualifiedByName = "mapPolicies"),
    @Mapping(source = "requestBody.stageCreate", target = "stageCreate"),
    @Mapping(
        source = "requestBody.tableType",
        target = "tableType",
        defaultExpression = "java(TableType.PRIMARY_TABLE)"),
    @Mapping(
        source = "requestBody.baseTableVersion",
        target = "tableVersion"), /* store base version to check later */
    @Mapping(target = "lastModifiedTime", ignore = true),
    @Mapping(target = "creationTime", ignore = true)
  })
  TableDto toTableDto(TableDto tableDto, CreateUpdateTableRequestBody requestBody);

  /**
   * Updated elements in {@link TableDto} based on {@link IcebergSnapshotsRequestBody} Note that
   * return of this method is for saving, thus read-only field like creationTime is omitted
   *
   * @param tableDto Destination {@link TableDto} that will be updated upon.
   * @param requestBody Source {@link IcebergSnapshotsRequestBody}
   * @return A new immutable {@link TableDto} with updated elements.
   */
  @Mappings({
    @Mapping(source = "tableDto.tableId", target = "tableId"),
    @Mapping(source = "tableDto.databaseId", target = "databaseId"),
    @Mapping(source = "tableDto.clusterId", target = "clusterId"),
    @Mapping(source = "requestBody.jsonSnapshots", target = "jsonSnapshots"),
    @Mapping(source = "requestBody.snapshotRefs", target = "snapshotRefs"),
    @Mapping(
        source = "requestBody.baseTableVersion",
        target = "tableVersion"), /* store base version to check later */
    @Mapping(source = "requestBody.createUpdateTableRequestBody.schema", target = "schema"),
    @Mapping(
        source = "requestBody.createUpdateTableRequestBody.tableProperties",
        target = "tableProperties"),
    @Mapping(
        source = "requestBody.createUpdateTableRequestBody.timePartitioning",
        target = "timePartitioning"),
    @Mapping(source = "requestBody.createUpdateTableRequestBody.clustering", target = "clustering"),
    @Mapping(
        source = "requestBody.createUpdateTableRequestBody.policies",
        target = "policies",
        qualifiedByName = "mapPolicies"),
    @Mapping(
        source = "requestBody.createUpdateTableRequestBody.stageCreate",
        target = "stageCreate"),
    @Mapping(
        source = "requestBody.createUpdateTableRequestBody.tableType",
        target = "tableType",
        defaultExpression = "java(TableType.PRIMARY_TABLE)"),
    @Mapping(target = "lastModifiedTime", ignore = true),
    @Mapping(target = "creationTime", ignore = true)
  })
  TableDto toTableDto(TableDto tableDto, IcebergSnapshotsRequestBody requestBody);

  /**
   * From a source {@link TableDto}, prepare a {@link
   * com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody}
   *
   * @param tableDto Source {@link TableDto} to transform.
   * @return Destination {@link
   *     com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody} to be forwarded to
   *     the client.
   */
  GetTableResponseBody toGetTableResponseBody(TableDto tableDto);

  @Mappings({
    @Mapping(
        conditionExpression = "java(tableIdentifier.namespace() != null)",
        expression = "java(tableIdentifier.namespace().toString())",
        target = "databaseId"),
    @Mapping(expression = "java(tableIdentifier.name())", target = "tableId")
  })
  TableDtoPrimaryKey toTableDtoPrimaryKey(TableIdentifier tableIdentifier);

  @Mappings({
    @Mapping(
        conditionExpression = "java(tableIdentifier.namespace() != null)",
        expression = "java(tableIdentifier.namespace().toString())",
        target = "databaseId"),
    @Mapping(expression = "java(tableIdentifier.name())", target = "tableId")
  })
  TableDto toTableDto(TableIdentifier tableIdentifier);
}
