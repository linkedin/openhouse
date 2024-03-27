package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.*;
import static com.linkedin.openhouse.tables.repository.impl.InternalRepositoryUtils.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import com.linkedin.openhouse.cluster.storage.filesystem.HdfsStorageProvider;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PartitionSpecMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.TableTypeMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.CommitInfo;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.internal.util.ConversionUtils;
import io.delta.standalone.types.StructType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/**
 * Delta implementation for internal repository. In the future, we can layer it so that Iceberg and
 * Delta are served through a common layer.
 */
@Primary
@Component
@Slf4j
public class OpenHouseDeltaInternalRepositoryImpl implements OpenHouseInternalRepository {

  @Autowired HdfsStorageProvider hdfsStorageProvider;

  @Autowired FsStorageProvider fsStorageProvider;

  @Autowired OpenHouseInternalRepositoryImpl openHouseInternalRepository; // TODO: clean this up

  @Autowired HouseTableRepository houseTableRepository;

  @Autowired PoliciesSpecMapper policiesMapper;

  @Autowired PartitionSpecMapper partitionSpecMapper;

  @Autowired TableTypeMapper tableTypeMapper;

  @Override
  public List<TableDto> findAllByDatabaseId(String databaseId) {
    return houseTableRepository.findAllByDatabaseId(databaseId).stream()
        .map(x -> TableDto.builder().databaseId(x.getDatabaseId()).tableId(x.getTableId()).build())
        .collect(Collectors.toList());
  }

  @Override
  public List<TableDtoPrimaryKey> findAllIds() {
    return null;
  }

  @Override
  public List<TableDto> searchTables(String databaseId) {
    return houseTableRepository.findAllByDatabaseId(databaseId).stream()
        .map(x -> TableDto.builder().databaseId(x.getDatabaseId()).tableId(x.getTableId()).build())
        .collect(Collectors.toList());
  }

  @Override
  public TableDto save(TableDto tableDto) {
    Optional<TableDto> existed =
        findById(
            TableDtoPrimaryKey.builder()
                .tableId(tableDto.getTableId())
                .databaseId(tableDto.getDatabaseId())
                .build());

    Path tablePath;

    Operation.Name operationType;

    if (!existed.isPresent()) {
      tablePath =
          new Path(
              constructTablePath(
                      fsStorageProvider,
                      tableDto.getDatabaseId(),
                      tableDto.getTableId(),
                      tableDto.getTableUUID())
                  .toString());
      operationType = Operation.Name.CREATE_TABLE;
    } else {
      tablePath = new Path(existed.get().getTableLocation());
      operationType = Operation.Name.UPDATE;
    }

    DeltaLog log = DeltaLog.forTable(hdfsStorageProvider.storageClient().getConf(), tablePath);

    OptimisticTransaction txn = log.startTransaction();

    Metadata metaData =
        txn.metadata()
            .copyBuilder()
            .configuration(openHouseInternalRepository.computePropsForTableCreation(tableDto))
            .partitionColumns(new ArrayList())
            .schema((StructType) StructType.fromJson(tableDto.getSchema()))
            .build();
    txn.updateMetadata(metaData);
    List<Action> totalCommitFiles = new ArrayList<>();
    CommitInfo commitInfo = null;
    for (String actionString :
        Optional.ofNullable(tableDto.getJsonSnapshots()).orElse(new ArrayList<>())) {
      Action action =
          ConversionUtils.convertAction(
              io.delta.standalone.internal.actions.Action.fromJson(actionString));
      if (!(action instanceof CommitInfo)) {
        totalCommitFiles.add(action);
      } else {
        commitInfo = (CommitInfo) action;
      }
    }
    if (commitInfo != null && log.snapshot().getVersion() != commitInfo.getReadVersion().get()) {
      throw new CommitFailedException(
          String.format(
              "Conflict detected for databaseId: %s, tableId: %s, expected version: %s actual version %s: %s",
              tableDto.getDatabaseId(),
              tableDto.getTableId(),
              commitInfo.getVersion().get(),
              log.snapshot().getVersion(),
              "The requested user table has been modified/created by other processes."));
    }

    txn.commit(
        totalCommitFiles,
        new Operation(operationType, ImmutableMap.of("ohVersion", tableDto.getTableVersion())),
        "Zippy/1.0.0");

    houseTableRepository.save(
        HouseTable.builder()
            .databaseId(tableDto.getDatabaseId())
            .tableId(tableDto.getTableId())
            .tableUUID(tableDto.getTableUUID())
            .clusterId(tableDto.getClusterId())
            .tableUri(tableDto.getTableUri())
            .tableLocation(tablePath.toString())
            .tableCreator(tableDto.getTableCreator())
            .tableVersion(tableDto.getTableVersion())
            .build());

    // return committed metadata, can be optimized
    return findById(
            TableDtoPrimaryKey.builder()
                .databaseId(tableDto.getDatabaseId())
                .tableId(tableDto.getTableId())
                .build())
        .get();
  }

  @Override
  public <S extends TableDto> Iterable<S> saveAll(Iterable<S> entities) {
    return null;
  }

  @Override
  public Optional<TableDto> findById(TableDtoPrimaryKey pk) {
    Optional<HouseTable> houseTable;
    try {
      houseTable =
          houseTableRepository.findById(
              HouseTablePrimaryKey.builder()
                  .databaseId(pk.getDatabaseId())
                  .tableId(pk.getTableId())
                  .build());
    } catch (HouseTableNotFoundException houseTableNotFoundException) {
      return Optional.empty();
    }
    if (houseTable.isPresent()) {
      DeltaLog table =
          DeltaLog.forTable(
              hdfsStorageProvider.storageClient().getConf(), houseTable.get().getTableLocation());
      Map<String, String> props = table.snapshot().getMetadata().getConfiguration();
      return Optional.of(
          convertToTableDto(
              table,
              props,
              fsStorageProvider,
              partitionSpecMapper,
              policiesMapper,
              tableTypeMapper));
    }
    return Optional.empty();
  }

  @Override
  public boolean existsById(TableDtoPrimaryKey tableDtoPrimaryKey) {
    return findById(tableDtoPrimaryKey).isPresent();
  }

  @Override
  public Iterable<TableDto> findAll() {
    return null;
  }

  @Override
  public Iterable<TableDto> findAllById(Iterable<TableDtoPrimaryKey> tableDtoPrimaryKeys) {
    return null;
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public void deleteById(TableDtoPrimaryKey tableDtoPrimaryKey) {}

  @Override
  public void delete(TableDto entity) {}

  @Override
  public void deleteAllById(Iterable<? extends TableDtoPrimaryKey> tableDtoPrimaryKeys) {}

  @Override
  public void deleteAll(Iterable<? extends TableDto> entities) {}

  @Override
  public void deleteAll() {}

  static TableDto convertToTableDto(
      DeltaLog table,
      Map<String, String> megaProps,
      FsStorageProvider fsStorageProvider,
      PartitionSpecMapper partitionSpecMapper,
      PoliciesSpecMapper policiesMapper,
      TableTypeMapper tableTypeMapper) {
    /* Contains everything needed to populate dto */
    TableDto tableDto =
        TableDto.builder()
            .tableId(megaProps.get(getCanonicalFieldName("tableId")))
            .databaseId(megaProps.get(getCanonicalFieldName("databaseId")))
            .clusterId(megaProps.get(getCanonicalFieldName("clusterId")))
            .tableUri(megaProps.get(getCanonicalFieldName("tableUri")))
            .tableUUID(megaProps.get(getCanonicalFieldName("tableUUID")))
            .tableLocation(table.getPath().toString())
            .tableVersion(megaProps.get(getCanonicalFieldName("tableVersion")))
            .tableCreator(megaProps.get(getCanonicalFieldName("tableCreator")))
            .policies(policiesMapper.toPoliciesObject(megaProps.get("policies")))
            .schema(table.snapshot().getMetadata().getSchema().toPrettyJson())
            .tableType(TableType.PRIMARY_TABLE)
            .jsonSnapshots(null)
            .tableProperties(megaProps)
            .build();

    return tableDto;
  }
}
