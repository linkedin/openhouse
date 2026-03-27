package com.linkedin.openhouse.housetables.service.impl;

import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.housetables.api.spec.model.StorageLocation;
import com.linkedin.openhouse.housetables.model.StorageLocationRow;
import com.linkedin.openhouse.housetables.model.TableStorageLocationRow;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.StorageLocationJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.TableStorageLocationJdbcRepository;
import com.linkedin.openhouse.housetables.service.StorageLocationService;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.transaction.annotation.Transactional;

public class StorageLocationServiceImpl implements StorageLocationService {

  private final StorageLocationJdbcRepository storageLocationRepo;
  private final TableStorageLocationJdbcRepository tableStorageLocationRepo;

  public StorageLocationServiceImpl(
      StorageLocationJdbcRepository storageLocationRepo,
      TableStorageLocationJdbcRepository tableStorageLocationRepo) {
    this.storageLocationRepo = storageLocationRepo;
    this.tableStorageLocationRepo = tableStorageLocationRepo;
  }

  @Override
  @Transactional
  public StorageLocation createStorageLocation(String uri) {
    String id = UUID.randomUUID().toString();
    StorageLocationRow row = StorageLocationRow.builder().storageLocationId(id).uri(uri).build();
    storageLocationRepo.save(row);
    return toModel(row);
  }

  @Override
  public StorageLocation getStorageLocation(String storageLocationId) {
    return storageLocationRepo
        .findById(storageLocationId)
        .map(this::toModel)
        .orElseThrow(() -> new NoSuchEntityException("StorageLocation", storageLocationId));
  }

  @Override
  public List<StorageLocation> getStorageLocationsForTable(String databaseId, String tableId) {
    return tableStorageLocationRepo.findByDatabaseIdAndTableId(databaseId, tableId).stream()
        .map(join -> getStorageLocation(join.getStorageLocationId()))
        .collect(Collectors.toList());
  }

  @Override
  @Transactional
  public StorageLocation createStorageLocation() {
    return createStorageLocation("");
  }

  @Override
  @Transactional
  public StorageLocation updateStorageLocationUri(String storageLocationId, String uri) {
    StorageLocationRow row =
        storageLocationRepo
            .findById(storageLocationId)
            .orElseThrow(() -> new NoSuchEntityException("StorageLocation", storageLocationId));
    StorageLocationRow updated = row.toBuilder().uri(uri).build();
    storageLocationRepo.save(updated);
    return toModel(updated);
  }

  @Override
  @Transactional
  public void addStorageLocationToTable(
      String databaseId, String tableId, String storageLocationId) {
    if (!storageLocationRepo.existsById(storageLocationId)) {
      throw new NoSuchEntityException("StorageLocation", storageLocationId);
    }
    TableStorageLocationRow join =
        TableStorageLocationRow.builder()
            .databaseId(databaseId)
            .tableId(tableId)
            .storageLocationId(storageLocationId)
            .build();
    tableStorageLocationRepo.save(join);
  }

  private StorageLocation toModel(StorageLocationRow row) {
    return StorageLocation.builder()
        .storageLocationId(row.getStorageLocationId())
        .uri(row.getUri())
        .build();
  }
}
