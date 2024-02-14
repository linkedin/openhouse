package com.linkedin.openhouse.hts.catalog.repository;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.data.GenericIcebergRowReadersWriters;
import java.util.ArrayList;
import java.util.Optional;
import lombok.Builder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.springframework.data.repository.CrudRepository;

/**
 * Main API for upsert-ing rows to an iceberg house table.
 *
 * <p>The constructor of this class requires the following: - an Iceberg {@link Catalog} - {@link
 * TableIdentifier} for the house table
 *
 * <p>This class provides functionality to GET/PUT/DELETE an {@link IcebergRow} identified uniquely
 * by {@link IcebergRowPrimaryKey}. An update is guaranteed to be atomic based on the version of the
 * {@link IcebergRow}, ie. Update will succeed only from a target version ({@link
 * IcebergRow#getCurrentVersion()}) to the next version ({@link IcebergRow#getNextVersion()}).
 *
 * <p>This class provides additional functionality to search rows based on partial keys ({@link
 * #searchByPartialId(IcebergRowPrimaryKey)} This will be useful to search rows based on filtering
 * criteria coming from the partial keys, for example for finding all tables for a given database in
 * the case of {@link com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRow}.
 *
 * <p>Assumptions for this API: - A valid iceberg Catalog is instantiated before the constructor is
 * called. - if the table exists already, its schema is in alignment with IcebergRow and its a
 * Iceberg V2 table.
 */
@Builder
public class IcebergHtsRepository<IR extends IcebergRow, IRPK extends IcebergRowPrimaryKey>
    implements CrudRepository<IR, IRPK> {

  private Catalog catalog;

  private TableIdentifier htsTableIdentifier;

  @Builder.Default
  private GenericIcebergRowReadersWriters<IR, IRPK> genericIcebergRowReadersWriters =
      new GenericIcebergRowReadersWriters<>();

  /**
   * @throws org.apache.iceberg.exceptions.CommitFailedException if conflicting commit occurs or
   *     version of the persisted row has evolved from the target version.
   */
  @Override
  public <IRI extends IR> IRI save(IRI icebergRow) {
    Table table;
    if (!catalog.tableExists(htsTableIdentifier)) {
      table =
          catalog.createTable(
              htsTableIdentifier,
              icebergRow.getSchema(),
              PartitionSpec.unpartitioned(),
              ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));
      table.newAppend().commit();
    } else {
      table = catalog.loadTable(htsTableIdentifier);
    }
    return (IRI) genericIcebergRowReadersWriters.put(table, icebergRow);
  }

  @Override
  public Optional<IR> findById(IRPK icebergRowPrimaryKey) {
    if (!catalog.tableExists(htsTableIdentifier)) {
      return Optional.empty();
    }
    Table table = catalog.loadTable(htsTableIdentifier);
    return genericIcebergRowReadersWriters.get(table, icebergRowPrimaryKey);
  }

  @Override
  public boolean existsById(IRPK icebergRowPrimaryKey) {
    return findById(icebergRowPrimaryKey).isPresent();
  }

  /** @throws NotFoundException when the row doesn't exist */
  @Override
  public void deleteById(IRPK icebergRowPrimaryKey) {
    if (!catalog.tableExists(htsTableIdentifier)) {
      throw new NotFoundException("Table not found: " + htsTableIdentifier.toString());
    }
    Table table = catalog.loadTable(htsTableIdentifier);
    genericIcebergRowReadersWriters.delete(table, icebergRowPrimaryKey);
  }

  /** @throws NotFoundException when the row doesn't exist */
  @Override
  public void delete(IR icebergRow) {
    if (!catalog.tableExists(htsTableIdentifier)) {
      throw new NotFoundException("Table not found: " + htsTableIdentifier.toString());
    }
    Table table = catalog.loadTable(htsTableIdentifier);
    genericIcebergRowReadersWriters.delete(table, (IRPK) icebergRow.getIcebergRowPrimaryKey());
  }

  public Iterable<IR> searchByPartialId(IRPK icebergRowPrimaryKey) {
    if (!catalog.tableExists(htsTableIdentifier)) {
      return new ArrayList<>();
    }
    Table table = catalog.loadTable(htsTableIdentifier);
    return genericIcebergRowReadersWriters.searchByPartialId(table, icebergRowPrimaryKey);
  }

  @Override
  public void deleteAll() {
    catalog.dropTable(htsTableIdentifier);
  }

  /* IMPLEMENT AS NEEDED */
  @Override
  public Iterable<IR> findAll() {
    throw getUnsupportedException();
  }

  @Override
  public <S extends IR> Iterable<S> saveAll(Iterable<S> entities) {
    throw getUnsupportedException();
  }

  @Override
  public Iterable<IR> findAllById(Iterable<IRPK> irpks) {
    throw getUnsupportedException();
  }

  @Override
  public long count() {
    throw getUnsupportedException();
  }

  @Override
  public void deleteAllById(Iterable<? extends IRPK> irpks) {
    throw getUnsupportedException();
  }

  @Override
  public void deleteAll(Iterable<? extends IR> entities) {
    throw getUnsupportedException();
  }

  private UnsupportedOperationException getUnsupportedException() {
    return new UnsupportedOperationException(
        "Only save, findById, existsById, deleteById supported for IcebergHtsRepository");
  }
}
