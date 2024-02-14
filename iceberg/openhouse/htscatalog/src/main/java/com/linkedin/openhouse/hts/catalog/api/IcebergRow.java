package com.linkedin.openhouse.hts.catalog.api;

import java.util.Objects;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;

/**
 * Generic interface for reading/writing unique rows in a House Table
 *
 * <p>Contains following specialized logic on top of a typical DTO: - Versioning: to ensures iceberg
 * atomically updates a row. - Serializing: to correctly translate the DTO to iceberg's
 * GenericRecord - Lookup: to uniquely identify a row based on its primary key.
 *
 * <p>Assumptions for this API: - Version is a string value, and a top level column (not nested). -
 * Implementation for PrimaryKey will be {@link #getIcebergRowPrimaryKey() getIcebergRowPrimaryKey}
 * correct, ie. {@link com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRow}
 * returns {@link com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRowPrimaryKey}
 * and not JobTableIcebergRowPrimaryKey.
 */
public interface IcebergRow {

  /**
   * Get {@link Schema} for this Row
   *
   * @return this row's schema
   */
  Schema getSchema();

  /**
   * Translate the row to {@link GenericRecord}, to be written to a Iceberg table
   *
   * @return this row's GenericRecord
   */
  GenericRecord getRecord();

  /**
   * Get the name of the Version Column. For example, {@link
   * com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRow} has version column
   * "tableVersion". The column has to be a top-level schema element and not a nested element.
   *
   * <p>This information is used internally to atomically update a row in an iceberg table.
   *
   * @return String representing the name of the column.
   */
  String getVersionColumnName();

  /**
   * Construct Primary Key for this IcebergRow. Should contain be a basic mapping from the entire
   * row to its primary key elements.
   *
   * <p>Used internally to identify the primary key elements for a given row.
   *
   * @return {@link IcebergRowPrimaryKey} for this row.
   */
  IcebergRowPrimaryKey getIcebergRowPrimaryKey();

  /**
   * Get version of this row.
   *
   * <p>This information is used internally to atomically update a row in an iceberg table.
   *
   * <p>The update to an iceberg row will fail if the target version does not match the existing
   * version in the table.
   *
   * @return current version of this row.
   */
  default String getCurrentVersion() {
    return (String) getRecord().getField(getVersionColumnName());
  }

  /**
   * Get next version when the row will be persisted.
   *
   * <p>This information is used internally to atomically update a row in an iceberg table. The
   * iceberg row will contain this version when update is successful.
   *
   * @return next version when this row will be persisted.
   */
  default String getNextVersion() {
    return String.valueOf(Objects.hashCode(this));
  }
}
