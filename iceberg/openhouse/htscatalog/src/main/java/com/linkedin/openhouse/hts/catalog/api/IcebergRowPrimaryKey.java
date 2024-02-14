package com.linkedin.openhouse.hts.catalog.api;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;

/**
 * Generic interface for identifying unique rows in a House Table
 *
 * <p>Contains following specialized logic on top of a typical DTO: - Searching: to uniquely
 * identify a row based on its primary key. - Deserializing: to construct an entire IcebergRow for a
 * given primary key
 *
 * <p>Assumptions for this API: - Columns for primary key cannot be nested, they are top-level
 * elements. - Their value cannot be null. - Implementation for constructing a row by {@link
 * #buildIcebergRow(Record) buildIcebergRow} is correct, ie. {@link
 * com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRowPrimaryKey} should return
 * {@link com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRow} and not
 * JobTableIcebergRow.
 */
public interface IcebergRowPrimaryKey {

  /**
   * Get {@link Schema} for this primary key
   *
   * @return this primary key's schema
   */
  Schema getSchema();

  /**
   * Translate the primary key to a {@link GenericRecord}, to be written to iceberg's {@Link
   * org.apache.iceberg.DeleteFile}
   *
   * @return this primary key's GenericRecord
   */
  GenericRecord getRecord();

  /**
   * Convert the primary key to a {@link Expression}
   *
   * <p>This information will be used internally to 1) Identify unique row 2) Optimize row delta
   * operations 3) To ensure upsert
   *
   * @return this primary key's Search Expression
   */
  Expression getSearchExpression();

  /**
   * Construct a entire {@link IcebergRow} from a given iceberg {@link Record}
   *
   * <p>This method serves as a deserializing tool while reading data from iceberg table for a given
   * primary key.
   *
   * @return translated {@link IcebergRow} from iceberg {@link Record}
   */
  IcebergRow buildIcebergRow(Record record);
}
