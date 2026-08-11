package com.linkedin.openhouse.javaclient.api;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Catalogs implementing this interface can resolve the column-level read entitlements of the
 * calling principal, which lets a query engine mask the columns the caller is not allowed to read.
 *
 * <p>The catalog is the policy decision point: it owns both the policy tags attached to columns and
 * the grants held by principals, so the engine never has to reason about roles or tags itself.
 */
public interface SupportsColumnEntitlements {

  @AllArgsConstructor
  @Getter
  @EqualsAndHashCode
  class ColumnEntitlementsDto {
    /** Policy tags present on the table that the caller is entitled to read. */
    List<String> grantedTags;

    /** Columns the caller must not read; the engine is expected to mask or reject them. */
    List<String> restrictedColumns;
  }

  /**
   * Resolves the calling principal's column entitlements on an OH table.
   *
   * @param tableIdentifier identifier for the table, ex: db.table
   * @return granted tags and restricted columns for the caller
   */
  ColumnEntitlementsDto getColumnEntitlements(TableIdentifier tableIdentifier);
}
