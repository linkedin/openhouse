package com.linkedin.openhouse.housetables.model;

import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Composite primary key for {@link TableStorageLocationRow}. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor
public class TableStorageLocationRowPrimaryKey implements Serializable {
  private String databaseId;
  private String tableId;
  private String storageLocationId;
}
