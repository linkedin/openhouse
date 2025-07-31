package com.linkedin.openhouse.internal.catalog.model;

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class SoftDeletedTablePrimaryKey implements Serializable {
  String databaseId;

  String tableId;

  private long deletedAtMs;
}
