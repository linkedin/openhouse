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
public class HouseTablePrimaryKey implements Serializable {
  private String tableId;

  private String databaseId;
}
