package com.linkedin.openhouse.housetables.model;

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
public class UserTableRowPrimaryKey implements Serializable {
  private String tableId;

  private String databaseId;

  @Override
  public String toString() {
    return this.databaseId + ":" + this.tableId;
  }
}
