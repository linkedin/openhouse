package com.linkedin.openhouse.housetables.model;

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@EqualsAndHashCode
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class SoftDeletedUserTableRowPrimaryKey implements Serializable {
  private String tableId;
  private String databaseId;
  private Long deletedAtMs;
}
