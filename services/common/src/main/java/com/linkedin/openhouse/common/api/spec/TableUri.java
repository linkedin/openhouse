package com.linkedin.openhouse.common.api.spec;

import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
/** Model object to construct a tableUri as unique identifier for a OpenHouse table. */
public class TableUri {
  @NotNull(message = "tableId cannot be null")
  private String tableId;

  @NotNull(message = "databaseId cannot be null")
  private String databaseId;

  @NotNull(message = "clusterId cannot be null")
  private String clusterId;

  public String toString() {
    return new StringBuilder()
        .append(clusterId)
        .append(".")
        .append(databaseId)
        .append(".")
        .append(tableId)
        .toString();
  }
}
