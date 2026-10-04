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
public class ReplicationConfigurationRowPrimaryKey implements Serializable {
  private String sourceDatabaseId;

  private String sourceTableId;

  private String destinationClusterId;

  private String destinationDatabaseId;

  private String destinationTableId;
}
