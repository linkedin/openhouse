package com.linkedin.openhouse.housetables.model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.persistence.Version;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "replication_configuration_state")
@IdClass(ReplicationConfigurationStateRowPrimaryKey.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ReplicationConfigurationStateRow {
  @Id private String sourceDatabaseId;

  @Id private String sourceTableId;

  @Version private Long version;

  private boolean configured;
}
