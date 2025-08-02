package com.linkedin.openhouse.internal.catalog.model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@IdClass(SoftDeletedTablePrimaryKey.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class SoftDeletedTableDto {

  @Id private String databaseId;

  @Id private String tableId;

  @Id private long deletedAtMs;

  private String tableLocation;

  private long purgeAfterMs;
}
