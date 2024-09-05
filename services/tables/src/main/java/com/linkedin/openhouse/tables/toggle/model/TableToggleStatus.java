package com.linkedin.openhouse.tables.toggle.model;

import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Data Model for persisting Feature-Toggle Rule in the HTS-Repository. */
@Entity
@IdClass(ToggleStatusKey.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TableToggleStatus {

  @Id private String featureId;

  @Id private String tableId;

  @Id private String databaseId;

  private ToggleStatus.StatusEnum toggleStatusEnum;
}
