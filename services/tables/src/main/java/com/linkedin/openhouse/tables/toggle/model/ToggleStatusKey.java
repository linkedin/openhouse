package com.linkedin.openhouse.tables.toggle.model;

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
public class ToggleStatusKey implements Serializable {
  private String featureId;

  private String tableId;

  private String databaseId;
}
