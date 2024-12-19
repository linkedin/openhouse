package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import lombok.Getter;

@Getter
public enum TimeGranularity {
  HOUR("H"),
  DAY("D"),
  MONTH("M"),
  YEAR("Y");

  private final String granularity;

  TimeGranularity(String granularity) {
    this.granularity = granularity;
  }
}
