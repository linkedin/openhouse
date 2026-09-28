package com.linkedin.openhouse.optimizer.analyzer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AnalyzerModeTest {

  @Test
  void from_parsesCaseInsensitively() {
    assertThat(AnalyzerMode.from("incremental")).isEqualTo(AnalyzerMode.INCREMENTAL);
    assertThat(AnalyzerMode.from("FULL")).isEqualTo(AnalyzerMode.FULL);
    assertThat(AnalyzerMode.from("  Full  ")).isEqualTo(AnalyzerMode.FULL);
  }

  @Test
  void from_defaultsToIncremental_whenBlank() {
    assertThat(AnalyzerMode.from(null)).isEqualTo(AnalyzerMode.INCREMENTAL);
    assertThat(AnalyzerMode.from("")).isEqualTo(AnalyzerMode.INCREMENTAL);
    assertThat(AnalyzerMode.from("   ")).isEqualTo(AnalyzerMode.INCREMENTAL);
  }

  @Test
  void from_rejectsUnknownValue() {
    assertThatThrownBy(() -> AnalyzerMode.from("weekly"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid analyzer.mode");
  }
}
