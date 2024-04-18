package com.linkedin.openhouse.housetables.e2e.togglerule;

import com.linkedin.openhouse.housetables.model.TableToggleRule;

public class ToggleStatusesTestConstants {
  private ToggleStatusesTestConstants() {
    // Util class ctor noop
  }

  // Following are single parameters that aligns with rule_0
  static final String TEST_DB_NAME = "db";
  static final String TEST_TABLE_NAME = "tbl";
  static final String TEST_FEATURE_NAME = "dummy1";

  static final TableToggleRule TEST_RULE_0 =
      TableToggleRule.builder()
          .feature(TEST_FEATURE_NAME)
          .creationTimeMs(System.currentTimeMillis())
          .tablePattern(TEST_TABLE_NAME)
          .databasePattern(TEST_DB_NAME)
          .build();

  static final TableToggleRule TEST_RULE_1 =
      TEST_RULE_0.toBuilder().tablePattern("testtbl1").build();
  static final TableToggleRule TEST_RULE_2 = TEST_RULE_0.toBuilder().feature("dummy2").build();
}
