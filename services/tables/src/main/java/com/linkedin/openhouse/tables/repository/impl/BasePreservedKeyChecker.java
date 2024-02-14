package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.IS_OH_PREFIXED;

import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import org.springframework.stereotype.Component;

@Component
public class BasePreservedKeyChecker implements PreservedKeyChecker {

  /**
   * Determine if a key in tblproperties is a preserved key. This behavior is subject to overwritten
   * in different environment.
   */
  public boolean isKeyPreserved(String key) {
    return IS_OH_PREFIXED.test(key);
  }

  @Override
  public String describePreservedSpace() {
    return "table properties starting with `openhouse.` cannot be modified";
  }
}
