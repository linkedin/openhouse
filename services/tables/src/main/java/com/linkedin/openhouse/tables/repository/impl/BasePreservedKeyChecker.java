package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.IS_OH_PREFIXED;

import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import org.springframework.stereotype.Component;

/**
 * A base implementation of {@link PreservedKeyChecker} interface that considers keys with
 * "openhouse." prefix as preserved keys.
 */
@Component
public class BasePreservedKeyChecker implements PreservedKeyChecker {

  /**
   * Determine if a key in tblproperties is a preserved key. This behavior is subject to overwritten
   * in different environment.
   */
  public boolean isKeyPreserved(String key) {
    return IS_OH_PREFIXED.test(key);
  }

  /**
   * Details of determining if a key is preserved for the given {@link TableDto} is hidden in the
   * implementation of aspect that associates with the annotation {@link TblPropsEnabler}
   */
  @Override
  @TblPropsEnabler
  public boolean isKeyPreservedForTable(String key, TableDto tableDto) {
    return isKeyPreserved(key);
  }

  @Override
  public String describePreservedSpace() {
    return "table properties starting with `openhouse.` cannot be modified";
  }
}
