package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.IS_OH_PREFIXED;

import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import com.linkedin.openhouse.tables.toggle.FeatureToggleAspect;
import org.springframework.stereotype.Component;

/**
 * A base implementation of {@link PreservedKeyChecker} interface that considers keys with
 * "openhouse." prefix as preserved keys. The temporary-table expiration time {@link
 * CatalogConstants#OPENHOUSE_LEASE_EXPIRATION_TIME_KEY} is not preserved, so users can set, change,
 * and remove it.
 */
@Component
public class BasePreservedKeyChecker implements PreservedKeyChecker {

  /**
   * Determine if a key in tblproperties is a preserved key. This behavior is subject to overwritten
   * in different environment.
   */
  public boolean isKeyPreserved(String key) {
    return (IS_OH_PREFIXED.test(key)
            && !CatalogConstants.OPENHOUSE_LEASE_EXPIRATION_TIME_KEY.equals(key))
        || InternalRepositoryUtils.POLICIES_KEY.equals(key);
  }

  /**
   * Details of determining if a key is preserved for the given {@link TableDto} is hidden in the
   * implementation of aspect {@link FeatureToggleAspect} that associates with the annotation {@link
   * PreservedPropsToggleEnabler}
   */
  @Override
  @PreservedPropsToggleEnabler
  public boolean isKeyPreservedForTable(String key, TableDto tableDto) {
    return isKeyPreserved(key);
  }

  /**
   * Prevent preserved keys from being added during table creation.
   *
   * @param key
   * @param tableDto
   * @return
   */
  @Override
  public boolean allowKeyInCreation(String key, TableDto tableDto) {
    return !isKeyPreservedForTable(key, tableDto);
  }

  @Override
  public String describePreservedSpace() {
    return "table properties starting with `openhouse.` (except `"
        + CatalogConstants.OPENHOUSE_LEASE_EXPIRATION_TIME_KEY
        + "`) and the `policies` key cannot be modified";
  }
}
