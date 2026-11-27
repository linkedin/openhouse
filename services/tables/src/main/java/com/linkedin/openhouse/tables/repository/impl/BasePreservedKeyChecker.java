package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.IS_OH_PREFIXED;

import com.google.common.collect.ImmutableSet;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import com.linkedin.openhouse.tables.toggle.FeatureToggleAspect;
import org.springframework.stereotype.Component;

/**
 * A base implementation of {@link PreservedKeyChecker} interface that considers keys with
 * "openhouse." prefix as preserved keys. The temporary table keys {@link
 * CatalogConstants#OPENHOUSE_TEMPORARY_KEY} and {@link
 * CatalogConstants#OPENHOUSE_EXPIRATION_TIME_KEY} are not preserved, so users can set them; the
 * commit path enforces their rules.
 */
@Component
public class BasePreservedKeyChecker implements PreservedKeyChecker {

  private static final ImmutableSet<String> TEMPORARY_TABLE_KEYS =
      ImmutableSet.of(
          CatalogConstants.OPENHOUSE_TEMPORARY_KEY, CatalogConstants.OPENHOUSE_EXPIRATION_TIME_KEY);

  /**
   * Determine if a key in tblproperties is a preserved key. This behavior is subject to overwritten
   * in different environment.
   */
  public boolean isKeyPreserved(String key) {
    return (IS_OH_PREFIXED.test(key) && !TEMPORARY_TABLE_KEYS.contains(key))
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
        + CatalogConstants.OPENHOUSE_TEMPORARY_KEY
        + "` and `"
        + CatalogConstants.OPENHOUSE_EXPIRATION_TIME_KEY
        + "`) and the `policies` key cannot be modified";
  }
}
