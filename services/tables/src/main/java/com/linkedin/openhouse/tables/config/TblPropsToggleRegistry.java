package com.linkedin.openhouse.tables.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/** A central registry for all table properties that are managed through Feature Toggle. */
@Component
public class TblPropsToggleRegistry {

  public static final String ENABLE_TBLTYPE = "enable_tabletype";

  // TODO: Using these vocabularies as MySQL validation
  private final Map<String, String> featureKeys = new HashMap<>();

  @PostConstruct
  public void initializeKeys() {
    // placeholders: demo purpose
    featureKeys.put("openhouse.tableType", ENABLE_TBLTYPE);
  }

  public Optional<String> obtainFeatureByKey(String key) {
    return Optional.ofNullable(featureKeys.get(key));
  }

  public boolean isFeatureRegistered(String featureId) {
    return featureKeys.containsValue(featureId);
  }
}
