package com.linkedin.openhouse.tables.config;

import java.util.Optional;

public interface TblPropsToggleRegistry {

  Optional<String> obtainFeatureByKey(String key);
}
