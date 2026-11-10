package com.linkedin.openhouse.housetables.model;

import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DatabaseMetadata {
  String databaseUuid;
  String databaseName;
  Map<String, String> properties;
}
