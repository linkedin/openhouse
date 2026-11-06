package com.linkedin.openhouse.internal.catalog.model;

import java.util.Map;
import lombok.Value;

@Value
public class DatabaseMetadata {
  String databaseUuid;
  String databaseName;
  Map<String, String> properties;
}
