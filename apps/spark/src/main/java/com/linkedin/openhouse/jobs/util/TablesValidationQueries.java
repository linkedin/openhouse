package com.linkedin.openhouse.jobs.util;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class TablesValidationQueries {
  public static final String DATA_FILES_QUERY = "SELECT * FROM %s.files";
  public static final String MANIFESTS_QUERY = "SELECT * FROM %s.manifests";
  public static final String SHOW_TABLES = "SHOW TABLES IN `%s` LIKE '%s'";
  public static final String SHOW_DATABASES = "SHOW DATABASES";
  public static final String DESCRIBE_TABLE = "DESCRIBE TABLE %s";

  public static List<String> get(String type) {
    List<String> queryContainer = new ArrayList<>();
    switch (type) {
      case "metadata":
        queryContainer.add(DATA_FILES_QUERY);
        queryContainer.add(MANIFESTS_QUERY);
        break;
      case "table":
        queryContainer.add(DESCRIBE_TABLE);
        break;
      case "database":
        queryContainer.add(SHOW_DATABASES);
        break;
      default:
        log.info("Query for operation type {} not found", type);
        break;
    }
    return queryContainer;
  }

  private TablesValidationQueries() {}
}
