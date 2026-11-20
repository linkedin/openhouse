package com.linkedin.openhouse.housetables.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/** Requests used to verify that controller binding preserves keys and dispatches correctly. */
final class HtsIndexScenarios {
  static final String DB = "Db042";
  static final String TABLE = "Table04200";
  static final String METADATA = "file:///index/04200.json";
  static final long DELETED_AT = 1700000000000L;

  final String name;
  final String method;
  final String path;
  final int status;
  final Map<String, String> parameters = new LinkedHashMap<>();
  String body;

  private HtsIndexScenarios(String name, String method, String path, int status) {
    this.name = name;
    this.method = method;
    this.path = path;
    this.status = status;
  }

  private HtsIndexScenarios param(String key, Object value) {
    parameters.put(key, String.valueOf(value));
    return this;
  }

  private HtsIndexScenarios key() {
    return param("databaseId", DB.toLowerCase(java.util.Locale.ROOT))
        .param("tableId", TABLE.toUpperCase(java.util.Locale.ROOT));
  }

  private HtsIndexScenarios page() {
    return param("page", 0).param("size", 3);
  }

  MockHttpServletRequestBuilder request() {
    MockHttpServletRequestBuilder request =
        MockMvcRequestBuilders.request(HttpMethod.valueOf(method), path);
    parameters.forEach(request::param);
    if (body != null) {
      request.contentType(MediaType.APPLICATION_JSON).content(body);
    }
    return request;
  }

  @Override
  public String toString() {
    return name;
  }

  static List<HtsIndexScenarios> all() {
    List<HtsIndexScenarios> cases = new ArrayList<>();
    cases.add(scenario("table-get", "GET", "/hts/tables", 200).key());
    for (String path : Arrays.asList("/hts/tables/query", "/v1/hts/tables/query")) {
      boolean paged = path.startsWith("/v1");
      String prefix = paged ? "tables-page-" : "tables-list-";
      HtsIndexScenarios databases = scenario(prefix + "databases", "GET", path, 200);
      HtsIndexScenarios database =
          scenario(prefix + "database", "GET", path, 200)
              .param("databaseId", DB.toLowerCase(java.util.Locale.ROOT));
      HtsIndexScenarios exact = scenario(prefix + "exact", "GET", path, 200).key();
      HtsIndexScenarios prefixPattern =
          scenario(prefix + "prefix", "GET", path, 200)
              .param("databaseId", DB)
              .param("tableId", "Table042%");
      HtsIndexScenarios suffixPattern =
          scenario(prefix + "leading-wildcard", "GET", path, 200)
              .param("databaseId", DB)
              .param("tableId", "%00");
      if (paged) {
        databases.page();
        database.page();
        prefixPattern.page();
        exact.param("size", 1);
        suffixPattern.param("size", 1);
      }
      cases.addAll(Arrays.asList(databases, database, exact, prefixPattern, suffixPattern));
    }
    for (boolean pagedByTable : Arrays.asList(false, true)) {
      for (boolean expires : Arrays.asList(false, true)) {
        HtsIndexScenarios deleted =
            scenario(
                    "soft-list-" + pagedByTable + "-" + expires,
                    "GET",
                    "/hts/tables/querySoftDeleted",
                    200)
                .param("databaseId", DB)
                .page();
        if (pagedByTable) {
          deleted.param("tableId", TABLE);
        }
        if (expires) {
          deleted.param("purgeAfterMs", DELETED_AT + 1005);
        }
        cases.add(deleted);
      }
    }
    HtsIndexScenarios put = scenario("table-update", "PUT", "/hts/tables", 200);
    put.body = tableBody(TABLE, METADATA);
    cases.add(put);
    HtsIndexScenarios create = scenario("table-create", "PUT", "/hts/tables", 201);
    create.body = tableBody("NewTable", "INITIAL_VERSION");
    cases.add(create);
    for (String path : Arrays.asList("/hts/tables", "/v1/hts/tables")) {
      HtsIndexScenarios hard = scenario("hard-delete-" + path, "DELETE", path, 204).key();
      if (path.startsWith("/v1")) {
        hard.param("isSoftDelete", false);
      }
      cases.add(hard);
    }
    cases.add(
        scenario("soft-delete", "DELETE", "/v1/hts/tables", 204).key().param("isSoftDelete", true));
    cases.add(
        scenario("table-rename", "PATCH", "/hts/tables/rename", 204)
            .param("fromDatabaseId", DB)
            .param("fromTableId", TABLE)
            .param("toDatabaseId", DB)
            .param("toTableId", "RenamedTable")
            .param("metadataLocation", "file:///index/renamed.json"));
    cases.add(
        scenario("table-restore", "PUT", "/hts/tables/restore", 200)
            .key()
            .param("deletedAtMs", DELETED_AT));
    for (boolean expires : Arrays.asList(false, true)) {
      HtsIndexScenarios purge =
          scenario("table-purge-" + expires, "DELETE", "/hts/tables/purge", 204).key();
      if (expires) {
        purge.param("purgeAfterMs", DELETED_AT + 1005);
      }
      cases.add(purge);
    }
    cases.add(scenario("job-get", "GET", "/hts/jobs", 200).param("jobId", "job04200"));
    HtsIndexScenarios jobPut = scenario("job-update", "PUT", "/hts/jobs", 200);
    jobPut.body = jobBody("job04200").replace("QUEUED", "RUNNING");
    cases.add(jobPut);
    HtsIndexScenarios jobCreate = scenario("job-create", "PUT", "/hts/jobs", 201);
    jobCreate.body = jobBody("job-new");
    cases.add(jobCreate);
    cases.add(scenario("job-delete", "DELETE", "/hts/jobs", 204).param("jobId", "job04200"));
    cases.add(scenario("jobs-list", "GET", "/hts/jobs/query", 200));
    cases.add(
        scenario("jobs-state-filter", "GET", "/hts/jobs/query", 200).param("state", "QUEUED"));
    cases.add(scenario("jobs-query-id", "GET", "/hts/jobs/query", 200).param("jobId", "job04200"));
    cases.add(
        scenario("toggle-feature", "GET", "/hts/togglestatuses", 200)
            .param("databaseId", DB)
            .param("tableId", TABLE)
            .param("featureId", "Feature042"));
    // Both A/B revisions expose these routes; the reverted f36f0333 baseline does not.
    if (Arrays.stream(
            com.linkedin.openhouse.housetables.controller.UserHouseTablesController.class
                .getDeclaredMethods())
        .anyMatch(method -> method.getName().equals("getUserView"))) {
      cases.add(scenario("entity-get", "GET", "/hts/entities", 200).key());
      cases.add(scenario("view-get", "GET", "/hts/views", 200).key());
      HtsIndexScenarios viewPut = scenario("view-update", "PUT", "/hts/views", 200);
      viewPut.body = tableBody(TABLE, METADATA);
      cases.add(viewPut);
      HtsIndexScenarios viewCreate = scenario("view-create", "PUT", "/hts/views", 201);
      viewCreate.body = tableBody("NewView", "INITIAL_VERSION");
      cases.add(viewCreate);
      cases.add(scenario("view-delete", "DELETE", "/hts/views", 204).key());
      cases.add(scenario("views-page-all", "GET", "/v1/hts/views/query", 200).page());
      for (String pattern : Arrays.asList("", TABLE, "Table042%", "%00")) {
        HtsIndexScenarios query =
            scenario(
                    "views-page-"
                        + (pattern.isEmpty() ? "database" : pattern.replace("%", "wildcard")),
                    "GET",
                    "/v1/hts/views/query",
                    200)
                .param("databaseId", DB)
                .page();
        if (!pattern.isEmpty()) {
          query.param("tableId", pattern);
        }
        if (pattern.equals(TABLE) || pattern.equals("%00")) {
          query.param("size", 1);
        }
        cases.add(query);
      }
    }
    return cases;
  }

  private static String tableBody(String table, String version) {
    return "{\"entity\":{\"databaseId\":\""
        + DB
        + "\",\"tableId\":\""
        + table
        + "\",\"tableVersion\":\""
        + version
        + "\",\"metadataLocation\":\"file:///index/new.json\",\"storageType\":\"hdfs\"}}";
  }

  private static String jobBody(String id) {
    return "{\"entity\":{\"jobId\":\""
        + id
        + "\",\"version\":\"1\",\"jobName\":\"index-test\","
        + "\"state\":\"QUEUED\",\"clusterId\":\"local\"}}";
  }

  private static HtsIndexScenarios scenario(String name, String method, String path, int status) {
    return new HtsIndexScenarios(name, method, path, status);
  }
}
