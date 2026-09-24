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

/** The endpoint inventory is also checked against controller annotations in the unit suite. */
final class HtsIndexScenarios {
  static final String DB = "Db042";
  static final String TABLE = "Table04200";
  static final String METADATA = "file:///index/04200.json";
  static final long DELETED_AT = 1700000000000L;

  final String name;
  final String method;
  final String path;
  final int status;
  final int rowBudget;
  final String scanReason;
  final List<String> tables;
  final Map<String, String> parameters = new LinkedHashMap<>();
  String body;
  String mutation;
  boolean countRequired;
  boolean restore;
  boolean views;
  Integer expectedResults;

  private HtsIndexScenarios(
      String name,
      String method,
      String path,
      int status,
      int budget,
      String reason,
      String... tables) {
    this.name = name;
    this.method = method;
    this.path = path;
    this.status = status;
    this.rowBudget = budget;
    this.scanReason = reason;
    this.tables = Arrays.asList(tables);
  }

  private HtsIndexScenarios param(String key, Object value) {
    parameters.put(key, String.valueOf(value));
    return this;
  }

  private HtsIndexScenarios key() {
    return param("databaseId", DB.toLowerCase(java.util.Locale.ROOT))
        .param("tableId", TABLE.toUpperCase(java.util.Locale.ROOT));
  }

  private HtsIndexScenarios results(int count) {
    expectedResults = count;
    return this;
  }

  private HtsIndexScenarios page() {
    param("page", 0).param("size", 3);
    countRequired = true;
    return this;
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

  String route() {
    return method + " " + path;
  }

  @Override
  public String toString() {
    return name;
  }

  static List<HtsIndexScenarios> all() {
    List<HtsIndexScenarios> cases = new ArrayList<>();
    cases.add(indexed("table-get", "GET", "/hts/tables", 200, 10, "user_table_row").key());
    for (String path : Arrays.asList("/hts/tables/query", "/v1/hts/tables/query")) {
      boolean paged = path.startsWith("/v1");
      String prefix = paged ? "tables-page-" : "tables-list-";
      HtsIndexScenarios databases =
          unbounded(prefix + "databases", path, "Enumerates all databases", "user_table_row")
              .results(paged ? 3 : 100);
      HtsIndexScenarios database =
          indexed(prefix + "database", "GET", path, 200, 300, "user_table_row")
              .param("databaseId", DB.toLowerCase(java.util.Locale.ROOT))
              .results(paged ? 3 : 100);
      HtsIndexScenarios exact =
          indexed(prefix + "exact", "GET", path, 200, 300, "user_table_row").key().results(1);
      HtsIndexScenarios prefixPattern =
          indexed(prefix + "prefix", "GET", path, 200, 300, "user_table_row")
              .param("databaseId", DB)
              .param("tableId", "Table042%")
              .results(paged ? 3 : 100);
      HtsIndexScenarios suffixPattern =
          indexed(prefix + "leading-wildcard", "GET", path, 200, 300, "user_table_row")
              .param("databaseId", DB)
              .param("tableId", "%00")
              .results(1);
      // Even a leading wildcard must use the selective database prefix of the composite index.
      if (paged) {
        databases.page();
        database.page();
        prefixPattern.page();
        exact.param("size", 1);
        exact.countRequired = true;
        suffixPattern.param("size", 1);
        suffixPattern.countRequired = true;
      }
      cases.addAll(Arrays.asList(databases, database, exact, prefixPattern, suffixPattern));
    }
    for (boolean pagedByTable : Arrays.asList(false, true)) {
      for (boolean expires : Arrays.asList(false, true)) {
        HtsIndexScenarios deleted =
            indexed(
                    "soft-list-" + pagedByTable + "-" + expires,
                    "GET",
                    "/hts/tables/querySoftDeleted",
                    200,
                    pagedByTable ? 30 : 300,
                    "soft_deleted_user_table_row")
                .param("databaseId", DB)
                .page()
                .results(3);
        if (pagedByTable) {
          deleted.param("tableId", TABLE);
        }
        if (expires) {
          deleted.param("purgeAfterMs", DELETED_AT + 1005);
        }
        cases.add(deleted);
      }
    }
    HtsIndexScenarios put =
        indexed("table-update", "PUT", "/hts/tables", 200, 10, "user_table_row");
    put.body = tableBody(TABLE, METADATA);
    put.mutation = "update";
    cases.add(put);
    HtsIndexScenarios create =
        indexed("table-create", "PUT", "/hts/tables", 201, 10, "user_table_row");
    create.body = tableBody("NewTable", "INITIAL_VERSION");
    cases.add(create);
    for (String path : Arrays.asList("/hts/tables", "/v1/hts/tables")) {
      HtsIndexScenarios hard =
          indexed("hard-delete-" + path, "DELETE", path, 204, 10, "user_table_row").key();
      if (path.startsWith("/v1")) {
        hard.param("isSoftDelete", false);
      }
      hard.mutation = "delete";
      cases.add(hard);
    }
    HtsIndexScenarios soft =
        indexed("soft-delete", "DELETE", "/v1/hts/tables", 204, 10, "user_table_row")
            .key()
            .param("isSoftDelete", true);
    soft.mutation = "delete";
    cases.add(soft);
    HtsIndexScenarios rename =
        indexed("table-rename", "PATCH", "/hts/tables/rename", 204, 10, "user_table_row")
            .param("fromDatabaseId", DB)
            .param("fromTableId", TABLE)
            .param("toDatabaseId", DB)
            .param("toTableId", "RenamedTable")
            .param("metadataLocation", "file:///index/renamed.json");
    rename.mutation = "update";
    cases.add(rename);
    HtsIndexScenarios restore =
        indexed(
                "table-restore",
                "PUT",
                "/hts/tables/restore",
                200,
                10,
                "user_table_row",
                "soft_deleted_user_table_row")
            .key()
            .param("deletedAtMs", DELETED_AT);
    restore.restore = true;
    restore.mutation = "delete";
    cases.add(restore);
    for (boolean expires : Arrays.asList(false, true)) {
      HtsIndexScenarios purge =
          indexed(
                  "table-purge-" + expires,
                  "DELETE",
                  "/hts/tables/purge",
                  204,
                  30,
                  "soft_deleted_user_table_row")
              .key();
      if (expires) {
        purge.param("purgeAfterMs", DELETED_AT + 1005);
      }
      purge.mutation = "delete";
      cases.add(purge);
    }
    cases.add(
        indexed("job-get", "GET", "/hts/jobs", 200, 10, "job_row").param("jobId", "job04200"));
    HtsIndexScenarios jobPut = indexed("job-update", "PUT", "/hts/jobs", 200, 10, "job_row");
    jobPut.body = jobBody("job04200").replace("QUEUED", "RUNNING");
    jobPut.mutation = "update";
    cases.add(jobPut);
    HtsIndexScenarios jobCreate = indexed("job-create", "PUT", "/hts/jobs", 201, 10, "job_row");
    jobCreate.body = jobBody("job-new");
    cases.add(jobCreate);
    HtsIndexScenarios jobDelete =
        indexed("job-delete", "DELETE", "/hts/jobs", 204, 10, "job_row").param("jobId", "job04200");
    jobDelete.mutation = "delete";
    cases.add(jobDelete);
    cases.add(
        unbounded("jobs-list", "/hts/jobs/query", "Enumerates all jobs", "job_row").results(10000));
    cases.add(
        unbounded(
                "jobs-state-filter",
                "/hts/jobs/query",
                "No state index in schema; current service filters jobs in memory",
                "job_row")
            .param("state", "QUEUED")
            .results(10000));
    // A key-selective API must not read all jobs just because its current implementation does.
    cases.add(
        indexed("jobs-query-id", "GET", "/hts/jobs/query", 200, 10, "job_row")
            .param("jobId", "job04200")
            .results(1));
    cases.add(
        indexed("toggle-feature", "GET", "/hts/togglestatuses", 200, 300, "table_toggle_rule")
            .param("databaseId", DB)
            .param("tableId", TABLE)
            .param("featureId", "Feature042"));
    // The pre-fix and combined-fix revisions both expose these routes; reverted main does not.
    if (Arrays.stream(
            com.linkedin.openhouse.housetables.controller.UserHouseTablesController.class
                .getDeclaredMethods())
        .anyMatch(method -> method.getName().equals("getUserView"))) {
      cases.add(
          indexed("entity-table-get", "GET", "/hts/entities", 200, 10, "user_table_row").key());
      HtsIndexScenarios entityView =
          indexed("entity-view-get", "GET", "/hts/entities", 200, 10, "user_table_row").key();
      entityView.views = true;
      cases.add(entityView);
      HtsIndexScenarios viewGet =
          indexed("view-get", "GET", "/hts/views", 200, 10, "user_table_row").key();
      viewGet.views = true;
      cases.add(viewGet);
      HtsIndexScenarios viewPut =
          indexed("view-update", "PUT", "/hts/views", 200, 10, "user_table_row");
      viewPut.body = tableBody(TABLE, METADATA);
      viewPut.mutation = "update";
      viewPut.views = true;
      cases.add(viewPut);
      HtsIndexScenarios viewCreate =
          indexed("view-create", "PUT", "/hts/views", 201, 10, "user_table_row");
      viewCreate.body = tableBody("NewView", "INITIAL_VERSION");
      viewCreate.views = true;
      cases.add(viewCreate);
      HtsIndexScenarios viewDelete =
          indexed("view-delete", "DELETE", "/hts/views", 204, 10, "user_table_row").key();
      viewDelete.views = true;
      viewDelete.mutation = "delete";
      cases.add(viewDelete);
      HtsIndexScenarios allViews =
          unbounded(
                  "views-page-all",
                  "/v1/hts/views/query",
                  "Enumerates all views without a database key",
                  "user_table_row")
              .page()
              .results(3);
      allViews.views = true;
      cases.add(allViews);
      for (String pattern : Arrays.asList("", TABLE, "Table042%", "%00")) {
        HtsIndexScenarios query =
            indexed(
                    "views-page-"
                        + (pattern.isEmpty() ? "database" : pattern.replace("%", "wildcard")),
                    "GET",
                    "/v1/hts/views/query",
                    200,
                    300,
                    "user_table_row")
                .param("databaseId", DB)
                .page();
        if (!pattern.isEmpty()) {
          query.param("tableId", pattern);
        }
        if (pattern.equals(TABLE) || pattern.equals("%00")) {
          query.param("size", 1).results(1);
        } else {
          query.results(3);
        }
        query.views = true;
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

  private static HtsIndexScenarios indexed(
      String name, String method, String path, int status, int budget, String... tables) {
    return new HtsIndexScenarios(name, method, path, status, budget, null, tables);
  }

  private static HtsIndexScenarios unbounded(
      String name, String path, String reason, String... tables) {
    return new HtsIndexScenarios(name, "GET", path, 200, 0, reason, tables);
  }
}
