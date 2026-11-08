package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Spark-3.5-only sanity suite asserting that Spark-SQL view CRUD (CREATE, CREATE OR REPLACE,
 * SELECT, DROP, SHOW VIEWS) is routed to the OpenHouse Views service rather than failing
 * client-side before any request is sent.
 *
 * <p>The embedded server's Views endpoints are a 404 stub ({@code ViewsDisabledService}), so a
 * fully successful round-trip is not yet possible. Each test asserts two invariants that hold
 * regardless of the stub: (1) the command reaches the Views service, and (2) the failure is never
 * the client-side {@code Invalid location: null}. Service-reach shows up in one of two forms, both
 * of which require a server round-trip against the stub: the raw 404 surfaces as a {@code
 * WebClientResponse*} (CREATE, SHOW VIEWS), or -- because OpenHouse keeps no local view state -- a
 * service-derived not-found that is only reachable by calling the service and getting a 404 (CREATE
 * OR REPLACE and DROP as view-not-found, SELECT as {@code TABLE_OR_VIEW_NOT_FOUND}). Runs on Spark
 * 3.5 only, as Iceberg 1.2 / Spark 3.1 has no {@code ViewCatalog}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OpenHouseViewSqlCrudReachesServiceTestSpark3_5 extends OpenHouseSparkITest {

  private static final String DB = "openhouse.crud_db";
  private static final String VIEW = DB + ".v_crud";

  private SparkSession spark;

  @BeforeAll
  void startEnabledViewSession() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    // Route view ops through the OpenHouse Views service (ViewApi) rather than an in-memory store.
    overrides.put("spark.sql.catalog.openhouse.iceberg-views-enabled", "true");
    // Parse CREATE/DROP VIEW through Iceberg's view-command rewrite, as the grid runtime does;
    // explicit for robustness even though the checked-out parser already defaults this to true.
    overrides.put("spark.sql.iceberg.rewriteViewCommands.enabled", "true");
    spark = getSparkSession("openhouse", overrides);
  }

  @AfterAll
  void stopSession() {
    if (spark != null) {
      spark.close();
    }
  }

  // ---- CREATE ----

  @Test
  public void testSparkSqlCreateViewReachesViewsService() {
    Throwable failure =
        assertThrows(Exception.class, () -> spark.sql("CREATE VIEW " + VIEW + " AS SELECT 1 AS c"));
    assertReachedViewsService("CREATE VIEW", failure);
    assertNotInvalidLocation("CREATE VIEW", failure);
  }

  // ---- UPDATE (CREATE OR REPLACE is the view "update") ----

  @Test
  public void testSparkSqlCreateOrReplaceViewReachesViewsService() {
    // Against the 404 stub, CREATE OR REPLACE resolves to the create path: createOrReplace()
    // probes the view (getViewV1 -> 404 -> absent), builds ViewMetadata (exercising the location
    // fix), then calls createViewV1 -> 404. Iceberg raises NoSuchViewException, which
    // SparkCatalog.replaceView re-throws as a bare NoSuchViewException that drops the 404 cause --
    // so only the service-derived not-found is observable, not the raw 404.
    Throwable failure =
        assertThrows(
            Exception.class,
            () -> spark.sql("CREATE OR REPLACE VIEW " + VIEW + " AS SELECT 2 AS c"));
    assertReachedViewsServiceViaNotFound("CREATE OR REPLACE VIEW", failure);
    assertNotInvalidLocation("CREATE OR REPLACE VIEW", failure);
  }

  // ---- READ ----

  @Test
  public void testSparkSqlSelectFromViewReachesViewsServiceThenFallsBack() {
    // loadView hits getViewV1 -> 404 -> absent; SparkCatalog then falls back to table resolution,
    // which also 404s, so analysis ends in TABLE_OR_VIEW_NOT_FOUND. That not-found is itself
    // service-derived (both probes are 404s from the service); the read got far enough to resolve,
    // never tripping the client-side Invalid-location failure.
    Throwable failure = assertThrows(Exception.class, () -> spark.sql("SELECT * FROM " + VIEW));
    assertTrue(
        chainHasType(failure, "AnalysisException")
            && chainHasMessage(failure, "TABLE_OR_VIEW_NOT_FOUND"),
        () ->
            "SELECT expected a TABLE_OR_VIEW_NOT_FOUND AnalysisException but got: "
                + render(failure));
    assertNotInvalidLocation("SELECT", failure);
  }

  // ---- DELETE ----

  @Test
  public void testSparkSqlDropViewReachesViewsService() {
    // dropView hits deleteViewV1 -> 404 -> drop-if-exists no-op (returns false); DROP VIEW without
    // IF EXISTS then surfaces a view-not-found. OpenHouse holds no local view state, so that
    // not-found is only reachable by calling the service and getting a 404 -- the raw 404 is
    // swallowed by drop-if-exists semantics.
    Throwable failure = assertThrows(Exception.class, () -> spark.sql("DROP VIEW " + VIEW));
    assertReachedViewsServiceViaNotFound("DROP VIEW", failure);
    assertNotInvalidLocation("DROP VIEW", failure);
  }

  // ---- LIST ----

  @Test
  public void testSparkSqlShowViewsReachesViewsService() {
    // SHOW VIEWS -> listViews -> getAllViewsV1 -> 404, surfaced as WebClientResponseWithMessage.
    Throwable failure = assertThrows(Exception.class, () -> spark.sql("SHOW VIEWS IN " + DB));
    assertReachedViewsService("SHOW VIEWS", failure);
    assertNotInvalidLocation("SHOW VIEWS", failure);
  }

  // ---- assertions / cause-chain helpers ----

  private static void assertReachedViewsService(String op, Throwable failure) {
    // The 404 from the Views stub surfaces as a WebClient response exception (raw or
    // message-wrapped); "WebClientResponse" matches both WebClientResponseException and
    // WebClientResponseWithMessageException.
    assertTrue(
        chainHasType(failure, "WebClientResponse"),
        () ->
            op
                + " did not reach the Views service (no WebClientResponse* in the cause chain): "
                + render(failure));
  }

  private static void assertReachedViewsServiceViaNotFound(String op, Throwable failure) {
    // No raw 404 reaches the chain here: it is converted to not-found semantics upstream
    // (drop-if-exists for DROP; SparkCatalog.replaceView re-throwing a bare NoSuchViewException for
    // CREATE OR REPLACE). OpenHouse keeps no local view state, so a view-not-found outcome is only
    // reachable by calling the service and getting a 404 -- that is the proof of service-reach.
    assertTrue(
        chainHasType(failure, "NoSuchView") || chainHasMessage(failure, "VIEW_NOT_FOUND"),
        () ->
            op
                + " expected a service-derived view-not-found (proof it reached the service): "
                + render(failure));
  }

  private static void assertNotInvalidLocation(String op, Throwable failure) {
    assertFalse(
        chainHasMessage(failure, "Invalid location"),
        () ->
            op
                + " regressed to the client-side 'Invalid location: null' instead of reaching the"
                + " service: "
                + render(failure));
  }

  private static List<Throwable> causeChain(Throwable top) {
    List<Throwable> chain = new ArrayList<>();
    for (Throwable t = top; t != null && !chain.contains(t); t = t.getCause()) {
      chain.add(t);
    }
    return chain;
  }

  private static boolean chainHasType(Throwable top, String simpleNameFragment) {
    return causeChain(top).stream()
        .anyMatch(t -> t.getClass().getName().contains(simpleNameFragment));
  }

  private static boolean chainHasMessage(Throwable top, String fragment) {
    return causeChain(top).stream()
        .anyMatch(t -> t.getMessage() != null && t.getMessage().contains(fragment));
  }

  private static String render(Throwable top) {
    StringBuilder sb = new StringBuilder();
    for (Throwable t : causeChain(top)) {
      sb.append("\n  <- ").append(t.getClass().getName()).append(": ").append(t.getMessage());
    }
    return sb.toString();
  }
}
