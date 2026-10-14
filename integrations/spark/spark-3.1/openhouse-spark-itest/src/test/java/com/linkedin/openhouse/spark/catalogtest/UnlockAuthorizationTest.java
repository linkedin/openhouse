package com.linkedin.openhouse.spark.catalogtest;

import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.OpaAuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tablestest.SpringH2TestApplication;
import com.linkedin.openhouse.tablestest.TestSparkSessionUtil;
import java.net.URI;
import java.util.UUID;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Authenticated Spark/HTTP integration with an enforcing test policy. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UnlockAuthorizationTest {
  private static final String DATABASE = "unlock_auth";
  private static final String ENABLED = "spark.sql.openhouse.unlock.enabled";
  private static final String OWNER = "unlock_owner";
  private static final String ADMIN = "unlock_admin";
  private static final String UPDATER = "unlock_updater";
  private static final String READER = "unlock_reader";

  private ConfigurableApplicationContext server;
  private SparkSession spark;
  private URI serverUri;
  private TableApi controls;
  private OpenHouseInternalRepository repository;
  private String tableId;
  private final OkHttpClient http = new OkHttpClient();

  @BeforeAll
  void start() throws Exception {
    org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.disable();
    server =
        new SpringApplication(SpringH2TestApplication.class, AuthorizationTestConfig.class)
            .run("--server.port=0");
    serverUri =
        URI.create(
            "http://localhost:" + ((WebServerApplicationContext) server).getWebServer().getPort());
    repository = server.getBean(OpenHouseInternalRepository.class);
    SparkSession.Builder builder = TestSparkSessionUtil.getBaseBuilder(URI.create("file:///"));
    for (String principal : new String[] {OWNER, ADMIN, UPDATER, READER}) {
      TestSparkSessionUtil.configureCatalogs(builder, principal, serverUri);
      builder.config("spark.sql.catalog." + principal + ".auth-token", token(principal));
      builder.config("spark.sql.catalog." + principal + ".cache-enabled", false);
    }
    spark = builder.getOrCreate();
    ApiClient client = new ApiClient();
    client.setBasePath(serverUri.toString());
    client.addDefaultHeader("Authorization", "Bearer " + token(ADMIN));
    controls = new TableApi(client);
  }

  @BeforeEach
  void createLockedTable() {
    spark.conf().set(ENABLED, true);
    tableId = "table_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql("CREATE TABLE " + OWNER + "." + DATABASE + "." + tableId + " (id BIGINT)");
    Assertions.assertEquals(OWNER, storedTable().getTableCreator());
    lock(CreateUpdateLockRequestBody.ReasonEnum.SYSTEM_ONLY);
  }

  @Test
  void authorizationPreflightIsAuthenticatedAndReadOnly() throws Exception {
    TableDto before = storedTable();
    Assertions.assertEquals(204, preflight(token(OWNER)));
    Assertions.assertEquals(204, preflight(token(ADMIN)));
    Assertions.assertEquals(204, preflight(token(UPDATER)));
    Assertions.assertEquals(403, preflight(token(READER)));
    Assertions.assertEquals(401, preflight(null));
    Assertions.assertEquals(401, preflight("not-a-jwt"));
    Assertions.assertEquals(before, storedTable());
  }

  @Test
  void unauthorizedCallerIsRejectedDuringSparkAnalysis() throws Exception {
    TableDto before = storedTable();
    LogicalPlan parsed =
        spark.sessionState().sqlParser().parsePlan(unlockSql(READER, " REASON SYSTEM_ONLY"));
    LogicalPlan analyzed = spark.sessionState().analyzer().execute(parsed);
    WebClientResponseWithMessageException denied =
        Assertions.assertThrows(
            WebClientResponseWithMessageException.class,
            () -> spark.sessionState().analyzer().checkAnalysis(analyzed));
    Assertions.assertEquals(403, denied.getStatusCode());
    Assertions.assertEquals(before, storedTable());

    for (String suffix : new String[] {"", " REASON LEGACY", " REASON SYSTEM_ONLY"}) {
      WebClientResponseWithMessageException executionDenied =
          Assertions.assertThrows(
              WebClientResponseWithMessageException.class,
              () -> spark.sql(unlockSql(READER, suffix)));
      Assertions.assertEquals(403, executionDenied.getStatusCode());
      Assertions.assertEquals(before, storedTable());
    }
  }

  @Test
  void readAccessDoesNotAuthorizeUnlockEvenWhenAlreadyUnlocked() {
    controls.deleteLockByReasonV1(DATABASE, tableId, "SYSTEM_ONLY").block();
    spark.sql("DESCRIBE TABLE " + READER + "." + DATABASE + "." + tableId);
    TableDto before = storedTable();
    WebClientResponseWithMessageException denied =
        Assertions.assertThrows(
            WebClientResponseWithMessageException.class, () -> spark.sql(unlockSql(READER, "")));
    Assertions.assertEquals(403, denied.getStatusCode());
    Assertions.assertEquals(before, storedTable());
  }

  @Test
  void authorizedCallersCanUnlockWithoutLoadingLockedTable() {
    for (String principal : new String[] {OWNER, ADMIN, UPDATER}) {
      WebClientResponseWithMessageException locked =
          Assertions.assertThrows(
              WebClientResponseWithMessageException.class,
              () -> spark.sql("DESCRIBE TABLE " + principal + "." + DATABASE + "." + tableId));
      Assertions.assertEquals(423, locked.getStatusCode());
      spark.sql(unlockSql(principal, " REASON SYSTEM_ONLY"));
      Assertions.assertNull(storedTable().getPolicies().getLockState());
      lock(CreateUpdateLockRequestBody.ReasonEnum.LEGACY);
      spark.sql(unlockSql(principal, ""));
      Assertions.assertNull(storedTable().getPolicies().getLockState());
      lock(CreateUpdateLockRequestBody.ReasonEnum.SYSTEM_ONLY);
    }
  }

  @Test
  void disabledCommandLeavesLockUntouched() {
    spark.conf().set(ENABLED, false);
    TableDto before = storedTable();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> spark.sql(unlockSql(OWNER, " REASON SYSTEM_ONLY")));
    Assertions.assertEquals(before, storedTable());
  }

  @AfterEach
  void dropTable() {
    if (tableId != null) {
      TableDto table = storedTable();
      if (table.getPolicies() != null && table.getPolicies().getLockState() != null) {
        controls
            .deleteLockByReasonV1(
                DATABASE, tableId, table.getPolicies().getLockState().getReason().name())
            .block();
      }
      spark.sql("DROP TABLE " + ADMIN + "." + DATABASE + "." + tableId);
    }
  }

  @AfterAll
  void stop() {
    if (spark != null) {
      spark.close();
    }
    if (server != null) {
      server.close();
    }
  }

  private String unlockSql(String principal, String suffix) {
    return "ALTER TABLE " + principal + "." + DATABASE + "." + tableId + " UNLOCK" + suffix;
  }

  private void lock(CreateUpdateLockRequestBody.ReasonEnum reason) {
    controls
        .createLockV1(
            DATABASE, tableId, new CreateUpdateLockRequestBody().locked(true).reason(reason))
        .block();
  }

  private TableDto storedTable() {
    return repository
        .findById(TableDtoPrimaryKey.builder().databaseId(DATABASE).tableId(tableId).build())
        .orElseThrow(() -> new AssertionError("Missing test table " + tableId));
  }

  private int preflight(String token) throws Exception {
    Request.Builder request =
        new Request.Builder()
            .url(serverUri + "/v1/databases/" + DATABASE + "/tables/" + tableId + "/lock")
            .head();
    if (token != null) {
      request.header("Authorization", "Bearer " + token);
    }
    try (Response response = http.newCall(request.build()).execute()) {
      return response.code();
    }
  }

  private static String token(String principal) throws Exception {
    return new DummyTokenInterceptor.DummySecurityJWT(principal).buildNoopJWT();
  }

  @Configuration
  static class AuthorizationTestConfig implements WebMvcConfigurer {
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
      registry.addInterceptor(new DummyTokenInterceptor());
    }

    @Bean
    @Primary
    AuthorizationHandler ownerAuthorizationHandler() {
      return new OpaAuthorizationHandler() {
        @Override
        public boolean checkAccessDecision(String principal, TableDto table, Privileges privilege) {
          // Readers can read metadata, but only owners, metadata updaters and lock admins unlock.
          return OWNER.equals(principal) && principal.equals(table.getTableCreator())
              || ADMIN.equals(principal)
              || UPDATER.equals(principal)
              || READER.equals(principal) && privilege == Privileges.GET_TABLE_METADATA;
        }

        @Override
        public boolean checkAccessDecision(
            String principal, DatabaseDto database, Privileges privilege) {
          return OWNER.equals(principal) || ADMIN.equals(principal);
        }
      };
    }
  }
}
