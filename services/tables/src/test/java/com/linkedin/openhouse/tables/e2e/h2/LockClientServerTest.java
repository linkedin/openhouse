package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.config.TablesMvcConstants.HTTP_HEADER_SYSTEM_ACTION;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.security.DummyTokenInterceptor.DummySecurityJWT;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.client.model.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.client.model.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.LockState;
import com.linkedin.openhouse.tables.client.model.Policies;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import java.time.Duration;
import java.util.Optional;
import javax.servlet.Filter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.openapitools.jackson.nullable.JsonNullable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(
    classes = SpringH2Application.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
@Import(LockClientServerTest.HeaderObservation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LockClientServerTest {
  private static final String DATABASE_ID = GET_TABLE_RESPONSE_BODY.getDatabaseId();
  private static final String TABLE_ID = "lock_client_roundtrip";
  private static final String TABLE_CREATOR = GET_TABLE_RESPONSE_BODY.getTableCreator();
  private static final String OBSERVED_HEADER = "X-Test-System-Action";
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  @LocalServerPort private int port;

  private ApiClient apiClient;
  private TableApi tableApi;
  private String tableUUID;
  private boolean tableCreated;

  @BeforeEach
  void createTable() throws Exception {
    apiClient = new ApiClient();
    apiClient.setBasePath("http://localhost:" + port);
    apiClient.addDefaultHeader(
        HttpHeaders.AUTHORIZATION, "Bearer " + new DummySecurityJWT(TABLE_CREATOR).buildNoopJWT());
    tableApi = new TableApi(apiClient);
    CreateUpdateTableRequestBody request =
        apiClient
            .getObjectMapper()
            .readValue(
                buildCreateUpdateTableRequestBody(
                        GET_TABLE_RESPONSE_BODY.toBuilder().tableId(TABLE_ID).build())
                    .toJson(),
                CreateUpdateTableRequestBody.class);
    tableUUID =
        Optional.ofNullable(tableApi.createTableV1(DATABASE_ID, request).block(TIMEOUT))
            .map(GetTableResponseBody::getTableUUID)
            .orElseThrow(() -> new AssertionError("Table creation returned no table UUID"));
    tableCreated = true;
  }

  @AfterEach
  void deleteTable() {
    if (tableCreated) {
      activeLock()
          .ifPresent(
              lock -> {
                if (lock.getReason() == LockState.ReasonEnum.LEGACY) {
                  tableApi.deleteLockV1(DATABASE_ID, TABLE_ID).block(TIMEOUT);
                } else {
                  tableApi
                      .deleteLockByReasonV1(
                          DATABASE_ID,
                          TABLE_ID,
                          lock.getReason().getValue(),
                          lock.getTableUUID(),
                          lock.getLockOwner())
                      .block(TIMEOUT);
                }
              });
      tableApi.deleteTableV1(DATABASE_ID, TABLE_ID).block(TIMEOUT);
    }
  }

  /**
   * Exercises the lock lifecycle a table owner drives from the generated client against the running
   * service: create the table, lock it, read the lock back, unlock it, and read the table back
   * without a lock.
   */
  @Test
  void tableCreatorLocksReadsAndUnlocksOwnTable() {
    ResponseEntity<Void> locked =
        tableApi
            .createLockV1WithHttpInfo(
                DATABASE_ID,
                TABLE_ID,
                new CreateUpdateLockRequestBody()
                    .locked(true)
                    .message("Locked by the table creator")
                    .creationTime(System.currentTimeMillis())
                    .expirationInDays(1))
            .block(TIMEOUT);
    assertNotNull(locked);
    assertEquals(HttpStatus.CREATED, locked.getStatusCode());

    LockState lock = activeLock().orElseThrow(() -> new AssertionError("Expected an active lock"));
    assertEquals(LockState.ReasonEnum.LEGACY, lock.getReason());
    assertEquals("Locked by the table creator", lock.getMessage());
    assertEquals(Integer.valueOf(1), lock.getExpirationInDays());

    ResponseEntity<Void> unlocked =
        tableApi.deleteLockV1WithHttpInfo(DATABASE_ID, TABLE_ID).block(TIMEOUT);
    assertNotNull(unlocked);
    assertEquals(HttpStatus.NO_CONTENT, unlocked.getStatusCode());

    assertEquals(Optional.empty(), activeLock());
  }

  @Test
  void tableCreatorLocksReadsAndUnlocksOwnTableWithStructuredReason() {
    ResponseEntity<Void> locked =
        tableApi
            .createLockV1WithHttpInfo(
                DATABASE_ID,
                TABLE_ID,
                new CreateUpdateLockRequestBody()
                    .locked(true)
                    .reason(CreateUpdateLockRequestBody.ReasonEnum.TIER3_AUTO_CLEANUP)
                    .message("Cleanup starts tomorrow")
                    .expectedTableUUID(tableUUID)
                    .creationTime(System.currentTimeMillis())
                    .expirationInDays(1))
            .block(TIMEOUT);
    assertNotNull(locked);
    assertEquals(HttpStatus.CREATED, locked.getStatusCode());

    LockState lock = activeLock().orElseThrow(() -> new AssertionError("Expected an active lock"));
    assertEquals(LockState.ReasonEnum.TIER3_AUTO_CLEANUP, lock.getReason());
    assertEquals("Cleanup starts tomorrow", lock.getMessage());
    assertEquals(tableUUID, lock.getTableUUID());
    assertEquals(TABLE_CREATOR, lock.getLockOwner());

    ResponseEntity<Void> unlocked =
        tableApi
            .deleteLockByReasonV1WithHttpInfo(
                DATABASE_ID,
                TABLE_ID,
                LockState.ReasonEnum.TIER3_AUTO_CLEANUP.getValue(),
                tableUUID,
                TABLE_CREATOR)
            .block(TIMEOUT);
    assertNotNull(unlocked);
    assertEquals(HttpStatus.NO_CONTENT, unlocked.getStatusCode());

    assertEquals(Optional.empty(), activeLock());
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "true,TIER3_AUTO_CLEANUP,TIER3_AUTO_CLEANUP",
        "true,NULL,LEGACY",
        "false,NULL,LEGACY",
        "NULL,NULL,LEGACY",
        "NULL,LEGACY,LEGACY",
        "NULL,OMITTED,LEGACY"
      },
      nullValues = "NULL")
  void lockRoundTrip(String systemAction, String reason, LockState.ReasonEnum expectedReason) {
    if (systemAction != null) {
      apiClient.addDefaultHeader(HTTP_HEADER_SYSTEM_ACTION, systemAction);
    }
    CreateUpdateLockRequestBody request =
        new CreateUpdateLockRequestBody()
            .locked(true)
            .creationTime(System.currentTimeMillis())
            .expirationInDays(1);
    if ("OMITTED".equals(reason)) {
      request.setReason_JsonNullable(JsonNullable.undefined());
    } else {
      request.reason(
          reason == null ? null : CreateUpdateLockRequestBody.ReasonEnum.fromValue(reason));
    }
    if (expectedReason != LockState.ReasonEnum.LEGACY) {
      request.expectedTableUUID(tableUUID);
    }

    ResponseEntity<Void> created =
        tableApi.createLockV1WithHttpInfo(DATABASE_ID, TABLE_ID, request).block(TIMEOUT);
    assertNotNull(created);
    assertEquals(HttpStatus.CREATED, created.getStatusCode());
    assertEquals(systemAction, created.getHeaders().getFirst(OBSERVED_HEADER));

    ResponseEntity<GetTableResponseBody> response =
        tableApi.getTableV1WithHttpInfo(DATABASE_ID, TABLE_ID).block(TIMEOUT);
    assertNotNull(response);
    assertEquals(HttpStatus.OK, response.getStatusCode());
    assertEquals(systemAction, response.getHeaders().getFirst(OBSERVED_HEADER));
    assertNotNull(response.getBody());
    LockState lock = response.getBody().getPolicies().getLockState();
    assertTrue(lock.getLocked());
    assertEquals(expectedReason, lock.getReason());
  }

  private Optional<LockState> activeLock() {
    return Optional.ofNullable(tableApi.getTableV1(DATABASE_ID, TABLE_ID).block(TIMEOUT))
        .map(GetTableResponseBody::getPolicies)
        .map(Policies::getLockState)
        .filter(lock -> Boolean.TRUE.equals(lock.getLocked()));
  }

  @TestConfiguration
  static class HeaderObservation {
    @Bean
    Filter observeSystemAction() {
      return (request, response, chain) -> {
        String value = ((HttpServletRequest) request).getHeader(HTTP_HEADER_SYSTEM_ACTION);
        if (value != null) {
          ((HttpServletResponse) response).setHeader(OBSERVED_HEADER, value);
        }
        chain.doFilter(request, response);
      };
    }
  }
}
