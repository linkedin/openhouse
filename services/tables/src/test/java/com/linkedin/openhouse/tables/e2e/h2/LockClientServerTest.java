package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.config.TablesMvcConstants.HTTP_HEADER_ACTION_TYPE;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.security.DummyTokenInterceptor.DummySecurityJWT;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.client.model.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.client.model.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.client.model.LockState;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import javax.servlet.Filter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
import org.springframework.web.reactive.function.client.WebClientResponseException;

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
  private static final String OBSERVED_HEADER = "X-Test-Action-Type";
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  @LocalServerPort private int port;

  private ApiClient apiClient;
  private TableApi tableApi;
  private boolean tableCreated;

  @BeforeEach
  void createTable() throws Exception {
    apiClient = createApiClient();
    tableApi = new TableApi(apiClient);
    CreateUpdateTableRequestBody request =
        apiClient
            .getObjectMapper()
            .readValue(
                buildCreateUpdateTableRequestBody(
                        GET_TABLE_RESPONSE_BODY.toBuilder().tableId(TABLE_ID).build())
                    .toJson(),
                CreateUpdateTableRequestBody.class);
    tableApi.createTableV1(DATABASE_ID, request).block(TIMEOUT);
    tableCreated = true;
  }

  @AfterEach
  void deleteTable() {
    if (tableCreated) {
      tableApi.deleteTableV1(DATABASE_ID, TABLE_ID).block(TIMEOUT);
    }
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "SYSTEM,SYSTEM_ONLY,SYSTEM_ONLY",
        "SYSTEM,NULL,LEGACY",
        "NULL,NULL,LEGACY",
        "NULL,LEGACY,LEGACY",
        "NULL,OMITTED,LEGACY"
      },
      nullValues = "NULL")
  void lockRoundTrip(String actionType, String reason, String expectedReason) {
    if (actionType != null) {
      apiClient.addDefaultHeader("X-OpenHouse-Action-Type", actionType);
    }
    CreateUpdateLockRequestBody request =
        new CreateUpdateLockRequestBody()
            .locked(true)
            .creationTime(System.currentTimeMillis())
            .expirationInDays(1);
    if (!"OMITTED".equals(reason)) {
      request.reason(
          reason == null ? null : CreateUpdateLockRequestBody.ReasonEnum.fromValue(reason));
    }

    ResponseEntity<Void> created =
        tableApi.createLockV1WithHttpInfo(DATABASE_ID, TABLE_ID, request).block(TIMEOUT);
    assertNotNull(created);
    assertEquals(HttpStatus.CREATED, created.getStatusCode());
    assertEquals(actionType, created.getHeaders().getFirst(OBSERVED_HEADER));

    ResponseEntity<GetTableResponseBody> response =
        tableApi.getTableV1WithHttpInfo(DATABASE_ID, TABLE_ID).block(TIMEOUT);
    assertNotNull(response);
    assertEquals(HttpStatus.OK, response.getStatusCode());
    assertEquals(actionType, response.getHeaders().getFirst(OBSERVED_HEADER));
    assertNotNull(response.getBody());
    LockState lock = response.getBody().getPolicies().getLockState();
    assertTrue(lock.getLocked());
    assertEquals(expectedReason, lock.getReason().getValue());
    if ("SYSTEM_ONLY".equals(expectedReason)) {
      tableApi.deleteLockByReasonV1(DATABASE_ID, TABLE_ID, expectedReason).block(TIMEOUT);
    } else {
      tableApi.deleteLockV1(DATABASE_ID, TABLE_ID).block(TIMEOUT);
    }
    assertNull(
        tableApi.getTableV1(DATABASE_ID, TABLE_ID).block(TIMEOUT).getPolicies().getLockState());
  }

  @ParameterizedTest
  @CsvSource(
      value = {"NULL,423", "SYSTEM,200", "USER,400"},
      nullValues = "NULL")
  void systemOnlyReadAndWriteRoundTrip(String declaration, int expectedStatus) throws Exception {
    GetTableResponseBody current = tableApi.getTableV1(DATABASE_ID, TABLE_ID).block(TIMEOUT);
    tableApi
        .createLockV1(
            DATABASE_ID,
            TABLE_ID,
            new CreateUpdateLockRequestBody()
                .locked(true)
                .reason(CreateUpdateLockRequestBody.ReasonEnum.SYSTEM_ONLY)
                .message("maintenance in progress"))
        .block(TIMEOUT);
    if (declaration != null) {
      apiClient.addDefaultHeader(HTTP_HEADER_ACTION_TYPE, declaration);
    }
    ApiClient inspectionClient = createApiClient();
    inspectionClient.addDefaultHeader(HTTP_HEADER_ACTION_TYPE, "SYSTEM");
    TableApi inspectionApi = new TableApi(inspectionClient);
    LockState lock =
        inspectionApi.getTableV1(DATABASE_ID, TABLE_ID).block(TIMEOUT).getPolicies().getLockState();
    assertEquals(LockState.ReasonEnum.SYSTEM_ONLY, lock.getReason());
    SnapshotApi snapshotApi = new SnapshotApi(apiClient);
    if (expectedStatus == 200) {
      current = tableApi.getTableV1(DATABASE_ID, TABLE_ID).block(TIMEOUT);
      current =
          tableApi
              .updateTableV1(DATABASE_ID, TABLE_ID, updateRequest(current, "metadata"))
              .block(TIMEOUT);
      assertEquals("metadata", current.getTableProperties().get("lock-evaluation-write"));
      assertEquals(lock, current.getPolicies().getLockState());
      current =
          snapshotApi
              .putSnapshotsV1(DATABASE_ID, TABLE_ID, snapshotRequest(current, "snapshot"))
              .block(TIMEOUT);
      assertEquals("snapshot", current.getTableProperties().get("lock-evaluation-write"));
      assertEquals(lock, current.getPolicies().getLockState());
    } else {
      WebClientResponseException denied =
          assertThrows(
              WebClientResponseException.class,
              () -> tableApi.getTableV1(DATABASE_ID, TABLE_ID).block(TIMEOUT));
      assertEquals(expectedStatus, denied.getRawStatusCode());
      if (expectedStatus == 423) {
        assertTrue(denied.getResponseBodyAsString().contains("SYSTEM_ONLY"));
        assertTrue(denied.getResponseBodyAsString().contains("reason-targeted OpenHouse unlock"));
      }
      CreateUpdateTableRequestBody update = updateRequest(current, "denied");
      IcebergSnapshotsRequestBody snapshots = snapshotRequest(current, "denied");
      assertEquals(
          expectedStatus,
          assertThrows(
                  WebClientResponseException.class,
                  () -> tableApi.updateTableV1(DATABASE_ID, TABLE_ID, update).block(TIMEOUT))
              .getRawStatusCode());
      assertEquals(
          expectedStatus,
          assertThrows(
                  WebClientResponseException.class,
                  () -> snapshotApi.putSnapshotsV1(DATABASE_ID, TABLE_ID, snapshots).block(TIMEOUT))
              .getRawStatusCode());
    }
    assertEquals(
        lock,
        inspectionApi
            .getTableV1(DATABASE_ID, TABLE_ID)
            .block(TIMEOUT)
            .getPolicies()
            .getLockState());
    tableApi.deleteLockByReasonV1(DATABASE_ID, TABLE_ID, "SYSTEM_ONLY").block(TIMEOUT);
    assertNull(
        tableApi.getTableV1(DATABASE_ID, TABLE_ID).block(TIMEOUT).getPolicies().getLockState());
  }

  private ApiClient createApiClient() throws Exception {
    ApiClient client = new ApiClient();
    client.setBasePath("http://localhost:" + port);
    client.addDefaultHeader(
        HttpHeaders.AUTHORIZATION,
        "Bearer " + new DummySecurityJWT(GET_TABLE_RESPONSE_BODY.getTableCreator()).buildNoopJWT());
    return client;
  }

  private CreateUpdateTableRequestBody updateRequest(GetTableResponseBody current, String value) {
    HashMap<String, String> properties = new HashMap<>(current.getTableProperties());
    properties.put("lock-evaluation-write", value);
    return new CreateUpdateTableRequestBody()
        .databaseId(DATABASE_ID)
        .tableId(TABLE_ID)
        .clusterId(current.getClusterId())
        .baseTableVersion(current.getTableLocation())
        .schema(current.getSchema())
        .timePartitioning(current.getTimePartitioning())
        .clustering(current.getClustering())
        .sortOrder(current.getSortOrder())
        .tableProperties(properties);
  }

  private IcebergSnapshotsRequestBody snapshotRequest(GetTableResponseBody current, String value) {
    return new IcebergSnapshotsRequestBody()
        .baseTableVersion(current.getTableLocation())
        .createUpdateTableRequestBody(updateRequest(current, value))
        .jsonSnapshots(Collections.emptyList());
  }

  @TestConfiguration
  static class HeaderObservation {
    @Bean
    Filter observeActionType() {
      return (request, response, chain) -> {
        String value = ((HttpServletRequest) request).getHeader(HTTP_HEADER_ACTION_TYPE);
        if (value != null) {
          ((HttpServletResponse) response).setHeader(OBSERVED_HEADER, value);
        }
        chain.doFilter(request, response);
      };
    }
  }
}
