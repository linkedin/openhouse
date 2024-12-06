package com.linkedin.openhouse.internal.catalog.repository;

import static com.linkedin.openhouse.internal.catalog.HouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.*;

import com.google.gson.Gson;
import com.linkedin.openhouse.housetables.client.api.ToggleStatusApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyUserTable;
import com.linkedin.openhouse.housetables.client.model.GetAllEntityResponseBodyUserTable;
import com.linkedin.openhouse.housetables.client.model.UserTable;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.resolver.dns.DnsNameResolverTimeoutException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Mono;

/** As part of prerequisite of this test, bring up the /hts Springboot application. */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest
public class HouseTableRepositoryImplTest {

  @Autowired
  @Qualifier("repoTest")
  HouseTableRepository htsRepo;

  @Autowired HouseTableMapper houseTableMapper;

  @SpyBean UserTableApi userTableApi;

  @TestConfiguration
  public static class MockWebServerConfiguration {
    /**
     * The {@link Bean} injected along with {@link HouseTableRepositoryImpl} to achieve /tables to
     * /hts communication without bring up a second spring server within one JVM. This bean is
     * preferred to be scoped in this test only given the scope of {@link MockWebServer} object.
     */
    @Bean
    public UserTableApi provideMockHtsApiInstance() {
      return new UserTableApi(getMockServerApiClient());
    }

    @Bean
    public ToggleStatusApi provideMockHtsApiInstanceForToggle() {
      return new ToggleStatusApi(getMockServerApiClient());
    }

    private ApiClient getMockServerApiClient() {
      // Routing the client to access port from Mock server so that Mock server can respond with
      // stub response.
      ApiClient apiClient = new ApiClient();
      String baseUrl = String.format("http://localhost:%s", mockHtsServer.getPort());
      apiClient.setBasePath(baseUrl);
      return apiClient;
    }

    /**
     * The {@link Bean} injected only to those tests concerning on hitting HTS server within from
     * /tables' constructs. Usage: Declare {@link
     * org.springframework.beans.factory.annotation.Autowired} for the injected field along with
     * {@link Qualifier} as default injection is {@link HouseTablesH2Repository} for e2e package.
     */
    @Bean
    @Qualifier("repoTest")
    public HouseTableRepository provideRealHtsRepository() {
      return new HouseTableRepositoryImpl();
    }
  }

  /**
   * The mock HTS server running in /tables e2e test context. With this mock server, the whole code
   * path of generated client are executed, instead of being mocked if using Mockito to stub the
   * behavior of client.
   */
  private static MockWebServer mockHtsServer;

  @BeforeAll
  static void setUp() throws IOException {
    mockHtsServer = new MockWebServer();
    mockHtsServer.start();
  }

  @AfterAll
  static void tearDown() throws IOException {
    mockHtsServer.shutdown();
  }

  @Test
  public void testRepoFindById() {
    EntityResponseBodyUserTable response = new EntityResponseBodyUserTable();
    response.entity(houseTableMapper.toUserTable(HOUSE_TABLE));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(response))
            .addHeader("Content-Type", "application/json"));

    HouseTable result =
        htsRepo
            .findById(
                HouseTablePrimaryKey.builder()
                    .tableId(HOUSE_TABLE.getTableId())
                    .databaseId(HOUSE_TABLE.getDatabaseId())
                    .build())
            .get();

    Assertions.assertEquals(result.getTableId(), HOUSE_TABLE.getTableId());
    Assertions.assertEquals(result.getDatabaseId(), HOUSE_TABLE.getDatabaseId());
    Assertions.assertEquals(result.getTableLocation(), HOUSE_TABLE.getTableLocation());
    Assertions.assertEquals(result.getTableVersion(), HOUSE_TABLE.getTableVersion());
    Assertions.assertEquals(result.getStorageType(), HOUSE_TABLE.getStorageType());
  }

  @Test
  public void testRepoSave() {
    EntityResponseBodyUserTable putResponse = new EntityResponseBodyUserTable();
    putResponse.entity(houseTableMapper.toUserTable(HOUSE_TABLE));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(201)
            .setBody((new Gson()).toJson(putResponse))
            .addHeader("Content-Type", "application/json"));

    HouseTable result = htsRepo.save(HOUSE_TABLE);
    Assertions.assertEquals(result.getTableId(), HOUSE_TABLE.getTableId());
    Assertions.assertEquals(result.getDatabaseId(), HOUSE_TABLE.getDatabaseId());
    Assertions.assertEquals(result.getTableLocation(), HOUSE_TABLE.getTableLocation());
    Assertions.assertEquals(result.getTableVersion(), HOUSE_TABLE.getTableVersion());
    Assertions.assertEquals(result.getStorageType(), HOUSE_TABLE.getStorageType());
  }

  @Test
  public void testRepoSaveWithClientError() {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(409)
            .setBody("RESPONSE BODY DOESNT MATTER")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        HouseTableConcurrentUpdateException.class, () -> htsRepo.save(HOUSE_TABLE), "409");

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(404)
            .setBody("RESPONSE BODY DOESNT MATTER")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        HouseTableNotFoundException.class, () -> htsRepo.save(HOUSE_TABLE), "404");

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(400)
            .setBody("RESPONSE BODY DOESNT MATTER")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        HouseTableCallerException.class, () -> htsRepo.save(HOUSE_TABLE), "400");

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(401)
            .setBody("RESPONSE BODY DOESNT MATTER")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        HouseTableCallerException.class, () -> htsRepo.save(HOUSE_TABLE), "401");

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(429)
            .setBody("RESPONSE BODY DOESNT MATTER")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        HouseTableCallerException.class, () -> htsRepo.save(HOUSE_TABLE), "429");

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(403)
            .setBody("RESPONSE BODY DOESNT MATTER")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        HouseTableCallerException.class, () -> htsRepo.save(HOUSE_TABLE), "403");
  }

  @Test
  public void testRepoSaveWithEmptyMono() {
    // Leave the entity as empty.
    EntityResponseBodyUserTable putResponse = new EntityResponseBodyUserTable();
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(putResponse))
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        RuntimeException.class,
        () -> htsRepo.save(HOUSE_TABLE),
        "Unexpected null value from publisher.");
  }

  @Test
  public void testFindByIdWithErrors() {
    HashMap<Integer, Class> map = new HashMap<>();
    map.put(404, HouseTableNotFoundException.class);
    map.put(409, HouseTableConcurrentUpdateException.class);
    map.put(400, HouseTableCallerException.class);

    for (HashMap.Entry<Integer, Class> entry : map.entrySet()) {
      mockHtsServer.enqueue(
          new MockResponse()
              .setResponseCode(entry.getKey())
              .setBody("")
              .addHeader("Content-Type", "application/json"));
      Assertions.assertThrowsExactly(
          entry.getValue(),
          () ->
              htsRepo
                  .findById(
                      HouseTablePrimaryKey.builder()
                          .tableId(HOUSE_TABLE.getTableId())
                          .databaseId(HOUSE_TABLE.getDatabaseId())
                          .build())
                  .get(),
          entry.getKey().toString());
    }
  }

  @Test
  public void testRepoDelete() {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(204)
            .setBody("")
            .addHeader("Content-Type", "application/json"));

    Assertions.assertDoesNotThrow(
        () ->
            htsRepo.deleteById(
                HouseTablePrimaryKey.builder()
                    .tableId(HOUSE_TABLE.getTableId())
                    .databaseId(HOUSE_TABLE.getDatabaseId())
                    .build()));
  }

  @Test
  public void testRepoDeleteNotFound() {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(404)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        HouseTableNotFoundException.class,
        () ->
            htsRepo.deleteById(
                HouseTablePrimaryKey.builder()
                    .tableId(HOUSE_TABLE.getTableId())
                    .databaseId(HOUSE_TABLE.getDatabaseId())
                    .build()),
        "404");
  }

  @Test
  public void testListOfTablesInDatabase() {
    List<UserTable> tables = new ArrayList<>();
    tables.add(houseTableMapper.toUserTable(HOUSE_TABLE));
    tables.add(houseTableMapper.toUserTable(HOUSE_TABLE_SAME_DB));
    GetAllEntityResponseBodyUserTable listResponse = new GetAllEntityResponseBodyUserTable();
    /**
     * Need to use the reflection trick to help initializing the object with generated class {@link
     * GetAllUserTablesResponseBody}, which somehow doesn't provided proper setter in the generated
     * code.
     */
    Field resultField =
        ReflectionUtils.findField(GetAllEntityResponseBodyUserTable.class, "results");
    Assertions.assertNotNull(resultField);
    ReflectionUtils.makeAccessible(resultField);
    ReflectionUtils.setField(resultField, listResponse, tables);

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(listResponse))
            .addHeader("Content-Type", "application/json"));
    List<HouseTable> returnList = htsRepo.findAllByDatabaseId(HOUSE_TABLE.getDatabaseId());
    assertThat(returnList).hasSize(2);
  }

  @Test
  public void testListWithEmptyResult() {
    // Shouldn't expect failure but gracefully getting an empty list.
    List<UserTable> tables = new ArrayList<>();
    GetAllEntityResponseBodyUserTable listResponse = new GetAllEntityResponseBodyUserTable();
    Field resultField =
        ReflectionUtils.findField(GetAllEntityResponseBodyUserTable.class, "results");
    Assertions.assertNotNull(resultField);
    ReflectionUtils.makeAccessible(resultField);
    ReflectionUtils.setField(resultField, listResponse, tables);

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(listResponse))
            .addHeader("Content-Type", "application/json"));

    List<HouseTable> returnList = htsRepo.findAllByDatabaseId(HOUSE_TABLE.getDatabaseId());
    assertThat(returnList).hasSize(0);
  }

  @Test
  public void testListOfAllTables() {
    List<UserTable> tables = new ArrayList<>();
    tables.add(houseTableMapper.toUserTableWithDatabaseId(HOUSE_TABLE));
    tables.add(houseTableMapper.toUserTableWithDatabaseId(HOUSE_TABLE_SAME_DB));
    tables.add(houseTableMapper.toUserTableWithDatabaseId(HOUSE_TABLE_DIFF_DB));
    GetAllEntityResponseBodyUserTable listResponse = new GetAllEntityResponseBodyUserTable();

    Field resultField =
        ReflectionUtils.findField(GetAllEntityResponseBodyUserTable.class, "results");
    Assertions.assertNotNull(resultField);
    ReflectionUtils.makeAccessible(resultField);
    ReflectionUtils.setField(resultField, listResponse, tables);

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(listResponse))
            .addHeader("Content-Type", "application/json"));
    Iterable<HouseTable> returnList = htsRepo.findAll();
    assertThat(returnList).hasSize(3);
  }

  @Test
  public void testRetryForFindByIdHtsCall() {
    // Injecting a Gateway timeout and an internal server error, will be translated to retryable
    // error. In fact only 500, 502, 503, 504 are retryable based on
    // com.linkedin.openhouse.internal.catalog.repository.HtsRetryUtils.getHtsRetryTemplate AND
    // com.linkedin.openhouse.internal.catalog.repository.HouseTableRepositoryImpl.handleHtsHttpError
    // All of them are covered in the following two tests.
    // Then inject a non retryable error(409) which should terminate the retry attempt and exception
    // will be
    // thrown directly.
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(504)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(500)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(409)
            .setBody("")
            .addHeader("Content-Type", "application/json"));

    HouseTablePrimaryKey testKey =
        HouseTablePrimaryKey.builder()
            .tableId(HOUSE_TABLE.getTableId())
            .databaseId(HOUSE_TABLE.getDatabaseId())
            .build();

    CustomRetryListener retryListener = new CustomRetryListener();
    ((HouseTableRepositoryImpl) htsRepo)
        .getHtsRetryTemplate(
            Arrays.asList(
                HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
        .registerListener(retryListener);
    Assertions.assertThrows(
        HouseTableConcurrentUpdateException.class, () -> htsRepo.findById(testKey));
    int actualRetryCount = retryListener.getRetryCount();
    Assertions.assertEquals(actualRetryCount, HtsRetryUtils.MAX_RETRY_ATTEMPT);
  }

  @Test
  public void testNoRetryForStateUnknown() {
    for (int i : Arrays.asList(500, 501, 502, 503, 504)) {
      mockHtsServer.enqueue(
          new MockResponse()
              .setResponseCode(i)
              .setBody("")
              .addHeader("Content-Type", "application/json"));
      CustomRetryListener retryListener = new CustomRetryListener();
      ((HouseTableRepositoryImpl) htsRepo)
          .getHtsRetryTemplate(Collections.singletonList(IllegalStateException.class))
          .registerListener(retryListener);
      Assertions.assertThrows(
          HouseTableRepositoryStateUnknownException.class, () -> htsRepo.save(HOUSE_TABLE));
      int actualRetryCount = retryListener.getRetryCount();
      // Should not be retrying table writes regardless of the error to avoid corruption
      Assertions.assertEquals(actualRetryCount, 0);
    }
  }

  @Test
  public void testRetryForHtsFindByIdCallOnConcurrentException() {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(409)
            .setBody("")
            .addHeader("Content-Type", "application/json"));

    CustomRetryListener retryListener = new CustomRetryListener();
    ((HouseTableRepositoryImpl) htsRepo)
        .getHtsRetryTemplate(
            Arrays.asList(
                HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
        .registerListener(retryListener);

    HouseTablePrimaryKey testKey =
        HouseTablePrimaryKey.builder()
            .tableId(HOUSE_TABLE.getTableId())
            .databaseId(HOUSE_TABLE.getDatabaseId())
            .build();
    Assertions.assertThrows(
        HouseTableConcurrentUpdateException.class, () -> htsRepo.findById(testKey));
    int actualRetryCount = retryListener.getRetryCount();
    Assertions.assertEquals(actualRetryCount, 1);
  }

  @Test
  public void testWriteTimeout() {
    EntityResponseBodyUserTable putResponse = new EntityResponseBodyUserTable();
    putResponse.entity(houseTableMapper.toUserTable(HOUSE_TABLE));
    int writeTimeout = 60;
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(putResponse))
            .setHeadersDelay(writeTimeout - 2, TimeUnit.SECONDS)
            .addHeader("Content-Type", "application/json"));
    Assertions.assertDoesNotThrow(() -> htsRepo.save(HOUSE_TABLE));
  }

  @Test
  public void testReadTimeoutWithRetries() {
    EntityResponseBodyUserTable response = new EntityResponseBodyUserTable();
    response.entity(houseTableMapper.toUserTable(HOUSE_TABLE));
    int readTimeout = 30;
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(response))
            .setHeadersDelay(readTimeout + 1, TimeUnit.SECONDS)
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(response))
            .setHeadersDelay(readTimeout + 1, TimeUnit.SECONDS)
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(response))
            .setHeadersDelay(0, TimeUnit.SECONDS) // Last attempt should pass
            .addHeader("Content-Type", "application/json"));

    CustomRetryListener retryListener = new CustomRetryListener();
    ((HouseTableRepositoryImpl) htsRepo)
        .getHtsRetryTemplate(
            Arrays.asList(
                HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
        .registerListener(retryListener);

    Assertions.assertDoesNotThrow(
        () ->
            htsRepo.findById(
                HouseTablePrimaryKey.builder()
                    .tableId(HOUSE_TABLE.getTableId())
                    .databaseId(HOUSE_TABLE.getDatabaseId())
                    .build()));
    Assertions.assertEquals(retryListener.getRetryCount(), 2);
  }

  @Test
  void testDnsResolverTimeoutDoesNotRetry() {
    DnsNameResolverTimeoutException mockDnsException =
        new DnsNameResolverTimeoutException(
            InetSocketAddress.createUnresolved("localhost", 8080),
            new DefaultDnsQuestion("test", DnsRecordType.ANY),
            "DNS resolution timeout");
    Mockito.doReturn(Mono.error(mockDnsException)).when(userTableApi).putUserTable(Mockito.any());

    CustomRetryListener retryListener = new CustomRetryListener();
    ((HouseTableRepositoryImpl) htsRepo)
        .getHtsRetryTemplate(
            Arrays.asList(
                HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
        .registerListener(retryListener);

    Assertions.assertThrows(
        HouseTableRepositoryStateUnknownException.class, () -> htsRepo.save(HOUSE_TABLE));
    int actualRetryCount = retryListener.getRetryCount();
    // Should not be retrying table writes regardless of the error to avoid corruption
    Assertions.assertEquals(actualRetryCount, 0);
  }
}
