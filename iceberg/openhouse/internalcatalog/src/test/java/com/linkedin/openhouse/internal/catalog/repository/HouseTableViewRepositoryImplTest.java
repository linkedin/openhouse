package com.linkedin.openhouse.internal.catalog.repository;

import static com.linkedin.openhouse.internal.catalog.HouseTableModelConstants.HOUSE_TABLE;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.openhouse.housetables.client.api.ToggleStatusApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyUserTable;
import com.linkedin.openhouse.housetables.client.model.GetAllEntityResponseBodyUserTable;
import com.linkedin.openhouse.housetables.client.model.PageUserTable;
import com.linkedin.openhouse.housetables.client.model.UserTable;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Mono;

/**
 * Wire-level behaviour of the typed view adapters on {@link HouseTableRepositoryImpl}. Driving a
 * real server rather than stubbing the generated client means a wrong route, verb, or body fails
 * here instead of in production.
 *
 * <p>This class targets the adapter, not the commit engine, which is why it lives beside the other
 * repository tests.
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest
public class HouseTableViewRepositoryImplTest {

  private static final String VIEW_DB = "viewdb";
  private static final String VIEW_ID = "v1";
  private static final String VIEW_METADATA_LOCATION = "/viewdb/v1-uuid/00001-abc.metadata.json";

  @Autowired
  @Qualifier("viewRepoTest")
  HouseTableRepository htsRepo;

  @Autowired HouseTableMapper houseTableMapper;

  @SpyBean UserTableApi userTableApi;

  @TestConfiguration
  public static class MockWebServerConfiguration {
    @Bean
    public UserTableApi provideMockHtsApiInstance() {
      return new UserTableApi(getMockServerApiClient());
    }

    @Bean
    public ToggleStatusApi provideMockHtsApiInstanceForToggle() {
      return new ToggleStatusApi(getMockServerApiClient());
    }

    private ApiClient getMockServerApiClient() {
      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(String.format("http://localhost:%s", mockHtsServer.getPort()));
      return apiClient;
    }

    @Bean
    @Qualifier("viewRepoTest")
    public HouseTableRepository provideRealHtsRepository() {
      return new HouseTableRepositoryImpl();
    }
  }

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

  /** The server outlives a method, so a queued response would otherwise leak to the next test. */
  @AfterEach
  void resetMockServerState() throws InterruptedException {
    mockHtsServer.setDispatcher(new QueueDispatcher());
    RecordedRequest leftover = mockHtsServer.takeRequest(1, TimeUnit.MILLISECONDS);
    while (leftover != null) {
      leftover = mockHtsServer.takeRequest(1, TimeUnit.MILLISECONDS);
    }
  }

  private static HouseTablePrimaryKey viewKey() {
    return HouseTablePrimaryKey.builder().databaseId(VIEW_DB).tableId(VIEW_ID).build();
  }

  private static HouseTable viewPointer() {
    return HouseTable.builder()
        .databaseId(VIEW_DB)
        .tableId(VIEW_ID)
        .tableLocation(VIEW_METADATA_LOCATION)
        .tableVersion("INITIAL_VERSION")
        .storageType("local")
        .build();
  }

  private UserTable viewUserTable(String entityType) {
    UserTable userTable = houseTableMapper.toUserTable(viewPointer());
    userTable.setEntityType(entityType);
    return userTable;
  }

  private void enqueueEntity(int code, UserTable entity) {
    EntityResponseBodyUserTable response = new EntityResponseBodyUserTable();
    response.entity(entity);
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(code)
            .setBody(new Gson().toJson(response))
            .addHeader("Content-Type", "application/json"));
  }

  private void enqueueStatus(int code) {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(code)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
  }

  private void enqueueViewPage(List<UserTable> views, int number, int size, long totalElements) {
    GetAllEntityResponseBodyUserTable pageResponse = new GetAllEntityResponseBodyUserTable();
    PageUserTable pageUserTable = new PageUserTable();
    pageUserTable.setContent(views);
    pageUserTable.setNumber(number);
    pageUserTable.setSize(size);
    pageUserTable.setTotalElements(totalElements);
    Field pageResults =
        ReflectionUtils.findField(GetAllEntityResponseBodyUserTable.class, "pageResults");
    Assertions.assertNotNull(pageResults);
    ReflectionUtils.makeAccessible(pageResults);
    ReflectionUtils.setField(pageResults, pageResponse, pageUserTable);
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody(new Gson().toJson(pageResponse))
            .addHeader("Content-Type", "application/json"));
  }

  private static RecordedRequest nextRequest() throws InterruptedException {
    RecordedRequest request = mockHtsServer.takeRequest(30, TimeUnit.SECONDS);
    Assertions.assertNotNull(request, "expected a request to reach House Table");
    return request;
  }

  private CustomRetryListener listenOnReadRetries() {
    CustomRetryListener retryListener = new CustomRetryListener();
    ((HouseTableRepositoryImpl) htsRepo)
        .getHtsRetryTemplate(
            Arrays.asList(
                HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
        .registerListener(retryListener);
    return retryListener;
  }

  private CustomRetryListener listenOnMutationRetries() {
    CustomRetryListener retryListener = new CustomRetryListener();
    ((HouseTableRepositoryImpl) htsRepo)
        .getHtsRetryTemplate(Collections.singletonList(IllegalStateException.class))
        .registerListener(retryListener);
    return retryListener;
  }

  /**
   * A write timeout of a minute is correct in production and useless in a test, so this instance
   * shortens only that seam and keeps every other behaviour of the real adapter. The returned repo
   * has its own retry template, so a listener must be bound to it rather than to the injected bean.
   */
  private HouseTableRepositoryImpl fastWriteTimeoutRepo() {
    HouseTableRepositoryImpl repo =
        new HouseTableRepositoryImpl() {
          @Override
          protected Duration writeRequestTimeout() {
            return Duration.ofMillis(250);
          }
        };
    ReflectionTestUtils.setField(repo, "apiInstance", userTableApi);
    ReflectionTestUtils.setField(repo, "houseTableMapper", houseTableMapper);
    return repo;
  }

  private static CustomRetryListener listenOnMutationRetriesOf(HouseTableRepositoryImpl repo) {
    CustomRetryListener retryListener = new CustomRetryListener();
    repo.getHtsRetryTemplate(Collections.singletonList(IllegalStateException.class))
        .registerListener(retryListener);
    return retryListener;
  }

  /* -------------------------------------------------------------------------
   * Generated-client endpoint wiring.
   * ---------------------------------------------------------------------- */

  /** Neutral occupancy resolves through the type-agnostic entity route. */
  @Test
  public void findEntityByIdCallsTheNeutralEntityEndpoint() throws InterruptedException {
    enqueueEntity(200, viewUserTable("VIEW"));

    Optional<HouseTable> found = htsRepo.findEntityById(viewKey());

    RecordedRequest request = nextRequest();
    Assertions.assertEquals("GET", request.getMethod());
    assertThat(request.getPath()).startsWith("/hts/entities?");
    assertThat(request.getPath()).contains("databaseId=" + VIEW_DB);
    assertThat(request.getPath()).contains("tableId=" + VIEW_ID);
    Assertions.assertTrue(found.isPresent());
    Assertions.assertEquals("VIEW", found.get().getEntityType());
    Assertions.assertEquals(VIEW_METADATA_LOCATION, found.get().getTableLocation());
  }

  /**
   * House Table resolves a legacy null to TABLE before the wire, so only canonical values arrive.
   */
  @Test
  public void findEntityByIdMapsTheCanonicalTableDiscriminator() throws InterruptedException {
    enqueueEntity(200, viewUserTable("TABLE"));

    Optional<HouseTable> found = htsRepo.findEntityById(viewKey());

    nextRequest();
    Assertions.assertTrue(found.isPresent());
    Assertions.assertEquals("TABLE", found.get().getEntityType());
    Assertions.assertEquals(VIEW_METADATA_LOCATION, found.get().getTableLocation());
  }

  @Test
  public void findViewByIdCallsTheTypedViewEndpoint() throws InterruptedException {
    enqueueEntity(200, viewUserTable("VIEW"));

    Optional<HouseTable> found = htsRepo.findViewById(viewKey());

    RecordedRequest request = nextRequest();
    Assertions.assertEquals("GET", request.getMethod());
    assertThat(request.getPath()).startsWith("/hts/views?");
    assertThat(request.getPath()).contains("databaseId=" + VIEW_DB);
    assertThat(request.getPath()).contains("tableId=" + VIEW_ID);
    Assertions.assertTrue(found.isPresent());
    Assertions.assertEquals(VIEW_METADATA_LOCATION, found.get().getTableLocation());
    Assertions.assertEquals("local", found.get().getStorageType());
    Assertions.assertEquals("VIEW", found.get().getEntityType());
  }

  /** A 404 on a typed lookup reads as absent: a table at a view key is simply not there. */
  @Test
  public void findViewByIdTreatsNotFoundAsAbsent() {
    enqueueStatus(404);

    Assertions.assertFalse(htsRepo.findViewById(viewKey()).isPresent());
  }

  @Test
  public void findAllViewsByDatabaseIdCallsThePaginatedViewQueryEndpoint()
      throws InterruptedException {
    List<UserTable> views = new ArrayList<>();
    views.add(viewUserTable("VIEW"));
    UserTable second = viewUserTable("VIEW");
    second.setTableId("v2");
    views.add(second);
    enqueueViewPage(views, 0, 2, 5L);

    Pageable pageable = PageRequest.of(0, 2, Sort.unsorted());
    Page<HouseTable> page = htsRepo.findAllViewsByDatabaseId(VIEW_DB, pageable);

    RecordedRequest request = nextRequest();
    Assertions.assertEquals("GET", request.getMethod());
    assertThat(request.getPath()).startsWith("/v1/hts/views/query?");
    assertThat(request.getPath()).contains("databaseId=" + VIEW_DB);
    assertThat(request.getPath()).contains("page=0");
    assertThat(request.getPath()).contains("size=2");

    Assertions.assertEquals(2, page.getContent().size());
    Assertions.assertEquals(5L, page.getTotalElements());
    Assertions.assertEquals(3, page.getTotalPages());
    Assertions.assertTrue(
        page.getContent().stream().allMatch(row -> "VIEW".equals(row.getEntityType())));
  }

  /** Entity type is required by the contract, so this client states it rather than inferring. */
  @Test
  public void saveViewPutsToTheTypedViewRouteDeclaringTheViewEntityType()
      throws InterruptedException {
    enqueueEntity(201, viewUserTable("VIEW"));

    HouseTable saved = htsRepo.saveView(viewPointer());

    RecordedRequest request = nextRequest();
    Assertions.assertEquals("PUT", request.getMethod());
    assertThat(request.getPath()).isEqualTo("/hts/views");

    JsonObject body = JsonParser.parseString(request.getBody().readUtf8()).getAsJsonObject();
    JsonObject entity = body.getAsJsonObject("entity");
    JsonElement declaredEntityType = entity.get("entityType");
    Assertions.assertTrue(
        declaredEntityType != null && !declaredEntityType.isJsonNull(),
        "outgoing view body must declare its entity type");
    Assertions.assertEquals(
        "VIEW",
        declaredEntityType.getAsString(),
        "the view route must be told, in the canonical spelling, that it is receiving a view");
    Assertions.assertEquals(VIEW_METADATA_LOCATION, entity.get("metadataLocation").getAsString());
    Assertions.assertEquals("INITIAL_VERSION", entity.get("tableVersion").getAsString());

    Assertions.assertEquals(VIEW_METADATA_LOCATION, saved.getTableLocation());
  }

  @Test
  public void deleteViewByIdCallsTheTypedViewDeleteEndpoint() throws InterruptedException {
    enqueueStatus(204);

    Assertions.assertTrue(htsRepo.deleteViewById(viewKey()));

    RecordedRequest request = nextRequest();
    Assertions.assertEquals("DELETE", request.getMethod());
    assertThat(request.getPath()).startsWith("/hts/views?");
    assertThat(request.getPath()).contains("databaseId=" + VIEW_DB);
    assertThat(request.getPath()).contains("tableId=" + VIEW_ID);
  }

  @Test
  public void deleteViewByIdReportsFalseWhenTheViewIsNotThere() {
    enqueueStatus(404);

    Assertions.assertFalse(htsRepo.deleteViewById(viewKey()));
  }

  /* -------------------------------------------------------------------------
   * Typed-view contract defence. Only a present, non-canonical discriminator on a
   * typed VIEW response is a violation; a valid table at a view key is absent, and the
   * neutral route has no expected type at all.
   * ---------------------------------------------------------------------- */

  /** A counting wrapper: a retried integrity failure subscribes the same publisher twice. */
  private AtomicInteger stubViewPointRead(EntityResponseBodyUserTable response) {
    AtomicInteger subscriptions = new AtomicInteger();
    Mono<EntityResponseBodyUserTable> counted =
        Mono.fromSupplier(
            () -> {
              subscriptions.incrementAndGet();
              return response;
            });
    Mockito.doReturn(counted).when(userTableApi).getUserView(Mockito.any(), Mockito.any());
    return subscriptions;
  }

  private static EntityResponseBodyUserTable entityBody(UserTable entity) {
    EntityResponseBodyUserTable response = new EntityResponseBodyUserTable();
    response.entity(entity);
    return response;
  }

  /**
   * The server cannot answer the view route with a table, so a response that does is corruption,
   * not a miss. Reporting it as absent would let a create overwrite a live row.
   */
  @Test
  public void findViewByIdRejectsAPresentNonViewDiscriminatorWithoutRetrying() {
    for (String corrupt : Arrays.asList("TABLE", "view", "View", "MATERIALIZED_VIEW")) {
      AtomicInteger subscriptions = stubViewPointRead(entityBody(viewUserTable(corrupt)));
      CustomRetryListener retryListener = listenOnReadRetries();

      IllegalStateException thrown =
          Assertions.assertThrows(
              IllegalStateException.class,
              () -> htsRepo.findViewById(viewKey()),
              "discriminator " + corrupt);

      assertThat(thrown.getMessage()).contains(VIEW_DB).contains(VIEW_ID).contains(corrupt);
      Assertions.assertEquals(
          1,
          subscriptions.get(),
          "a contract violation is not a transport failure and must not be retried: " + corrupt);
      Assertions.assertEquals(0, retryListener.getRetryCount(), "discriminator " + corrupt);
      Mockito.clearInvocations(userTableApi);
    }
  }

  /** Blank is not a spelling of VIEW either, and it must not read as a missing field. */
  @Test
  public void findViewByIdRejectsABlankDiscriminatorWithoutRetrying() {
    AtomicInteger subscriptions = stubViewPointRead(entityBody(viewUserTable("")));
    CustomRetryListener retryListener = listenOnReadRetries();

    IllegalStateException thrown =
        Assertions.assertThrows(IllegalStateException.class, () -> htsRepo.findViewById(viewKey()));

    assertThat(thrown.getMessage()).contains(VIEW_DB).contains(VIEW_ID);
    Assertions.assertEquals(1, subscriptions.get());
    Assertions.assertEquals(0, retryListener.getRetryCount());
  }

  @Test
  public void findViewByIdRejectsAMissingDiscriminatorWithoutRetrying() {
    AtomicInteger subscriptions = stubViewPointRead(entityBody(viewUserTable(null)));
    CustomRetryListener retryListener = listenOnReadRetries();

    IllegalStateException thrown =
        Assertions.assertThrows(IllegalStateException.class, () -> htsRepo.findViewById(viewKey()));

    assertThat(thrown.getMessage()).contains(VIEW_DB).contains(VIEW_ID);
    Assertions.assertEquals(1, subscriptions.get());
    Assertions.assertEquals(0, retryListener.getRetryCount());
  }

  /** One bad row fails the page: dropping it would hide corruption and break the totals. */
  @Test
  public void findAllViewsByDatabaseIdRejectsAnyNonViewRowOnThePage() throws InterruptedException {
    for (String corrupt : Arrays.asList("TABLE", "view", "View", "MATERIALIZED_VIEW", "")) {
      List<UserTable> views = new ArrayList<>();
      views.add(viewUserTable("VIEW"));
      UserTable bad = viewUserTable(corrupt);
      bad.setTableId("v2");
      views.add(bad);
      enqueueViewPage(views, 0, 2, 2L);

      CustomRetryListener retryListener = listenOnReadRetries();

      IllegalStateException thrown =
          Assertions.assertThrows(
              IllegalStateException.class,
              () ->
                  htsRepo.findAllViewsByDatabaseId(VIEW_DB, PageRequest.of(0, 2, Sort.unsorted())),
              "discriminator " + corrupt);

      assertThat(thrown.getMessage()).contains("v2");
      Assertions.assertEquals(
          0,
          retryListener.getRetryCount(),
          "a corrupt page is not retried into a second read, discriminator " + corrupt);
      nextRequest();
      Assertions.assertNull(
          mockHtsServer.takeRequest(1, TimeUnit.SECONDS),
          "the corrupt page must not be re-fetched, discriminator " + corrupt);
    }
  }

  @Test
  public void findAllViewsByDatabaseIdRejectsAMissingDiscriminatorOnThePage()
      throws InterruptedException {
    enqueueViewPage(Collections.singletonList(viewUserTable(null)), 0, 1, 1L);

    CustomRetryListener retryListener = listenOnReadRetries();

    IllegalStateException thrown =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> htsRepo.findAllViewsByDatabaseId(VIEW_DB, PageRequest.of(0, 1, Sort.unsorted())));

    assertThat(thrown.getMessage()).contains(VIEW_DB).contains(VIEW_ID);
    assertThat(thrown.getMessage().toLowerCase())
        .as("a missing discriminator must be described, not left to be inferred")
        .containsAnyOf("null", "missing", "absent");
    Assertions.assertEquals(0, retryListener.getRetryCount());
    nextRequest();
    Assertions.assertNull(mockHtsServer.takeRequest(1, TimeUnit.SECONDS));
  }

  /** The untyped route has no expected type to violate, so it classifies nothing. */
  @Test
  public void findEntityByIdNeverAppliesTheTypedViewDefence() throws InterruptedException {
    for (String occupant : Arrays.asList("TABLE", "MATERIALIZED_VIEW", "view")) {
      enqueueEntity(200, viewUserTable(occupant));

      Optional<HouseTable> found = htsRepo.findEntityById(viewKey());

      Assertions.assertTrue(found.isPresent(), occupant);
      Assertions.assertEquals(
          occupant,
          found.get().getEntityType(),
          "the occupant's type is reported verbatim for the engine to classify");
      nextRequest();
    }
  }

  /* -------------------------------------------------------------------------
   * One write, no blind retry.
   * ---------------------------------------------------------------------- */

  /** Retrying could double-apply, so the adapter sends once and reports unknown state. */
  @Test
  public void saveViewSendsExactlyOneRequestPerAmbiguousServerError() throws InterruptedException {
    for (int code : Arrays.asList(500, 504)) {
      enqueueStatus(code);
      CustomRetryListener retryListener = listenOnMutationRetries();

      Assertions.assertThrows(
          HouseTableRepositoryStateUnknownException.class,
          () -> htsRepo.saveView(viewPointer()),
          String.valueOf(code));

      RecordedRequest request = nextRequest();
      Assertions.assertEquals("PUT", request.getMethod());
      assertThat(request.getPath()).isEqualTo("/hts/views");
      Assertions.assertNull(
          mockHtsServer.takeRequest(1, TimeUnit.SECONDS),
          "a view write must never be retried, code " + code);
      Mockito.verify(userTableApi, Mockito.times(1)).putUserView(Mockito.any());
      Mockito.clearInvocations(userTableApi);
      Assertions.assertEquals(
          0, retryListener.getRetryCount(), "no retry may be recorded for a view write");
    }
  }

  /** The request left this process before the connection died, so the outcome is unknown. */
  @Test
  public void saveViewReportsUnknownStateWhenTheConnectionDiesAfterTheRequestIsSent()
      throws InterruptedException {
    mockHtsServer.enqueue(
        new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST));

    CustomRetryListener retryListener = listenOnMutationRetries();

    Assertions.assertThrows(
        HouseTableRepositoryStateUnknownException.class, () -> htsRepo.saveView(viewPointer()));

    Mockito.verify(userTableApi, Mockito.times(1)).putUserView(Mockito.any());
    RecordedRequest sent = nextRequest();
    Assertions.assertEquals("PUT", sent.getMethod());
    assertThat(sent.getPath()).isEqualTo("/hts/views");
    Assertions.assertNull(
        mockHtsServer.takeRequest(1, TimeUnit.SECONDS),
        "the request already left this process; resending it could double-apply");
    Assertions.assertEquals(0, retryListener.getRetryCount());
  }

  /**
   * A publisher that never completes, so the {@code timeout} operator itself has to produce the
   * failure. An injected {@code IllegalStateException} would pass even with the operator deleted.
   * The outer deadline turns a deleted operator into a bounded failure instead of a hung worker.
   */
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  public void saveViewReportsUnknownStateOnARealWriteTimeout() {
    AtomicInteger subscriptions = new AtomicInteger();
    Mockito.doReturn(
            Mono.<EntityResponseBodyUserTable>never()
                .doOnSubscribe(subscription -> subscriptions.incrementAndGet()))
        .when(userTableApi)
        .putUserView(Mockito.any());

    HouseTableRepositoryImpl repo = fastWriteTimeoutRepo();
    CustomRetryListener retryListener = listenOnMutationRetriesOf(repo);

    HouseTableRepositoryStateUnknownException thrown =
        Assertions.assertThrows(
            HouseTableRepositoryStateUnknownException.class, () -> repo.saveView(viewPointer()));

    Assertions.assertTrue(
        thrown.getCause() instanceof TimeoutException,
        "an elapsed write budget must be reported as a timeout, got: " + thrown.getCause());
    Assertions.assertEquals(1, subscriptions.get(), "a timed-out write must never be resubscribed");
    Mockito.verify(userTableApi, Mockito.times(1)).putUserView(Mockito.any());
    Assertions.assertEquals(0, retryListener.getRetryCount());
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  public void deleteViewByIdReportsUnknownStateOnARealWriteTimeout() {
    AtomicInteger subscriptions = new AtomicInteger();
    Mockito.doReturn(
            Mono.<Void>never().doOnSubscribe(subscription -> subscriptions.incrementAndGet()))
        .when(userTableApi)
        .deleteView(Mockito.any(), Mockito.any());

    HouseTableRepositoryImpl repo = fastWriteTimeoutRepo();
    CustomRetryListener retryListener = listenOnMutationRetriesOf(repo);

    HouseTableRepositoryStateUnknownException thrown =
        Assertions.assertThrows(
            HouseTableRepositoryStateUnknownException.class, () -> repo.deleteViewById(viewKey()));

    Assertions.assertTrue(
        thrown.getCause() instanceof TimeoutException,
        "an elapsed delete budget must be reported as a timeout, got: " + thrown.getCause());
    Assertions.assertEquals(
        1, subscriptions.get(), "a timed-out delete must never be resubscribed");
    Mockito.verify(userTableApi, Mockito.times(1)).deleteView(Mockito.any(), Mockito.any());
    Assertions.assertEquals(0, retryListener.getRetryCount());
  }

  /** A losing swap is a conflict, never retried into a second write. */
  @Test
  public void saveViewSurfacesConflictWithoutRetrying() throws InterruptedException {
    enqueueStatus(409);
    CustomRetryListener retryListener = listenOnMutationRetries();

    Assertions.assertThrowsExactly(
        HouseTableConcurrentUpdateException.class, () -> htsRepo.saveView(viewPointer()));

    nextRequest();
    Assertions.assertNull(mockHtsServer.takeRequest(1, TimeUnit.SECONDS));
    Assertions.assertEquals(0, retryListener.getRetryCount());
  }

  /** Caller failures keep their classification so the later layer can preserve status. */
  @Test
  public void saveViewPreservesCallerFailuresWithoutRetrying() throws InterruptedException {
    for (int code : Arrays.asList(400, 401, 403, 429)) {
      enqueueStatus(code);
      CustomRetryListener retryListener = listenOnMutationRetries();

      Assertions.assertThrowsExactly(
          HouseTableCallerException.class,
          () -> htsRepo.saveView(viewPointer()),
          String.valueOf(code));

      Mockito.verify(userTableApi, Mockito.times(1)).putUserView(Mockito.any());
      Mockito.clearInvocations(userTableApi);
      nextRequest();
      Assertions.assertNull(
          mockHtsServer.takeRequest(1, TimeUnit.SECONDS),
          "a caller failure must not be retried, code " + code);
      Assertions.assertEquals(0, retryListener.getRetryCount(), "code " + code);
    }

    enqueueStatus(404);
    Assertions.assertThrowsExactly(
        HouseTableNotFoundException.class, () -> htsRepo.saveView(viewPointer()));
    Mockito.verify(userTableApi, Mockito.times(1)).putUserView(Mockito.any());
  }

  /** An empty success signal never reaches the error handler, so the guard has to catch it. */
  @Test
  public void saveViewReportsUnknownStateWhenThePublisherCompletesEmpty() {
    AtomicInteger subscriptions = new AtomicInteger();
    Mockito.doReturn(
            Mono.<EntityResponseBodyUserTable>empty()
                .doOnSubscribe(subscription -> subscriptions.incrementAndGet()))
        .when(userTableApi)
        .putUserView(Mockito.any());

    CustomRetryListener retryListener = listenOnMutationRetries();

    HouseTableRepositoryStateUnknownException thrown =
        Assertions.assertThrows(
            HouseTableRepositoryStateUnknownException.class, () -> htsRepo.saveView(viewPointer()));
    assertThat(thrown.getMessage()).contains("no entity");

    Assertions.assertEquals(1, subscriptions.get(), "an ambiguous write is never resubscribed");
    Mockito.verify(userTableApi, Mockito.times(1)).putUserView(Mockito.any());
    Assertions.assertEquals(0, retryListener.getRetryCount());
  }

  /** A 200 whose body carries no entity leaves the same ambiguity as an empty completion. */
  @Test
  public void saveViewReportsUnknownStateWhenTheResponseCarriesNoEntity()
      throws InterruptedException {
    enqueueEntity(200, null);

    Assertions.assertThrows(
        HouseTableRepositoryStateUnknownException.class, () -> htsRepo.saveView(viewPointer()));

    RecordedRequest request = nextRequest();
    Assertions.assertEquals("PUT", request.getMethod());
    Assertions.assertNull(
        mockHtsServer.takeRequest(1, TimeUnit.SECONDS), "an ambiguous write is never resent");
  }

  /* -------------------------------------------------------------------------
   * The paginated list is a read, so it keeps the shared bounded retry.
   * ---------------------------------------------------------------------- */

  /** A read is safe to repeat, so the list must engage the bounded retry, not escape raw. */
  @Test
  public void findAllViewsByDatabaseIdRetriesATransientServerErrorAndThenSucceeds()
      throws InterruptedException {
    enqueueStatus(503);
    enqueueViewPage(Collections.singletonList(viewUserTable("VIEW")), 0, 1, 1L);

    CustomRetryListener retryListener = listenOnReadRetries();

    Page<HouseTable> page =
        htsRepo.findAllViewsByDatabaseId(VIEW_DB, PageRequest.of(0, 1, Sort.unsorted()));

    Assertions.assertEquals(1, page.getContent().size());
    Assertions.assertEquals(1, retryListener.getRetryCount());
    assertThat(nextRequest().getPath()).startsWith("/v1/hts/views/query?");
    assertThat(nextRequest().getPath()).startsWith("/v1/hts/views/query?");
    Assertions.assertNull(mockHtsServer.takeRequest(1, TimeUnit.SECONDS));
  }

  /** Bounded: once the attempts are used up the classified failure is reported. */
  @Test
  public void findAllViewsByDatabaseIdStopsAfterTheConfiguredNumberOfAttempts()
      throws InterruptedException {
    for (int i = 0; i < HtsRetryUtils.MAX_RETRY_ATTEMPT; i++) {
      enqueueStatus(500);
    }

    CustomRetryListener retryListener = listenOnReadRetries();

    Assertions.assertThrows(
        HouseTableRepositoryStateUnknownException.class,
        () -> htsRepo.findAllViewsByDatabaseId(VIEW_DB, PageRequest.of(0, 1, Sort.unsorted())));

    Assertions.assertEquals(HtsRetryUtils.MAX_RETRY_ATTEMPT, retryListener.getRetryCount());
    for (int i = 0; i < HtsRetryUtils.MAX_RETRY_ATTEMPT; i++) {
      Assertions.assertEquals("GET", nextRequest().getMethod());
    }
    Assertions.assertNull(
        mockHtsServer.takeRequest(1, TimeUnit.SECONDS), "the bound must stop further attempts");
  }

  /* -------------------------------------------------------------------------
   * The typed delete is a mutation too, and gets the same one-shot treatment.
   * ---------------------------------------------------------------------- */

  @Test
  public void deleteViewByIdSendsExactlyOneRequestPerAmbiguousServerError()
      throws InterruptedException {
    for (int code : Arrays.asList(500, 504)) {
      enqueueStatus(code);
      CustomRetryListener retryListener = listenOnMutationRetries();

      Assertions.assertThrows(
          HouseTableRepositoryStateUnknownException.class,
          () -> htsRepo.deleteViewById(viewKey()),
          String.valueOf(code));

      RecordedRequest request = nextRequest();
      Assertions.assertEquals("DELETE", request.getMethod());
      assertThat(request.getPath()).startsWith("/hts/views?");
      Assertions.assertNull(
          mockHtsServer.takeRequest(1, TimeUnit.SECONDS),
          "a view delete must never be retried, code " + code);
      Mockito.verify(userTableApi, Mockito.times(1)).deleteView(Mockito.any(), Mockito.any());
      Mockito.clearInvocations(userTableApi);
      Assertions.assertEquals(0, retryListener.getRetryCount(), "code " + code);
    }
  }

  @Test
  public void deleteViewByIdReportsUnknownStateWhenTheConnectionDiesAfterTheRequestIsSent()
      throws InterruptedException {
    mockHtsServer.enqueue(
        new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST));

    CustomRetryListener retryListener = listenOnMutationRetries();

    Assertions.assertThrows(
        HouseTableRepositoryStateUnknownException.class, () -> htsRepo.deleteViewById(viewKey()));

    Mockito.verify(userTableApi, Mockito.times(1)).deleteView(Mockito.any(), Mockito.any());
    nextRequest();
    Assertions.assertNull(mockHtsServer.takeRequest(1, TimeUnit.SECONDS));
    Assertions.assertEquals(0, retryListener.getRetryCount());
  }

  /** Same reasoning as the write: a mutation retry is never safe. */
  @Test
  public void deleteViewByIdPreservesCallerFailuresWithoutRetrying() throws InterruptedException {
    for (int code : Arrays.asList(400, 401, 403, 429)) {
      enqueueStatus(code);
      CustomRetryListener retryListener = listenOnMutationRetries();

      Assertions.assertThrowsExactly(
          HouseTableCallerException.class,
          () -> htsRepo.deleteViewById(viewKey()),
          String.valueOf(code));

      Mockito.verify(userTableApi, Mockito.times(1)).deleteView(Mockito.any(), Mockito.any());
      Mockito.clearInvocations(userTableApi);
      RecordedRequest request = nextRequest();
      Assertions.assertEquals("DELETE", request.getMethod());
      assertThat(request.getPath()).startsWith("/hts/views?");
      Assertions.assertNull(
          mockHtsServer.takeRequest(1, TimeUnit.SECONDS),
          "a caller failure must not be retried, code " + code);
      Assertions.assertEquals(0, retryListener.getRetryCount(), "code " + code);
    }
  }

  /** A typed delete never negotiates the table route's soft-delete flag. */
  @Test
  public void deleteViewByIdNeverTouchesTheTableDeleteRoutes() throws InterruptedException {
    enqueueStatus(204);

    Assertions.assertTrue(htsRepo.deleteViewById(viewKey()));

    RecordedRequest request = nextRequest();
    assertThat(request.getPath()).startsWith("/hts/views?");
    assertThat(request.getPath()).doesNotContain("isSoftDelete");
    Mockito.verify(userTableApi, Mockito.never())
        .deleteTable(Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.verify(userTableApi, Mockito.never()).deleteTable1(Mockito.any(), Mockito.any());
  }

  /* -------------------------------------------------------------------------
   * Reads keep the existing bounded retry.
   * ---------------------------------------------------------------------- */

  /** Reads are safe to repeat, so the typed read keeps the shared bounded retry. */
  @Test
  public void findViewByIdRetriesTransientServerErrorsUpToTheConfiguredBound() {
    enqueueStatus(504);
    enqueueStatus(500);
    enqueueStatus(409);

    CustomRetryListener retryListener = listenOnReadRetries();

    Assertions.assertThrows(
        HouseTableConcurrentUpdateException.class, () -> htsRepo.findViewById(viewKey()));
    Assertions.assertEquals(HtsRetryUtils.MAX_RETRY_ATTEMPT, retryListener.getRetryCount());
  }

  /** The occupancy read is a read too, and gets the same bounded retry. */
  @Test
  public void findEntityByIdRetriesTransientServerErrors() {
    enqueueStatus(503);
    enqueueEntity(200, viewUserTable("TABLE"));

    CustomRetryListener retryListener = listenOnReadRetries();

    Optional<HouseTable> found = htsRepo.findEntityById(viewKey());

    Assertions.assertTrue(found.isPresent());
    Assertions.assertEquals("TABLE", found.get().getEntityType());
    Assertions.assertEquals(1, retryListener.getRetryCount());
  }

  /** An absent key is absent on the neutral route too, and is not an error. */
  @Test
  public void findEntityByIdTreatsNotFoundAsAbsent() {
    enqueueStatus(404);

    Assertions.assertFalse(htsRepo.findEntityById(viewKey()).isPresent());
  }

  /** The table point read is untouched by any of this and keeps its own route. */
  @Test
  public void tablePointersRemainReachableOnlyThroughTheTableRoutes() throws InterruptedException {
    enqueueEntity(200, houseTableMapper.toUserTable(HOUSE_TABLE));

    HouseTable table =
        htsRepo
            .findById(
                HouseTablePrimaryKey.builder()
                    .databaseId(HOUSE_TABLE.getDatabaseId())
                    .tableId(HOUSE_TABLE.getTableId())
                    .build())
            .get();

    RecordedRequest request = nextRequest();
    assertThat(request.getPath()).startsWith("/hts/tables?");
    Assertions.assertEquals(HOUSE_TABLE.getTableLocation(), table.getTableLocation());
  }
}
