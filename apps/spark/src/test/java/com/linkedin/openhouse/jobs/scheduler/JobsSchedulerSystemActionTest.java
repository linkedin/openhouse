package com.linkedin.openhouse.jobs.scheduler;

import static com.linkedin.openhouse.common.utils.SystemActionContext.HTTP_HEADER_SYSTEM_ACTION;
import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.TablesClientFactory;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class JobsSchedulerSystemActionTest {
  private static final String TOKEN = "test-only-jobs-auth-token";
  private static final String TABLE_PATH = "/v1/databases/db/tables/table";

  @ParameterizedTest
  @CsvSource({
    "SNAPSHOTS_EXPIRATION,false",
    "SNAPSHOTS_EXPIRATION,true",
    "ORPHAN_FILES_DELETION,false"
  })
  void mainConfiguresMetadataClientBeforeScheduling(JobConf.JobTypeEnum type, boolean systemAction)
      throws Exception {
    BlockingQueue<List<String>> requests = new LinkedBlockingQueue<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> respond(exchange, requests));
    server.start();
    try {
      String[] args = args(type, systemAction, "http://127.0.0.1:" + server.getAddress().getPort());
      TablesClient client = runMainWithoutLaunchingJobs(args);
      assertMetadataRequests(client, requests, systemAction);

      List<GetTableResponseBody> shallowTables =
          client.getAllTablesAsync("db").block(Duration.ofSeconds(10));
      assertNotNull(shallowTables);
      assertEquals(1, shallowTables.size());
      assertRequest(requests, "/v1/databases/db/tables/search", systemAction);
      TableMetadata asyncMetadata =
          client.getTableMetadataAsync(shallowTables.get(0)).block(Duration.ofSeconds(10));
      assertNotNull(asyncMetadata);
      assertEquals("table-owner", asyncMetadata.getCreator());
      assertRequest(requests, TABLE_PATH, systemAction);

      // A later default client must not inherit a declaration through the shared API factory.
      CommandLine commandLine = JobsScheduler.parseArgs(args);
      TablesClientFactory factory = JobsScheduler.getTablesClientFactory(commandLine);
      assertMetadataRequests(factory.create(), requests, false);
      if (systemAction) {
        assertMetadataRequests(factory.create(true), requests, true);
        assertMetadataRequests(factory.create(false), requests, false);
        assertMetadataRequests(factory.create(), requests, false);
      }
      assertTrue(requests.isEmpty());
    } finally {
      server.stop(0);
    }
  }

  private static TablesClient runMainWithoutLaunchingJobs(String[] args) throws Exception {
    FutureTask<TablesClient> main =
        new FutureTask<>(
            () -> {
              List<TablesClient> clients = new ArrayList<>();
              try (MockedConstruction<JobsScheduler> schedulers =
                  Mockito.mockConstruction(
                      JobsScheduler.class,
                      (scheduler, context) -> {
                        clients.add((TablesClient) context.arguments().get(3));
                        ((ThreadPoolExecutor) context.arguments().get(0)).shutdown();
                      })) {
                JobsScheduler.main(args);
                assertEquals(1, schedulers.constructed().size());
                return clients.get(0);
              }
            });
    // main registers a shutdown hook that joins its caller; let that caller finish before JVM exit.
    new Thread(main, "jobs-scheduler-cli-test").start();
    return main.get(30, TimeUnit.SECONDS);
  }

  @ParameterizedTest
  @EnumSource(
      value = JobConf.JobTypeEnum.class,
      names = "SNAPSHOTS_EXPIRATION",
      mode = EnumSource.Mode.EXCLUDE)
  void declarationRejectsOtherJobTypes(JobConf.JobTypeEnum type) {
    RuntimeException error =
        assertThrows(
            RuntimeException.class,
            () -> JobsScheduler.parseArgs(args(type, true, "http://127.0.0.1:1")));
    assertInstanceOf(ParseException.class, error.getCause());
    assertTrue(error.getCause().getMessage().contains("--systemAction"));
    assertTrue(error.getCause().getMessage().contains("SNAPSHOTS_EXPIRATION"));
  }

  private static String[] args(JobConf.JobTypeEnum type, boolean systemAction, String baseUrl) {
    List<String> args =
        new ArrayList<>(
            Arrays.asList(
                "--type",
                type.getValue(),
                "--cluster",
                "test",
                "--tablesURL",
                baseUrl,
                "--jobsURL",
                baseUrl,
                "--tableMinAgeThresholdHours",
                "0",
                "--tokenFile",
                Paths.get("../../services/jobs/src/test/resources/test-jobs-auth-token.txt")
                    .toAbsolutePath()
                    .toString()));
    if (systemAction) {
      args.add("--systemAction");
    }
    return args.toArray(new String[0]);
  }

  private static void assertRequest(
      BlockingQueue<List<String>> requests, String path, boolean systemAction)
      throws InterruptedException {
    List<String> request = requests.poll(5, TimeUnit.SECONDS);
    assertNotNull(request);
    assertEquals(path, request.get(0));
    assertEquals("Bearer " + TOKEN, request.get(1));
    assertEquals(systemAction ? "true" : null, request.get(2));
  }

  private static void assertMetadataRequests(
      TablesClient client, BlockingQueue<List<String>> requests, boolean systemAction)
      throws InterruptedException {
    List<TableMetadata> metadata = client.getTableMetadataList();
    assertEquals(1, metadata.size());
    assertEquals("table-owner", metadata.get(0).getCreator());
    assertRequest(requests, "/v1/databases", systemAction);
    assertRequest(requests, "/v1/databases/db/tables/search", systemAction);
    assertRequest(requests, TABLE_PATH, systemAction);
  }

  private static void respond(HttpExchange exchange, BlockingQueue<List<String>> requests)
      throws IOException {
    String path = exchange.getRequestURI().getPath();
    requests.add(
        Arrays.asList(
            path,
            exchange.getRequestHeaders().getFirst("Authorization"),
            exchange.getRequestHeaders().getFirst(HTTP_HEADER_SYSTEM_ACTION)));
    String body;
    switch (path) {
      case "/v1/databases":
        body = "{\"results\":[{\"databaseId\":\"db\"}]}";
        break;
      case "/v1/databases/db/tables/search":
        body = "{\"results\":[{\"databaseId\":\"db\",\"tableId\":\"table\"}]}";
        break;
      case TABLE_PATH:
        body =
            "{\"databaseId\":\"db\",\"tableId\":\"table\",\"tableCreator\":\"table-owner\","
                + "\"tableType\":\"PRIMARY_TABLE\",\"creationTime\":1}";
        break;
      default:
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
        return;
    }
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, bytes.length);
    try (java.io.OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    } finally {
      exchange.close();
    }
  }
}
