package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.javaclient.exception.WebClientRequestWithMessageException;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.spark.SparkTestBase;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.apache.spark.sql.AnalysisException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SparkTestBase.class)
public class UnlockStatementTest {

  @BeforeEach
  void enableUnlock() {
    spark.conf().set("spark.sql.openhouse.unlock.enabled", true);
  }

  @AfterEach
  void resetUnlock() {
    spark.conf().unset("spark.sql.openhouse.unlock.enabled");
  }

  @Test
  public void testUnlockUsesSingleDeleteWithoutTableLoad() {
    List<String> requests = recordRequests(new MockResponse().setResponseCode(204));
    spark.sql("ALTER TABLE openhouse.dunlock.t1 UNLOCK REASON SYSTEM_ONLY");
    Assertions.assertEquals(
        Collections.singletonList("DELETE /v1/databases/dunlock/tables/t1/lock/SYSTEM_ONLY"),
        requests);
  }

  @ParameterizedTest
  @CsvSource({"LEGACY,204", "SYSTEM_ONLY,204", "SYSTEM_ONLY,409"})
  public void testSuccessLogIncludesTargetAndReasonOnlyAfterSuccess(String reason, int status) {
    recordRequests(new MockResponse().setResponseCode(status));
    Logger logger = Logger.getLogger("com.linkedin.openhouse.javaclient.OpenHouseCatalog");
    Level previousLevel = logger.getLevel();
    StringWriter output = new StringWriter();
    WriterAppender appender = new WriterAppender(new PatternLayout("%p %m%n"), output);
    logger.addAppender(appender);
    logger.setLevel(Level.DEBUG);
    try {
      String sql = "ALTER TABLE openhouse.dunlock.t1 UNLOCK REASON " + reason;
      if (status == 204) {
        spark.sql(sql);
        Assertions.assertTrue(
            output
                .toString()
                .contains(
                    "INFO Calling unlockTable succeeded with identifier: dunlock.t1, reason: "
                        + reason),
            output.toString());
      } else {
        Assertions.assertThrows(WebClientResponseWithMessageException.class, () -> spark.sql(sql));
        Assertions.assertFalse(output.toString().contains("Calling unlockTable succeeded"));
      }
      Assertions.assertTrue(
          output
              .toString()
              .contains("DEBUG Calling unlockTable with identifier: dunlock.t1, reason: " + reason),
          output.toString());
      Assertions.assertFalse(
          output.toString().contains("INFO Calling unlockTable with identifier:"));
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(previousLevel);
      appender.close();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " REASON LEGACY", " REASON legacy", " REASON `legacy`"})
  public void testLegacyDefaultAndExplicitReasonUseLegacyEndpoint(String suffix) {
    List<String> requests = recordRequests(new MockResponse().setResponseCode(204));
    spark.sql("ALTER TABLE openhouse.dunlock.t1 UNLOCK" + suffix);
    Assertions.assertEquals(
        Collections.singletonList("DELETE /v1/databases/dunlock/tables/t1/lock"), requests);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "UNKNOWN",
        "tier3_auto_cleanup",
        "``",
        "` `",
        "`.`",
        "`..`",
        "`SYSTEM``ONLY`",
        "` SYSTEM_ONLY `"
      })
  public void testInvalidReasonIsRejectedBeforeHttp(String reason) {
    List<String> requests = recordRequests(new MockResponse().setResponseCode(204));
    OpenhouseParseException failure =
        Assertions.assertThrows(
            OpenhouseParseException.class,
            () -> spark.sql("ALTER TABLE openhouse.dunlock.t1 UNLOCK REASON " + reason));
    Assertions.assertTrue(
        failure.getMessage().contains("Expected one of: LEGACY, SYSTEM_ONLY."),
        failure.getMessage());
    Assertions.assertTrue(requests.isEmpty(), requests.toString());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "openhouse.``.t1",
        "openhouse.dunlock.``",
        "openhouse.`.`.t1",
        "openhouse.`..`.t1",
        "openhouse.dunlock.`.`",
        "openhouse.dunlock.`..`",
        "openhouse.t1",
        "openhouse.dunlock.extra.t1"
      })
  public void testInvalidTargetIsRejectedDuringAnalysisBeforeHttp(String target) {
    List<String> requests = recordRequests(new MockResponse().setResponseCode(204));
    Assertions.assertThrows(
        AnalysisException.class, () -> spark.sql("ALTER TABLE " + target + " UNLOCK"));
    Assertions.assertTrue(requests.isEmpty(), requests.toString());
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "openhouse.`my db`.`ta``ble`|/v1/databases/my%20db/tables/ta%60ble/lock",
        "openhouse.`db/x`.`ta?#%ble`|/v1/databases/db%2Fx/tables/ta%3F%23%25ble/lock",
        "openhouse.`a.b`.`..table`|/v1/databases/a.b/tables/..table/lock"
      })
  public void testQuotedTargetComponentsPreserveUrlSegments(String target, String path) {
    List<String> requests = recordRequests(new MockResponse().setResponseCode(204));
    spark.sql("ALTER TABLE " + target + " UNLOCK");
    Assertions.assertEquals(Collections.singletonList("DELETE " + path), requests);
  }

  @ParameterizedTest
  @ValueSource(ints = {401, 403, 404, 405, 409, 500})
  public void testUnlockHttpFailurePropagates(int status) {
    List<String> requests = recordRequests(new MockResponse().setResponseCode(status));
    WebClientResponseWithMessageException failure =
        Assertions.assertThrows(
            WebClientResponseWithMessageException.class,
            () -> spark.sql("ALTER TABLE openhouse.dunlock.t1 UNLOCK"));
    Assertions.assertEquals(status, failure.getStatusCode());
    Assertions.assertEquals(
        Collections.singletonList("DELETE /v1/databases/dunlock/tables/t1/lock"), requests);
  }

  private List<String> recordRequests(MockResponse response) {
    List<String> requests = Collections.synchronizedList(new ArrayList<>());
    mockTableService.setDispatcher(
        new Dispatcher() {
          @NotNull
          @Override
          public MockResponse dispatch(@NotNull RecordedRequest request) {
            requests.add(request.getMethod() + " " + request.getPath());
            return response;
          }
        });
    return requests;
  }

  @Test
  public void testUnlockRequestFailure() {
    List<String> requests =
        recordRequests(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST));
    WebClientRequestWithMessageException failure =
        Assertions.assertThrows(
            WebClientRequestWithMessageException.class,
            () -> spark.sql("ALTER TABLE openhouse.dunlock.t1 UNLOCK"));
    Assertions.assertTrue(failure.getMessage().contains("Connection prematurely closed"));
    Assertions.assertFalse(requests.isEmpty());
    Assertions.assertTrue(
        requests.stream().allMatch("DELETE /v1/databases/dunlock/tables/t1/lock"::equals));
  }
}
