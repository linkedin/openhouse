package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.optimizer.client.model.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import io.opentelemetry.api.common.Attributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;

class BatchedSparkAppMetricsTest {
  private static final String SCOPE = BaseSparkApp.METRICS_SCOPE;
  private static final String JOB_ID = "batch-job";
  private static final UpdateOperationRequest.OperationTypeEnum OFD =
      UpdateOperationRequest.OperationTypeEnum.ORPHAN_FILES_DELETION;
  private static final UpdateOperationRequest.OperationTypeEnum STATS =
      UpdateOperationRequest.OperationTypeEnum.TABLE_STATS_COLLECTION;
  private final OtelEmitter emitter = Mockito.mock(OtelEmitter.class);
  private final Operations ops = Mockito.mock(Operations.class);
  private final SparkSession spark = Mockito.mock(SparkSession.class);
  private final RuntimeConfig conf = Mockito.mock(RuntimeConfig.class);
  private final OptimizerServiceClient client = Mockito.mock(OptimizerServiceClient.class);
  private final StatsCollectionSink sink = Mockito.mock(StatsCollectionSink.class);
  private Optional<OptimizerServiceClient> optimizerClient;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    optimizerClient = Optional.of(client);
    Mockito.when(ops.spark()).thenReturn(spark);
    Mockito.when(spark.conf()).thenReturn(conf);
    Mockito.when(
            conf.get(
                ArgumentMatchers.eq("spark.sql.catalog.openhouse.cluster"),
                ArgumentMatchers.nullable(String.class)))
        .thenReturn("test-cluster");
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(Collections.emptyMap());
    Mockito.when(ops.getTable(ArgumentMatchers.anyString())).thenReturn(table);
    DeleteOrphanFiles.Result result = Mockito.mock(DeleteOrphanFiles.Result.class);
    Mockito.when(result.orphanFileLocations()).thenReturn(Collections.singletonList("sample"));
    Mockito.when(
            ops.deleteOrphanFiles(
                ArgumentMatchers.eq(table),
                ArgumentMatchers.anyLong(),
                ArgumentMatchers.eq(".backup"),
                ArgumentMatchers.eq(2),
                ArgumentMatchers.eq(true),
                ArgumentMatchers.eq(1),
                ArgumentMatchers.any(Operations.FileDeletionMetrics.class)))
        .thenAnswer(
            invocation -> {
              Operations.FileDeletionMetrics metrics = invocation.getArgument(6);
              metrics.recordDeletion(OptionalLong.of(30));
              metrics.recordDeletion(OptionalLong.of(70));
              return result;
            });
    Row row = Mockito.mock(Row.class);
    Mockito.when(row.fieldIndex("tableName")).thenReturn(0);
    Mockito.when(row.getString(0)).thenReturn("a");
    Dataset<Row> tables = Mockito.mock(Dataset.class);
    Mockito.when(tables.collectAsList()).thenReturn(Collections.singletonList(row));
    Mockito.when(spark.sql(ArgumentMatchers.anyString())).thenReturn(tables);
    Mockito.when(ops.collectTableStats(ArgumentMatchers.anyString()))
        .thenReturn(Mockito.mock(IcebergTableStats.class));
    Mockito.when(
            client.updateOperation(
                ArgumentMatchers.anyString(), ArgumentMatchers.any(UpdateOperationRequest.class)))
        .thenReturn(Optional.of(new TableOperationsHistory()));
  }

  @Test
  void orphanDeletionReportsEachOutcomeIncludingValidationFailure() {
    Mockito.when(ops.getTable("db.b")).thenThrow(new IllegalStateException("Table unavailable"));

    Assertions.assertDoesNotThrow(() -> ofdApp("db.a", "db.b", "db.c").runInner(ops));

    verifyTerminal(OFD, "SUCCESS", 1);
    verifyTerminal(OFD, "FAILED", 2);
    verifyValidation("SUCCESS", 1);
    verifyValidation("FAILED", 1);
    verifyValidation("NOT_RUN", 1);
    verifyCount(AppConstants.RECLAIMED_FILE_COUNT, 2, attributes(OFD), 2);
    verifyCount(AppConstants.RECLAIMED_FILE_COUNT, 0, attributes(OFD), 1);
    verifyCount(AppConstants.RECLAIMED_FILE_BYTES, 100, attributes(OFD), 2);
    verifyCount(
        AppConstants.ORPHAN_FILE_COUNT,
        1,
        attributes(OFD).toBuilder().put(AppConstants.TABLE_NAME, "db.a").build(),
        1);
    verifyCallback("op-a", OFD, UpdateOperationRequest.StatusEnum.SUCCESS);
    verifyCallback("op-b", OFD, UpdateOperationRequest.StatusEnum.FAILED);
    verifyCallback("op-c", OFD, UpdateOperationRequest.StatusEnum.FAILED);
  }

  @Test
  void allFailedOrphanOperationsStillEmitBeforeTheBatchThrows() {
    Mockito.when(ops.getTable(ArgumentMatchers.anyString()))
        .thenThrow(new IllegalStateException("Catalog unavailable"));

    Assertions.assertThrows(RuntimeException.class, () -> ofdApp("db.a", "db.b").runInner(ops));

    verifyTerminal(OFD, "FAILED", 2);
    verifyTerminal(OFD, "SUCCESS", 0);
    verifyValidation("NOT_RUN", 2);
    verifyCount(AppConstants.RECLAIMED_FILE_COUNT, 0, attributes(OFD), 2);
  }

  @Test
  void partialReclamationIsReportedWhenDeletionFails() {
    Mockito.when(
            ops.deleteOrphanFiles(
                ArgumentMatchers.any(Table.class),
                ArgumentMatchers.anyLong(),
                ArgumentMatchers.anyString(),
                ArgumentMatchers.anyInt(),
                ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyInt(),
                ArgumentMatchers.any(Operations.FileDeletionMetrics.class)))
        .thenAnswer(
            invocation -> {
              Operations.FileDeletionMetrics metrics = invocation.getArgument(6);
              metrics.recordDeletion(OptionalLong.of(30));
              metrics.recordDeletion(OptionalLong.empty());
              throw new IllegalStateException("Deletion interrupted");
            });

    Assertions.assertThrows(RuntimeException.class, () -> ofdApp("db.a").runInner(ops));

    verifyTerminal(OFD, "FAILED", 1);
    verifyValidation("NOT_RUN", 1);
    verifyCount(AppConstants.RECLAIMED_FILE_COUNT, 2, attributes(OFD), 1);
    verifyCount(AppConstants.RECLAIMED_FILE_BYTES, 30, attributes(OFD), 1);
    verifyCount(AppConstants.RECLAIMED_FILE_SIZE_UNKNOWN_COUNT, 1, attributes(OFD), 1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyBatchesDoNotEmitOperations(boolean stats) throws Exception {
    if (stats) {
      statsApp(Collections.emptyList()).runInner(ops);
    } else {
      ofdApp().runInner(ops);
    }

    Mockito.verifyNoInteractions(emitter, client);
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void callbackFailuresDoNotRewriteOrDuplicateTerminalMetrics(
      boolean stats, boolean callbackThrows) {
    if (callbackThrows) {
      Mockito.when(
              client.updateOperation(
                  ArgumentMatchers.anyString(), ArgumentMatchers.any(UpdateOperationRequest.class)))
          .thenThrow(new IllegalStateException("Callback failed"));
    } else {
      Mockito.when(
              client.updateOperation(
                  ArgumentMatchers.anyString(), ArgumentMatchers.any(UpdateOperationRequest.class)))
          .thenReturn(Optional.empty());
    }
    UpdateOperationRequest.OperationTypeEnum type = stats ? STATS : OFD;

    Assertions.assertDoesNotThrow(
        () -> {
          if (stats) {
            statsApp(Collections.singletonList("db.a")).runInner(ops);
          } else {
            ofdApp("db.a").runInner(ops);
          }
        });

    verifyTerminal(type, "SUCCESS", 1);
    verifyTerminal(type, "FAILED", 0);
    InOrder inOrder = Mockito.inOrder(emitter, client);
    inOrder
        .verify(emitter)
        .count(SCOPE, AppConstants.OPERATION_TERMINAL_COUNT, 1, terminal(type, "SUCCESS"));
    inOrder
        .verify(emitter)
        .time(
            ArgumentMatchers.eq(SCOPE),
            ArgumentMatchers.eq(AppConstants.OPERATION_DURATION),
            ArgumentMatchers.longThat(value -> value >= 0),
            ArgumentMatchers.eq(terminal(type, "SUCCESS")));
    inOrder.verify(client).updateOperation(ArgumentMatchers.eq("op-a"), ArgumentMatchers.any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void legacyBatchesWithoutCallbackStillEmitMetrics(boolean stats) throws Exception {
    optimizerClient = Optional.empty();
    if (stats) {
      statsApp(Collections.singletonList("db.a")).runInner(ops);
    } else {
      ofdApp("db.a").runInner(ops);
    }

    verifyTerminal(stats ? STATS : OFD, "SUCCESS", 1);
    Mockito.verifyNoInteractions(client);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void metricsFailuresDoNotFailExecutionOrSuppressCallback(boolean stats) {
    Mockito.doThrow(new IllegalStateException("Metrics unavailable"))
        .when(emitter)
        .count(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyLong(),
            ArgumentMatchers.any());
    Mockito.doThrow(new IllegalStateException("Metrics unavailable"))
        .when(emitter)
        .time(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyLong(),
            ArgumentMatchers.any());

    Assertions.assertDoesNotThrow(
        () -> {
          if (stats) {
            statsApp(Collections.singletonList("db.a")).runInner(ops);
          } else {
            ofdApp("db.a").runInner(ops);
          }
        });

    verifyCallback("op-a", stats ? STATS : OFD, UpdateOperationRequest.StatusEnum.SUCCESS);
  }

  @Test
  void statsCollectionReportsCollectionAndPublishingFailures() {
    Mockito.when(ops.collectTableStats("db.b"))
        .thenThrow(new IllegalStateException("Collection failed"));
    Mockito.doThrow(new IllegalStateException("Publishing failed"))
        .when(sink)
        .publishStats(ArgumentMatchers.eq("db.c"), ArgumentMatchers.any());

    Assertions.assertDoesNotThrow(
        () -> statsApp(Arrays.asList("db.a", "db.b", "db.c")).runInner(ops));

    verifyTerminal(STATS, "SUCCESS", 1);
    verifyTerminal(STATS, "FAILED", 2);
    verifyCallback("op-a", STATS, UpdateOperationRequest.StatusEnum.SUCCESS);
    verifyCallback("op-b", STATS, UpdateOperationRequest.StatusEnum.FAILED);
    verifyCallback("op-c", STATS, UpdateOperationRequest.StatusEnum.FAILED);
    Mockito.verify(emitter, Mockito.never())
        .count(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.eq(AppConstants.POST_RUN_VALIDATION_COUNT),
            ArgumentMatchers.anyLong(),
            ArgumentMatchers.any());
    Mockito.verify(emitter, Mockito.never())
        .count(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.eq(AppConstants.RECLAIMED_FILE_BYTES),
            ArgumentMatchers.anyLong(),
            ArgumentMatchers.any());
  }

  @Test
  void nullStatsFailEveryOperationWithoutLosingMetrics() {
    Mockito.when(ops.collectTableStats(ArgumentMatchers.anyString())).thenReturn(null);

    Assertions.assertThrows(
        RuntimeException.class, () -> statsApp(Arrays.asList("db.a", "db.b")).runInner(ops));

    verifyTerminal(STATS, "FAILED", 2);
    verifyTerminal(STATS, "SUCCESS", 0);
  }

  @Test
  void concurrentOperationsEmitOneTerminalSampleEach() {
    List<String> tables =
        IntStream.range(0, 20).mapToObj(index -> "db.table" + index).collect(Collectors.toList());

    Assertions.assertDoesNotThrow(() -> statsApp(tables).runInner(ops));

    verifyTerminal(STATS, "SUCCESS", tables.size());
    verifyTerminal(STATS, "FAILED", 0);
    Mockito.verify(client, Mockito.times(tables.size()))
        .updateOperation(ArgumentMatchers.anyString(), ArgumentMatchers.any());
  }

  @Test
  void missingIdentifiersHaveExplicitUnknownTags() {
    Mockito.when(conf.get(ArgumentMatchers.anyString(), ArgumentMatchers.nullable(String.class)))
        .thenReturn(null);
    BatchedOperationMetrics metrics =
        new BatchedOperationMetrics(ops, emitter, null, STATS, "db", "db.a");

    metrics.complete(UpdateOperationRequest.StatusEnum.SUCCESS);

    Attributes expected =
        terminal(STATS, "SUCCESS")
            .toBuilder()
            .put(AppConstants.CLUSTER_NAME, "unknown")
            .put(AppConstants.JOB_ID, "unknown")
            .build();
    verifyCount(AppConstants.OPERATION_TERMINAL_COUNT, 1, expected, 1);
  }

  private BatchedOrphanFilesDeletionSparkApp ofdApp(String... tables) {
    List<BatchedOrphanFilesDeletionSparkApp.BatchEntry> entries =
        Arrays.stream(tables)
            .map(
                fqtn -> {
                  String tableName = fqtn.split("\\.", 2)[1];
                  return BatchedOrphanFilesDeletionSparkApp.BatchEntry.builder()
                      .fqtn(fqtn)
                      .databaseName("db")
                      .tableName(tableName)
                      .operationId("op-" + tableName)
                      .tableUuid("uuid-" + tableName)
                      .build();
                })
            .collect(Collectors.toList());
    return new BatchedOrphanFilesDeletionSparkApp(
        JOB_ID,
        null,
        emitter,
        entries,
        null,
        3,
        TimeUnit.DAYS.toSeconds(7),
        ".backup",
        2,
        true,
        1) {
      @Override
      protected Optional<OptimizerServiceClient> newOptimizerClient() {
        return optimizerClient;
      }
    };
  }

  private BatchedTableStatsCollectionSparkApp statsApp(List<String> tables) {
    List<BatchedTableStatsCollectionSparkApp.BatchEntry> entries =
        tables.stream()
            .map(
                fqtn -> {
                  String tableName = fqtn.split("\\.", 2)[1];
                  return BatchedTableStatsCollectionSparkApp.BatchEntry.builder()
                      .fqtn(fqtn)
                      .databaseName("db")
                      .tableName(tableName)
                      .operationId("op-" + tableName)
                      .tableUuid("uuid-" + tableName)
                      .build();
                })
            .collect(Collectors.toList());
    return new BatchedTableStatsCollectionSparkApp(JOB_ID, null, emitter, entries, null, 4, sink) {
      @Override
      protected Optional<OptimizerServiceClient> newOptimizerClient() {
        return optimizerClient;
      }
    };
  }

  private Attributes attributes(UpdateOperationRequest.OperationTypeEnum type) {
    return Attributes.builder()
        .put(AppConstants.OPERATION_TYPE, type.name())
        .put(AppConstants.CLUSTER_NAME, "test-cluster")
        .put(AppConstants.DATABASE_NAME, "db")
        .put(AppConstants.JOB_ID, JOB_ID)
        .build();
  }

  private Attributes terminal(UpdateOperationRequest.OperationTypeEnum type, String status) {
    return attributes(type).toBuilder().put(AppConstants.STATUS, status).build();
  }

  private void verifyTerminal(
      UpdateOperationRequest.OperationTypeEnum type, String status, int count) {
    Attributes expected = terminal(type, status);
    verifyCount(AppConstants.OPERATION_TERMINAL_COUNT, 1, expected, count);
    Mockito.verify(emitter, Mockito.times(count))
        .time(
            ArgumentMatchers.eq(SCOPE),
            ArgumentMatchers.eq(AppConstants.OPERATION_DURATION),
            ArgumentMatchers.longThat(value -> value >= 0),
            ArgumentMatchers.eq(expected));
  }

  private void verifyValidation(String status, int count) {
    verifyCount(AppConstants.POST_RUN_VALIDATION_COUNT, 1, terminal(OFD, status), count);
  }

  private void verifyCount(String metric, long value, Attributes attributes, int count) {
    Mockito.verify(emitter, Mockito.times(count)).count(SCOPE, metric, value, attributes);
  }

  private void verifyCallback(
      String operationId,
      UpdateOperationRequest.OperationTypeEnum type,
      UpdateOperationRequest.StatusEnum status) {
    Mockito.verify(client)
        .updateOperation(
            ArgumentMatchers.eq(operationId),
            ArgumentMatchers.argThat(
                request -> request.getStatus() == status && request.getOperationType() == type));
  }
}
