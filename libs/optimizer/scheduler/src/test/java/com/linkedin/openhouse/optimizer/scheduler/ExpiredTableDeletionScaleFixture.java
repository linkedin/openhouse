package com.linkedin.openhouse.optimizer.scheduler;

import com.linkedin.openhouse.optimizer.db.HistoryStatus;
import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.TemporaryTable;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;

/**
 * A synthetic optimizer database for the expired table deletion scale tests. Every table gets one
 * {@link TableKind}, which fixes its properties, its current deletion operation, and its latest
 * deletion history, and so whether the analyzer must schedule it. The kinds repeat in a fixed
 * pattern of {@link #KIND_PATTERN_LENGTH} tables, and tables spread over {@link #DATABASE_COUNT}
 * databases and {@link #CREATOR_COUNT} creators, so each database holds every kind.
 */
@Getter
final class ExpiredTableDeletionScaleFixture {

  static final int TABLE_COUNT = 100_000;
  static final int DATABASE_COUNT = 10;
  static final int CREATOR_COUNT = 10;
  static final String TABLE_CREATOR_PROPERTY = "openhouse.tableCreator";
  private static final OperationType DELETION = OperationType.EXPIRED_TABLE_DELETION;

  /** Each kind's share is its count of tables in every {@link #KIND_PATTERN_LENGTH} tables. */
  enum TableKind {
    ORDINARY(8, false),
    NOT_EXPIRED(4, false),
    EXPIRED(3, true),
    DELETED(2, false),
    FAILED_IN_BACKOFF(1, false),
    FAILED_RETRY_DUE(1, true),
    DELETION_PENDING(1, false);

    private final int share;
    private final boolean scheduled;

    TableKind(int share, boolean scheduled) {
      this.share = share;
      this.scheduled = scheduled;
    }

    boolean isScheduled() {
      return scheduled;
    }
  }

  private static final List<TableKind> KIND_PATTERN =
      Arrays.stream(TableKind.values())
          .flatMap(kind -> Collections.nCopies(kind.share, kind).stream())
          .collect(Collectors.toUnmodifiableList());

  static final int KIND_PATTERN_LENGTH = KIND_PATTERN.size();

  private final List<TableStatsRow> tableStats;
  private final List<TableOperationsRow> operations;
  private final List<TableOperationsHistoryRow> history;
  private final Map<TableKind, Long> tableCountByKind;

  /** Fully qualified names, {@code database.table}, of the tables the analyzer must schedule. */
  private final Set<String> tablesAnalyzerSchedules;

  /**
   * The tables the scheduler then submits: those the analyzer schedules, and the {@link
   * TableKind#DELETION_PENDING} tables whose deletion was already waiting for the scheduler.
   */
  private final Set<String> tablesSchedulerSubmits;

  private ExpiredTableDeletionScaleFixture(List<SyntheticTable> tables) {
    this.tableStats =
        tables.stream().map(SyntheticTable::getStats).collect(Collectors.toUnmodifiableList());
    this.operations =
        tables.stream()
            .flatMap(table -> table.getOperation().stream())
            .collect(Collectors.toUnmodifiableList());
    this.history =
        tables.stream()
            .flatMap(table -> table.getHistory().stream())
            .collect(Collectors.toUnmodifiableList());
    this.tableCountByKind =
        tables.stream()
            .collect(Collectors.groupingBy(SyntheticTable::getKind, Collectors.counting()));
    this.tablesAnalyzerSchedules =
        tables.stream()
            .filter(table -> table.getKind().isScheduled())
            .map(table -> fullyQualifiedName(table.getStats()))
            .collect(Collectors.toUnmodifiableSet());
    this.tablesSchedulerSubmits =
        tables.stream()
            .filter(
                table ->
                    table.getKind().isScheduled() || table.getKind() == TableKind.DELETION_PENDING)
            .map(table -> fullyQualifiedName(table.getStats()))
            .collect(Collectors.toUnmodifiableSet());
  }

  /** Builds {@link #TABLE_COUNT} tables whose times are relative to {@code now}. */
  static ExpiredTableDeletionScaleFixture generate(Instant now) {
    return generate(now, TABLE_COUNT);
  }

  /**
   * Builds {@code tableCount} tables whose times are relative to {@code now}. A multiple of {@link
   * #KIND_PATTERN_LENGTH} times {@link #DATABASE_COUNT} gives every database every kind.
   */
  static ExpiredTableDeletionScaleFixture generate(Instant now, int tableCount) {
    return new ExpiredTableDeletionScaleFixture(
        IntStream.range(0, tableCount)
            .mapToObj(index -> SyntheticTable.create(index, now))
            .collect(Collectors.toList()));
  }

  long countOf(TableKind kind) {
    return tableCountByKind.getOrDefault(kind, 0L);
  }

  static String fullyQualifiedName(TableStatsRow stats) {
    return stats.getDatabaseName() + "." + stats.getTableName();
  }

  static String fullyQualifiedName(TableOperationsRow operation) {
    return operation.getDatabaseName() + "." + operation.getTableName();
  }

  @Getter
  private static final class SyntheticTable {
    private final TableKind kind;
    private final TableStatsRow stats;
    private final Optional<TableOperationsRow> operation;
    private final Optional<TableOperationsHistoryRow> history;

    private SyntheticTable(
        TableKind kind,
        TableStatsRow stats,
        Optional<TableOperationsRow> operation,
        Optional<TableOperationsHistoryRow> history) {
      this.kind = kind;
      this.stats = stats;
      this.operation = operation;
      this.history = history;
    }

    static SyntheticTable create(int index, Instant now) {
      TableKind kind = KIND_PATTERN.get(index % KIND_PATTERN_LENGTH);
      TableStatsRow stats =
          TableStatsRow.builder()
              .tableUuid(new UUID(0L, index).toString())
              .databaseName("db" + (index / KIND_PATTERN_LENGTH) % DATABASE_COUNT)
              .tableName(kind.name().toLowerCase() + "_" + index)
              .snapshot(SnapshotMetrics.builder().numCurrentFiles(10L).build())
              .tableProperties(
                  properties(
                      "creator" + (index / (KIND_PATTERN_LENGTH * DATABASE_COUNT)) % CREATOR_COUNT,
                      kind,
                      now))
              .updatedAt(now)
              .build();
      Function<TableOperationsRow.TableOperationsRowBuilder, TableOperationsRow> operationOf =
          builder ->
              builder
                  .id(new UUID(1L, index).toString())
                  .tableUuid(stats.getTableUuid())
                  .databaseName(stats.getDatabaseName())
                  .tableName(stats.getTableName())
                  .operationType(DELETION)
                  .attempts(1)
                  .build();
      Function<
              TableOperationsHistoryRow.TableOperationsHistoryRowBuilder, TableOperationsHistoryRow>
          historyOf =
              builder ->
                  builder
                      .id(new UUID(1L, index).toString())
                      .tableUuid(stats.getTableUuid())
                      .databaseName(stats.getDatabaseName())
                      .tableName(stats.getTableName())
                      .operationType(DELETION)
                      .build();
      switch (kind) {
        case DELETED:
          return new SyntheticTable(
              kind,
              stats,
              Optional.of(
                  operationOf.apply(
                      submitted(now.minus(Duration.ofMinutes(40)))
                          .nextEligibleAt(now.plus(Duration.ofMinutes(30))))),
              Optional.of(
                  historyOf.apply(
                      completed(HistoryStatus.SUCCESS, now.minus(Duration.ofMinutes(30))))));
        case FAILED_IN_BACKOFF:
          return new SyntheticTable(
              kind,
              stats,
              Optional.of(
                  operationOf.apply(
                      submitted(now.minus(Duration.ofMinutes(20)))
                          .nextEligibleAt(now.plus(Duration.ofMinutes(50))))),
              Optional.of(
                  historyOf.apply(
                      completed(HistoryStatus.FAILED, now.minus(Duration.ofMinutes(10))))));
        case FAILED_RETRY_DUE:
          return new SyntheticTable(
              kind,
              stats,
              Optional.of(
                  operationOf.apply(
                      submitted(now.minus(Duration.ofMinutes(130)))
                          .nextEligibleAt(now.minus(Duration.ofHours(1))))),
              Optional.of(
                  historyOf.apply(
                      completed(HistoryStatus.FAILED, now.minus(Duration.ofHours(2))))));
        case DELETION_PENDING:
          return new SyntheticTable(
              kind,
              stats,
              Optional.of(
                  operationOf.apply(
                      TableOperationsRow.builder()
                          .status(OperationStatus.PENDING)
                          .createdAt(now.minus(Duration.ofMinutes(5))))),
              Optional.empty());
        default:
          return new SyntheticTable(kind, stats, Optional.empty(), Optional.empty());
      }
    }

    private static TableOperationsRow.TableOperationsRowBuilder submitted(Instant scheduledAt) {
      return TableOperationsRow.builder()
          .status(OperationStatus.SCHEDULED)
          .createdAt(scheduledAt.minus(Duration.ofMinutes(1)))
          .scheduledAt(scheduledAt)
          .jobId("job-" + scheduledAt.toEpochMilli());
    }

    private static TableOperationsHistoryRow.TableOperationsHistoryRowBuilder completed(
        HistoryStatus status, Instant completedAt) {
      return TableOperationsHistoryRow.builder().status(status).completedAt(completedAt);
    }

    private static Map<String, String> properties(String creator, TableKind kind, Instant now) {
      Map<String, String> properties = new HashMap<>();
      properties.put(TABLE_CREATOR_PROPERTY, creator);
      expirationTime(kind, now)
          .ifPresent(
              expirationTime -> {
                properties.put(TemporaryTable.TEMPORARY_PROPERTY, "true");
                properties.put(
                    TemporaryTable.EXPIRATION_TIME_PROPERTY,
                    DateTimeFormatter.ISO_INSTANT.format(
                        expirationTime.truncatedTo(ChronoUnit.SECONDS)));
              });
      return properties;
    }

    private static Optional<Instant> expirationTime(TableKind kind, Instant now) {
      if (kind == TableKind.ORDINARY) {
        return Optional.empty();
      }
      return Optional.of(
          kind == TableKind.NOT_EXPIRED
              ? now.plus(Duration.ofDays(1))
              : now.minus(Duration.ofHours(25)));
    }
  }
}
