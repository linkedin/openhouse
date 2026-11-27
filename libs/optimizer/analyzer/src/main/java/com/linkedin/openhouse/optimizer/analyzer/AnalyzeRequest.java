package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.ToString;

/**
 * Sum-type filter describing what one analyzer invocation should evaluate. Every dimension is
 * optional and only <i>narrows</i> an otherwise "analyze everything enabled" default — i.e. the
 * invocation is a filter over all registered analyzers and all tables:
 *
 * <ul>
 *   <li>{@code operationTypes} — which analyzers to run; empty means all registered/enabled.
 *   <li>{@code databaseName} — restrict to one database; empty iterates every database one at a
 *       time so the per-query working set stays bounded by tables-per-db, not tables-total.
 *   <li>{@code tableName} / {@code tableUuid} — restrict within the selected database(s).
 *   <li>{@code table} — in-memory stats for a just-committed table (commit-driven path). When
 *       present the DB table scan is skipped and these stats are used directly; the other
 *       scan-scoping filters ({@code databaseName}/{@code tableName}/{@code tableUuid}) do not
 *       apply because the table is already fully identified.
 * </ul>
 *
 * <p>Examples (mirroring the intended CLI/REST surface):
 *
 * <pre>
 *   AnalyzeRequest.builder().build()                                  // everything, all analyzers
 *   AnalyzeRequest.builder().operationTypes(Set.of(OFD, STATS))...    // all tables, those analyzers
 *   AnalyzeRequest.builder().databaseName("foo").build()             // db foo, all analyzers
 *   AnalyzeRequest.builder().databaseName("foo").tableName("bar")
 *                 .operationTypes(Set.of(STATS)).build()              // foo.bar, STATS only
 * </pre>
 *
 * <p>Nullable raw fields with {@link Optional} getters keep the builder ergonomic while avoiding
 * {@code Optional} instance fields.
 */
@Builder
@ToString
public class AnalyzeRequest {
  @Builder.Default private final Set<OperationTypeDto> operationTypes = Collections.emptySet();
  private final String databaseName;
  private final String tableName;
  private final String tableUuid;
  private final TableDto table;

  /** Analyzers to run; empty means all registered/enabled. Never {@code null}. */
  public Set<OperationTypeDto> getOperationTypes() {
    return operationTypes == null ? Collections.emptySet() : operationTypes;
  }

  public Optional<String> getDatabaseName() {
    return Optional.ofNullable(databaseName);
  }

  public Optional<String> getTableName() {
    return Optional.ofNullable(tableName);
  }

  public Optional<String> getTableUuid() {
    return Optional.ofNullable(tableUuid);
  }

  /** In-memory stats for the commit-driven path; empty for a DB scan. */
  public Optional<TableDto> getTable() {
    return Optional.ofNullable(table);
  }
}
