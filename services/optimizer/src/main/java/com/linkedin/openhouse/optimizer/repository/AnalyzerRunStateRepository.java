package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.db.AnalyzerRunStateRow;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Repository for {@code analyzer_run_state} — the analyzer's per-operation-type incremental-scan
 * watermark. Keyed by operation-type name.
 */
public interface AnalyzerRunStateRepository extends JpaRepository<AnalyzerRunStateRow, String> {}
