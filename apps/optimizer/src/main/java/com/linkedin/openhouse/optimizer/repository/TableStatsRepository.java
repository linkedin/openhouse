package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import java.util.stream.Stream;
import javax.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

/** Spring Data JPA repository for {@code table_stats} rows in the optimizer DB. */
public interface TableStatsRepository extends JpaRepository<TableStatsRow, String> {

  /**
   * Streams all rows as a JDBC cursor rather than buffering them in memory. The caller must consume
   * the stream inside an active {@code @Transactional} method and close it when done.
   *
   * <p>{@code Integer.MIN_VALUE} is MySQL Connector/J's signal to enable row-by-row streaming
   * instead of loading the full result set into the driver buffer.
   */
  @Query("SELECT r FROM TableStatsRow r")
  @QueryHints(
      @QueryHint(
          name = org.hibernate.jpa.QueryHints.HINT_FETCH_SIZE,
          value = "" + Integer.MIN_VALUE))
  Stream<TableStatsRow> streamAll();
}
