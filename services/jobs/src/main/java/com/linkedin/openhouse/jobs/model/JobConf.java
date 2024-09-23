package com.linkedin.openhouse.jobs.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class JobConf {
  private JobType jobType;
  private String proxyUser;
  @Builder.Default private Map<String, String> executionConf = new HashMap<>();
  @Builder.Default private List<String> args = new ArrayList<>();

  public enum JobType {
    NO_OP,
    SQL_TEST,
    RETENTION,
    ORPHAN_FILES_DELETION,
    SNAPSHOTS_EXPIRATION,
    STAGED_FILES_DELETION,
    DATA_COMPACTION,
    ORPHAN_DIRECTORY_DELETION,
    TABLE_STATS_COLLECTION,
    DATA_LAYOUT_STRATEGY_GENERATION,
    DATA_LAYOUT_STRATEGY_EXECUTION
  }
}
