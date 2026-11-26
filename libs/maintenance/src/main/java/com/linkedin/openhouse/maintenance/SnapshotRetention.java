package com.linkedin.openhouse.maintenance;

import java.time.temporal.ChronoUnit;

/**
 * The snapshot retention a table's history policy gives it. The jobs scheduler decides which policy
 * fields count ({@link #isTimeBased}, {@link #isVersionBased}) and passes only those to the
 * snapshot expiration job, which applies {@link #of}. {@code VACUUM TABLE} reads the same policy
 * through {@link #fromHistoryPolicy}, which composes both steps.
 */
public final class SnapshotRetention {
  /** Snapshot age kept when a table has no time-based history policy. */
  public static final int DEFAULT_MAX_AGE = 3;

  public static final ChronoUnit DEFAULT_GRANULARITY = ChronoUnit.DAYS;

  private final int maxAge;
  private final ChronoUnit granularity;
  private final int versions;

  private SnapshotRetention(int maxAge, ChronoUnit granularity, int versions) {
    this.maxAge = maxAge;
    this.granularity = granularity;
    this.versions = versions;
  }

  /** Whether a history policy expires snapshots by age: it needs both a positive age and a unit. */
  public static boolean isTimeBased(Integer maxAge, Object granularity) {
    return maxAge != null && maxAge > 0 && granularity != null;
  }

  /** Whether a history policy keeps a number of versions. */
  public static boolean isVersionBased(Integer versions) {
    return versions != null && versions > 0;
  }

  /** The retention for a history policy's fields as stored, any of which may be null. */
  public static SnapshotRetention fromHistoryPolicy(
      Integer maxAge, String granularity, Integer versions) {
    boolean timeBased = isTimeBased(maxAge, granularity);
    return of(
        timeBased ? maxAge : 0,
        timeBased ? granularity : null,
        isVersionBased(versions) ? versions : 0);
  }

  /**
   * The retention for the expiration job's arguments: an age of 0 means none was passed, and falls
   * back to {@link #DEFAULT_MAX_AGE} {@link #DEFAULT_GRANULARITY}.
   */
  public static SnapshotRetention of(int maxAge, String granularity, int versions) {
    return maxAge == 0
        ? new SnapshotRetention(DEFAULT_MAX_AGE, DEFAULT_GRANULARITY, versions)
        : new SnapshotRetention(maxAge, toChronoUnit(granularity), versions);
  }

  /**
   * Convert a policy granularity (hour, day, month, year) or a {@link ChronoUnit} name, in any
   * case, to a {@link ChronoUnit}.
   */
  public static ChronoUnit toChronoUnit(String granularity) {
    String name = granularity.toUpperCase();
    switch (name) {
      case "HOUR":
        return ChronoUnit.HOURS;
      case "DAY":
        return ChronoUnit.DAYS;
      case "MONTH":
        return ChronoUnit.MONTHS;
      case "YEAR":
        return ChronoUnit.YEARS;
      default:
        return ChronoUnit.valueOf(name);
    }
  }

  public int maxAge() {
    return maxAge;
  }

  public ChronoUnit granularity() {
    return granularity;
  }

  public int versions() {
    return versions;
  }
}
