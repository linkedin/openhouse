package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Admission for explicitly opted-in tables; called after authorization, before reads or writes.
 * Tables without {@code read-bridge.column-default.enabled=true} are never checked.
 */
public class ColumnDefaultClientGate {
  private static final String ENABLED = ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP;
  private static final String PRODUCT = "openhouse-java-client/";
  private static final Pattern RELEASE = Pattern.compile("[0-9]{1,9}(?:\\.[0-9]{1,9}){2,3}");
  private final String minimum;
  /** Empty when no minimum is configured: then no client is admitted to opted-in tables. */
  private final Optional<int[]> minimumRelease;

  private final CatalogRequestContext requestContext;

  /**
   * @param minimum oldest admitted {@code openhouse-java-client} release, such as {@code 4.2.150}.
   *     Empty admits no client: every request to an opted-in table fails as a server error unless
   *     it carries the column-default policy bypass. It does not turn the gate off.
   * @param requestContext the request's client name, User-Agent and column-default policy bypass
   * @throws IllegalArgumentException if {@code minimum} is set but is not a 3- or 4-part release
   */
  public ColumnDefaultClientGate(String minimum, CatalogRequestContext requestContext) {
    this.minimum = minimum;
    this.minimumRelease =
        minimum.isEmpty()
            ? Optional.empty()
            : Optional.of(
                release(minimum)
                    .orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                "cluster.read-bridge.column-default.minimum-client-version must be"
                                    + " a 3- or 4-part release such as 4.2.150, got '"
                                    + minimum
                                    + "'")));
    this.requestContext = requestContext;
  }

  /**
   * Rejects loading {@code table} if it is opted in and the client is not admitted.
   *
   * @throws ColumnDefaultClientRejectedException (422) if the client is not admitted
   * @throws TableConfigUnavailableException (500) if no minimum is configured
   */
  public void checkLoad(TableDto table) {
    if (enabled(table.getTableProperties())) {
      requireAdmittedClient();
    }
  }

  /**
   * Rejects a write if the {@code stored} table or the {@code proposed} properties opt in and the
   * client is not admitted. {@code stored} is empty when the write creates the table.
   *
   * @throws ColumnDefaultClientRejectedException (422) if the client is not admitted
   * @throws TableConfigUnavailableException (500) if no minimum is configured
   */
  public void checkWrite(Optional<TableDto> stored, Map<String, String> proposed) {
    if (stored.filter(table -> enabled(table.getTableProperties())).isPresent()
        || enabled(proposed)) {
      requireAdmittedClient();
    }
  }

  /**
   * Whether the current request comes from a client that applies column defaults: {@code
   * client-name=spark} with an {@code openhouse-java-client} release at or above the minimum, in
   * the same major release family. False outside a request, and when no minimum is configured. The
   * bypass does not count.
   */
  public boolean admitsClient() {
    return requestContext
        .clientName()
        .filter("spark"::equals)
        .flatMap(spark -> requestContext.userAgent())
        .filter(userAgent -> userAgent.startsWith(PRODUCT))
        .flatMap(userAgent -> release(userAgent.substring(PRODUCT.length())))
        .filter(this::atOrAboveMinimum)
        .isPresent();
  }

  private void requireAdmittedClient() {
    if (requestContext.columnDefaultPolicyBypass() || admitsClient()) {
      return;
    }
    if (!minimumRelease.isPresent()) {
      throw new TableConfigUnavailableException(
          "Column-default tables admit no client: this server has no "
              + "cluster.read-bridge.column-default.minimum-client-version configured."
              + TablesMvcConstants.COLUMN_DEFAULT_POLICY_BYPASS_HINT);
    }
    throw new ColumnDefaultClientRejectedException(
        "Column-default tables require X-Client-Name: spark and User-Agent: openhouse-java-client/"
            + minimum
            + " or newer in the same major release family. This request sent X-Client-Name: "
            + requestContext.clientName().orElse("(none)")
            + ", User-Agent: "
            + requestContext.userAgent().orElse("(none)")
            + "."
            + TablesMvcConstants.COLUMN_DEFAULT_POLICY_BYPASS_HINT);
  }

  /**
   * Whether {@code release} is in the minimum's major release family and not older than the
   * minimum. OSS 0.x and LI 4.x advertise different artifacts; never order one above the other.
   */
  private boolean atOrAboveMinimum(int[] release) {
    return minimumRelease
        .filter(oldest -> release[0] == oldest[0] && compare(release, oldest) >= 0)
        .isPresent();
  }

  /**
   * Orders releases part by part. A release is older than its own hotfixes, so {@code 4.1.324}
   * ranks below {@code 4.1.324.0}, as in Gradle.
   */
  private static int compare(int[] left, int[] right) {
    for (int i = 0; i < Math.min(left.length, right.length); i++) {
      if (left[i] != right[i]) {
        return Integer.compare(left[i], right[i]);
      }
    }
    return Integer.compare(left.length, right.length);
  }

  /**
   * The numeric parts of a plain 3- or 4-part release such as {@code 0.5.100}. Empty for anything
   * else, including prereleases such as {@code 0.5.100-SNAPSHOT}.
   */
  private static Optional<int[]> release(String version) {
    return Optional.of(version)
        .filter(candidate -> RELEASE.matcher(candidate).matches())
        .map(
            candidate ->
                Arrays.stream(candidate.split("\\.")).mapToInt(Integer::parseInt).toArray());
  }

  private static boolean enabled(Map<String, String> properties) {
    String value = properties == null ? null : properties.get(ENABLED);
    return value != null && "true".equalsIgnoreCase(value.trim());
  }
}
