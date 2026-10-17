package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Map;
import java.util.regex.Pattern;
import org.springframework.data.util.Version;

/**
 * Admission for explicitly opted-in tables; called after authorization, before reads or writes.
 * Tables without {@code read-bridge.column-default.enabled=true} are never checked.
 */
public class ColumnDefaultClientGate {
  private static final String ENABLED = ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP;
  private static final String PRODUCT = "openhouse-java-client/";
  private static final Pattern RELEASE = Pattern.compile("[0-9]{1,9}(?:\\.[0-9]{1,9}){2,3}");
  private final String minimum;
  /** Null when no valid minimum is configured: then no client is admitted to opted-in tables. */
  private final Version minimumVersion;

  private final String majorPrefix;

  private final CatalogRequestContext requestContext;

  /**
   * @param minimum oldest admitted {@code openhouse-java-client} release, such as {@code 4.2.150}.
   *     Empty or invalid fails closed: every request to an opted-in table is rejected unless it
   *     carries the column-default policy bypass. It does not turn the gate off.
   * @param requestContext the request's client name, User-Agent and column-default policy bypass
   */
  public ColumnDefaultClientGate(String minimum, CatalogRequestContext requestContext) {
    this.minimum = minimum;
    this.minimumVersion = RELEASE.matcher(minimum).matches() ? Version.parse(minimum) : null;
    this.majorPrefix = minimumVersion == null ? "" : minimum.substring(0, minimum.indexOf('.') + 1);
    this.requestContext = requestContext;
  }

  public void check(TableDto existing, Map<String, String> proposed) {
    if (!enabled(existing == null ? null : existing.getTableProperties()) && !enabled(proposed)) {
      return;
    }
    if (requestContext.columnDefaultPolicyBypass() || admitsClient()) {
      return;
    }
    if (minimumVersion == null) {
      throw new ColumnDefaultClientRejectedException(
          "Column-default tables admit no client: this server has no valid "
              + "cluster.read-bridge.column-default.minimum-client-version (got '"
              + minimum
              + "')."
              + TablesMvcConstants.COLUMN_DEFAULT_POLICY_BYPASS_HINT);
    }
    throw new ColumnDefaultClientRejectedException(
        "Column-default tables require client-name=spark and openhouse-java-client/"
            + minimum
            + " or newer in the same major release family."
            + TablesMvcConstants.COLUMN_DEFAULT_POLICY_BYPASS_HINT);
  }

  /**
   * Whether the current request comes from a client that applies column defaults: {@code
   * client-name=spark} with an {@code openhouse-java-client} release at or above the minimum, in
   * the same major release family. False outside a request, and when no valid minimum is
   * configured. The bypass does not count.
   */
  public boolean admitsClient() {
    if (minimumVersion == null
        || !requestContext.clientName().filter("spark"::equals).isPresent()) {
      return false;
    }
    String userAgent = requestContext.userAgent().orElse("");
    if (!userAgent.startsWith(PRODUCT)) {
      return false;
    }
    String version = userAgent.substring(PRODUCT.length());
    // OSS 0.x and LI 4.x advertise different artifacts; never order one above the other.
    return version.startsWith(majorPrefix)
        && RELEASE.matcher(version).matches()
        && Version.parse(version).isGreaterThanOrEqualTo(minimumVersion);
  }

  private static boolean enabled(Map<String, String> properties) {
    String value = properties == null ? null : properties.get(ENABLED);
    return value != null && "true".equalsIgnoreCase(value.trim());
  }
}
