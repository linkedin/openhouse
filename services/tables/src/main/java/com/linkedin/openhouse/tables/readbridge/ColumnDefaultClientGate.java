package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Map;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import org.springframework.data.util.Version;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Admission for explicitly opted-in tables; called after authorization, before reads or writes.
 * Tables without {@code read-bridge.column-default.enabled=true} are never checked.
 */
public class ColumnDefaultClientGate {
  private static final String ENABLED =
      ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID + ".enabled";
  private static final String PRODUCT = "openhouse-java-client/";
  private static final Pattern RELEASE = Pattern.compile("[0-9]{1,9}(?:\\.[0-9]{1,9}){2,3}");
  private final String minimum;
  /** Null when no valid minimum is configured: then no client is admitted to opted-in tables. */
  private final Version minimumVersion;

  private final String majorPrefix;

  /**
   * @param minimum oldest admitted {@code openhouse-java-client} release, such as {@code 4.2.150}.
   *     Empty or invalid fails closed: every request to an opted-in table is rejected unless it
   *     carries the column-default policy bypass. It does not turn the gate off.
   */
  public ColumnDefaultClientGate(String minimum) {
    this.minimum = minimum;
    this.minimumVersion = RELEASE.matcher(minimum).matches() ? Version.parse(minimum) : null;
    this.majorPrefix = minimumVersion == null ? "" : minimum.substring(0, minimum.indexOf('.') + 1);
  }

  public void check(TableDto existing, Map<String, String> proposed) {
    if (!enabled(existing == null ? null : existing.getTableProperties()) && !enabled(proposed)) {
      return;
    }
    HttpServletRequest request = currentRequest();
    if (request != null) {
      String bypass =
          request.getHeader(TablesMvcConstants.HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS);
      if ("true".equalsIgnoreCase(bypass)) {
        return;
      }
      String userAgent = request.getHeader("User-Agent");
      if (minimumVersion != null
          && "spark".equals(request.getHeader("X-Client-Name"))
          && userAgent != null
          && userAgent.startsWith(PRODUCT)) {
        String version = userAgent.substring(PRODUCT.length());
        // OSS 0.x and LI 4.x advertise different artifacts; never order one above the other.
        if (version.startsWith(majorPrefix)
            && RELEASE.matcher(version).matches()
            && Version.parse(version).isGreaterThanOrEqualTo(minimumVersion)) {
          return;
        }
      }
    }
    String bypassHint =
        " To bypass column-default policy checks, set header."
            + TablesMvcConstants.HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS
            + "=true on the client catalog; incorrect reads or writes may result.";
    if (minimumVersion == null) {
      throw new ColumnDefaultClientRejectedException(
          "Column-default tables admit no client: this server has no valid "
              + "cluster.read-bridge.column-default.minimum-client-version (got '"
              + minimum
              + "')."
              + bypassHint);
    }
    throw new ColumnDefaultClientRejectedException(
        "Column-default tables require client-name=spark and openhouse-java-client/"
            + minimum
            + " or newer in the same major release family."
            + bypassHint);
  }

  private static HttpServletRequest currentRequest() {
    Object context = RequestContextHolder.getRequestAttributes();
    return context instanceof ServletRequestAttributes
        ? ((ServletRequestAttributes) context).getRequest()
        : null;
  }

  private static boolean enabled(Map<String, String> properties) {
    String value = properties == null ? null : properties.get(ENABLED);
    return value != null && "true".equalsIgnoreCase(value.trim());
  }
}
