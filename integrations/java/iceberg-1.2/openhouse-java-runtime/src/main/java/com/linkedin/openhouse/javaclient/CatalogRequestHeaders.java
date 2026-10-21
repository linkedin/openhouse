package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.client.ssl.WebClientFactory;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;
import org.springframework.http.HttpHeaders;

/**
 * Reads request headers from {@code header.<name>=<value>} catalog properties. Iceberg's REST
 * catalog defines this convention and sends each such property as a {@code <name>: <value>} header
 * on every request. In Spark, set {@code spark.sql.catalog.<catalog>.header.<name>=<value>}. A
 * caller can then send a header the server defines without a new client release.
 *
 * <p>Iceberg implements this only in its REST catalog. {@code OpenHouseCatalog} has its own API
 * client, so it adds the headers to that client; other catalogs in the same JVM do not send them.
 */
@Slf4j
final class CatalogRequestHeaders {
  static final String PREFIX = "header.";

  /** An HTTP header name: one or more RFC 7230 token characters. */
  private static final Pattern NAME = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");

  /** A character a header value cannot contain: a control character other than tab. */
  private static final Pattern VALUE_CONTROL = Pattern.compile("[\\x00-\\x08\\x0A-\\x1F\\x7F]");

  /** Maps each header the catalog sets to the catalog property it comes from. */
  private static final Map<String, String> SOURCE_PROPERTY =
      ImmutableMap.of(
          HttpHeaders.AUTHORIZATION, OpenHouseCatalog.AUTH_TOKEN,
          HttpHeaders.USER_AGENT, OpenHouseCatalog.CLIENT_VERSION,
          WebClientFactory.HTTP_HEADER_CLIENT_NAME, OpenHouseCatalog.CLIENT_NAME,
          WebClientFactory.HTTP_HEADER_ACTION_TYPE, OpenHouseCatalog.ACTION_TYPE,
          WebClientFactory.HTTP_HEADER_SESSION_ID, CatalogProperties.APP_ID);

  private CatalogRequestHeaders() {}

  /**
   * Returns the configured headers, one value each. Header names are case-insensitive. A property
   * that names no header, has a name or value HTTP cannot carry, names a header the catalog sets,
   * or names a header another property also names is ignored with a warning: initialization goes
   * on, and the catalog keeps its own values. The warning names the header, never the value.
   */
  static HttpHeaders from(Map<String, String> properties) {
    HttpHeaders headers = new HttpHeaders();
    PropertyUtil.propertiesWithPrefix(properties, PREFIX)
        .forEach(
            (name, value) -> {
              String unusable = unusableReason(name, value);
              if (unusable == null) {
                headers.add(name, value);
              } else {
                log.warn("Ignoring catalog property {}{}: {}", PREFIX, name, unusable);
              }
            });
    SOURCE_PROPERTY.forEach(
        (header, property) -> ignore(headers, header, "the catalog sets it from " + property));
    for (String header : new ArrayList<>(headers.keySet())) {
      if (headers.get(header).size() > 1) {
        ignore(headers, header, "more than one property names it");
      }
    }
    return headers;
  }

  /** Returns why HTTP cannot carry this header, or null if it can. */
  private static String unusableReason(String name, String value) {
    if (name.isEmpty()) {
      return "it names no header";
    }
    if (!NAME.matcher(name).matches()) {
      return "the name is not a valid HTTP header name";
    }
    if (VALUE_CONTROL.matcher(value).find()) {
      return "the value contains a control character";
    }
    return null;
  }

  private static void ignore(HttpHeaders headers, String header, String reason) {
    if (headers.remove(header) != null) {
      log.warn("Ignoring catalog property {}{}: {}", PREFIX, header, reason);
    }
  }
}
