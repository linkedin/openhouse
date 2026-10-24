package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.client.ssl.WebClientFactory;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
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

  /**
   * Headers each request sets for the media types it sends and accepts. The generated client adds
   * configured headers after these instead of replacing them, so a configured value would be sent
   * alongside the request's own.
   */
  private static final List<String> REQUEST_SET =
      ImmutableList.of(HttpHeaders.ACCEPT, HttpHeaders.CONTENT_TYPE);

  private CatalogRequestHeaders() {}

  /**
   * Returns the configured headers, one value each. Header names are case-insensitive. A property
   * that names no header, has a name or value HTTP cannot carry, names a header the catalog sets,
   * or names a header another property also names is ignored with a warning: initialization goes
   * on, and the catalog keeps its own values. The warning names the header, never the value.
   *
   * <p>The headers always declare the capabilities the catalog implements in {@code
   * X-OpenHouse-Client-Capabilities}; a configured value adds capabilities, never removes them.
   */
  static HttpHeaders from(Map<String, String> properties) {
    HttpHeaders headers = new HttpHeaders();
    PropertyUtil.propertiesWithPrefix(properties, PREFIX)
        .forEach(
            (name, value) -> {
              if (name.isEmpty()) {
                log.warn("Ignoring catalog property {}: it names no header", PREFIX);
              } else if (!NAME.matcher(name).matches()) {
                log.warn(
                    "Ignoring catalog property {}{}: the name is not a valid HTTP header name",
                    PREFIX,
                    name);
              } else if (VALUE_CONTROL.matcher(value).find()) {
                log.warn(
                    "Ignoring catalog property {}{}: the value contains a control character",
                    PREFIX,
                    name);
              } else {
                headers.add(name, value);
              }
            });
    SOURCE_PROPERTY.forEach(
        (header, property) -> ignore(headers, header, "the catalog sets it from " + property));
    REQUEST_SET.forEach(header -> ignore(headers, header, "each request sets it"));
    for (String header : new ArrayList<>(headers.keySet())) {
      if (headers.get(header).size() > 1) {
        ignore(headers, header, "more than one property names it");
      }
    }
    declare(headers, ReadBridge.CAPABILITY);
    return headers;
  }

  /**
   * Declares {@code capability} first, then those the configured header declares, lower-cased and
   * without duplicates.
   */
  private static void declare(HttpHeaders headers, String capability) {
    Set<String> capabilities = new LinkedHashSet<>();
    capabilities.add(capability);
    String configured = headers.getFirst(WebClientFactory.HTTP_HEADER_CLIENT_CAPABILITIES);
    if (configured != null) {
      for (String declared : configured.split(",")) {
        if (!declared.trim().isEmpty()) {
          capabilities.add(declared.trim().toLowerCase(Locale.ROOT));
        }
      }
    }
    headers.set(WebClientFactory.HTTP_HEADER_CLIENT_CAPABILITIES, String.join(",", capabilities));
  }

  private static void ignore(HttpHeaders headers, String header, String reason) {
    if (headers.remove(header) != null) {
      log.warn("Ignoring catalog property {}{}: {}", PREFIX, header, reason);
    }
  }
}
