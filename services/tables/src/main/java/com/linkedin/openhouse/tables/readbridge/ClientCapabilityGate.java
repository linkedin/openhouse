package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;

/**
 * Admits a request to a table only if its client declares every capability the table requires. A
 * table requires capability {@code <name>} with the property {@code
 * required-capability.<name>=true}; a client declares capabilities in {@value
 * TablesMvcConstants#HTTP_HEADER_CLIENT_CAPABILITIES}. Called after authorization, before loads and
 * writes. Tables that require nothing are never checked.
 */
@Slf4j
public class ClientCapabilityGate {
  /** Prefix of the table properties that name a capability the table requires. */
  public static final String REQUIRED_CAPABILITY_PREFIX = "required-capability.";

  /** Applies column defaults on read and keeps them on write. */
  public static final String COLUMN_DEFAULTS = "column-defaults";

  private final CatalogRequestContext requestContext;

  /** @param requestContext the request's client name, User-Agent, capabilities and bypass */
  public ClientCapabilityGate(CatalogRequestContext requestContext) {
    this.requestContext = requestContext;
  }

  /**
   * Rejects loading {@code table} if the client lacks a capability the table requires.
   *
   * @throws ClientCapabilityMissingException (422) if the client lacks a required capability
   */
  public void checkLoad(TableDto table) {
    require(table.getDatabaseId() + "." + table.getTableId(), required(table.getTableProperties()));
  }

  /**
   * Rejects a write to {@code databaseId.tableId} if the client lacks a capability the {@code
   * stored} table or the {@code proposed} properties require. {@code stored} is empty when the
   * write creates the table.
   *
   * @throws ClientCapabilityMissingException (422) if the client lacks a required capability
   */
  public void checkWrite(
      String databaseId, String tableId, Optional<TableDto> stored, Map<String, String> proposed) {
    Set<String> required = required(proposed);
    stored.ifPresent(table -> required.addAll(required(table.getTableProperties())));
    require(databaseId + "." + tableId, required);
  }

  /**
   * Whether the current request's client declares {@code capability}. False outside a request. The
   * bypass does not count.
   */
  public boolean declares(String capability) {
    return requestContext.clientCapabilities().contains(capability);
  }

  /**
   * Why the current request's client cannot use {@code table}, which requires {@code capability}.
   */
  public String missingCapability(String table, String capability) {
    return String.format(
        "CLIENT_CAPABILITY_MISSING: %s requires the %s capability, which this client does not"
            + " declare (X-Client-Name: %s, User-Agent: %s, %s: %s). %s",
        table,
        capability,
        requestContext.clientName().orElse("(none)"),
        requestContext.userAgent().orElse("(none)"),
        TablesMvcConstants.HTTP_HEADER_CLIENT_CAPABILITIES,
        declared(),
        remedy(capability));
  }

  /** What to do when a client lacks {@code capability}. */
  public static String remedy(String capability) {
    return "Use a client that declares "
        + capability
        + " in "
        + TablesMvcConstants.HTTP_HEADER_CLIENT_CAPABILITIES
        + ".";
  }

  private void require(String table, Set<String> required) {
    if (required.isEmpty()) {
      return;
    }
    Optional<String> missing =
        required.stream()
            .filter(capability -> !declares(capability) && !bypassed(capability))
            .findFirst();
    log.info(
        "{} {} requiring {}: X-Client-Name {}, User-Agent {}, capabilities {}",
        missing.isPresent() ? "Rejecting" : "Admitting",
        table,
        required,
        requestContext.clientName().orElse("(none)"),
        requestContext.userAgent().orElse("(none)"),
        declared());
    if (missing.isPresent()) {
      throw new ClientCapabilityMissingException(missingCapability(table, missing.get()));
    }
  }

  /** The column-default policy bypass also skips the column-defaults capability check. */
  private boolean bypassed(String capability) {
    return COLUMN_DEFAULTS.equals(capability) && requestContext.columnDefaultPolicyBypass();
  }

  private String declared() {
    Set<String> declared = requestContext.clientCapabilities();
    return declared.isEmpty() ? "(none)" : String.join(",", new TreeSet<>(declared));
  }

  /** The capabilities {@code properties} require, in name order. */
  private static Set<String> required(Map<String, String> properties) {
    Set<String> required = new TreeSet<>();
    if (properties == null) {
      return required;
    }
    properties.forEach(
        (key, value) -> {
          if (key.startsWith(REQUIRED_CAPABILITY_PREFIX)
              && key.length() > REQUIRED_CAPABILITY_PREFIX.length()
              && value != null
              && "true".equalsIgnoreCase(value.trim())) {
            required.add(key.substring(REQUIRED_CAPABILITY_PREFIX.length()));
          }
        });
    return required;
  }
}
