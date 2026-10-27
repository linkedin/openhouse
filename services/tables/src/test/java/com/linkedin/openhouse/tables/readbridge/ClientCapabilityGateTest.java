package com.linkedin.openhouse.tables.readbridge;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClientCapabilityGateTest {

  private static final String COLUMN_DEFAULTS = "required-capability.column-defaults";
  private static final String V3 = "required-capability.v3";

  @Test
  void clientMustDeclareEveryCapabilityTheTableRequires() {
    TableDto table = table(ImmutableMap.of(COLUMN_DEFAULTS, "true", V3, "TRUE "));
    gate(ImmutableSet.of("column-defaults", "v3", "other")).checkLoad(table);
    ClientCapabilityMissingException rejected =
        Assertions.assertThrows(
            ClientCapabilityMissingException.class,
            () -> gate(ImmutableSet.of("column-defaults")).checkLoad(table));
    Assertions.assertTrue(
        rejected
            .getMessage()
            .startsWith("CLIENT_CAPABILITY_MISSING: db.tbl" + " requires the v3 capability"),
        rejected.getMessage());
  }

  /** Only {@code true} requires a capability; tables that require nothing are never checked. */
  @Test
  void onlyTrueRequiresACapability() {
    ClientCapabilityGate undeclared = gate(Collections.emptySet());
    for (Map<String, String> properties :
        ImmutableList.<Map<String, String>>of(
            ImmutableMap.of(COLUMN_DEFAULTS, "false"),
            ImmutableMap.of(COLUMN_DEFAULTS, "sometimes"),
            ImmutableMap.of("required-capability.", "true"),
            ImmutableMap.of("user.required-capability.v3", "true"),
            Collections.emptyMap())) {
      undeclared.checkLoad(table(properties));
      undeclared.checkWrite("db", "tbl", Optional.of(table(properties)), properties);
    }
  }

  /** A write needs what the stored table requires, even if it removes the requirement. */
  @Test
  void writesNeedTheStoredAndProposedRequirements() {
    ClientCapabilityGate undeclared = gate(Collections.emptySet());
    TableDto stored = table(ImmutableMap.of(COLUMN_DEFAULTS, "true"));
    Assertions.assertThrows(
        ClientCapabilityMissingException.class,
        () -> undeclared.checkWrite("db", "tbl", Optional.of(stored), Collections.emptyMap()));
    Assertions.assertThrows(
        ClientCapabilityMissingException.class,
        () ->
            undeclared.checkWrite(
                "db", "tbl", Optional.empty(), ImmutableMap.of(COLUMN_DEFAULTS, "true")));
  }

  /**
   * A rejected client learns which table, which capability and what it sent; the bypass stays out
   * of the message.
   */
  @Test
  void rejectionNamesTheClientAndTheRemedy() {
    ClientCapabilityMissingException rejected =
        Assertions.assertThrows(
            ClientCapabilityMissingException.class,
            () ->
                gate(ImmutableSet.of("v3", "branching"))
                    .checkWrite(
                        "db", "tbl", Optional.empty(), ImmutableMap.of(COLUMN_DEFAULTS, "true")));
    Assertions.assertEquals(
        "CLIENT_CAPABILITY_MISSING: db.tbl requires the column-defaults capability, which this"
            + " client does not declare (X-Client-Name: trino, User-Agent:"
            + " openhouse-java-client/4.2.100, X-OpenHouse-Client-Capabilities: branching,v3). Use"
            + " a client that declares column-defaults in X-OpenHouse-Client-Capabilities.",
        rejected.getMessage());
  }

  /** The column-default policy bypass skips only the column-defaults capability. */
  @Test
  void bypassSkipsOnlyColumnDefaults() {
    ClientCapabilityGate bypass = gate(Collections.emptySet(), true);
    bypass.checkLoad(table(ImmutableMap.of(COLUMN_DEFAULTS, "true")));
    Assertions.assertFalse(bypass.declares(ClientCapabilityGate.COLUMN_DEFAULTS));
    Assertions.assertThrows(
        ClientCapabilityMissingException.class,
        () -> bypass.checkLoad(table(ImmutableMap.of(COLUMN_DEFAULTS, "true", V3, "true"))));
  }

  /** Names compare in lowercase, like the header, so a mixed-case property cannot lock a table. */
  @Test
  void capabilityNamesIgnoreCase() {
    TableDto table = table(ImmutableMap.of("required-capability.Column-Defaults", "true"));
    gate(ImmutableSet.of("column-defaults")).checkLoad(table);
    Assertions.assertThrows(
        ClientCapabilityMissingException.class, () -> gate(ImmutableSet.of("v3")).checkLoad(table));
  }

  private static TableDto table(Map<String, String> properties) {
    return TableDto.builder().databaseId("db").tableId("tbl").tableProperties(properties).build();
  }

  private static ClientCapabilityGate gate(Set<String> capabilities) {
    return gate(capabilities, false);
  }

  private static ClientCapabilityGate gate(Set<String> capabilities, boolean bypass) {
    return new ClientCapabilityGate(
        new CatalogRequestContext() {
          @Override
          public boolean columnDefaultPolicyBypass() {
            return bypass;
          }

          @Override
          public Optional<String> clientName() {
            return Optional.of("trino");
          }

          @Override
          public Optional<String> userAgent() {
            return Optional.of("openhouse-java-client/4.2.100");
          }

          @Override
          public Set<String> clientCapabilities() {
            return capabilities;
          }
        });
  }
}
