package com.linkedin.openhouse.tables.readbridge;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

class ColumnDefaultClientGateTest {
  private static final String MINIMUM = "4.2.150";
  private static final Map<String, String> OPTED_IN =
      Collections.singletonMap(ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP, "true");

  @AfterEach
  void clearRequest() {
    RequestContextHolder.resetRequestAttributes();
  }

  private static ColumnDefaultClientGate gate(String admittedNames) {
    return new ColumnDefaultClientGate(MINIMUM, admittedNames, new CatalogRequestContext() {});
  }

  private static void request(String clientName, String version) {
    MockHttpServletRequest request = new MockHttpServletRequest();
    if (clientName != null) {
      request.addHeader("X-Client-Name", clientName);
    }
    request.addHeader("User-Agent", "openhouse-java-client/" + version);
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
  }

  private static void assertAdmitted(ColumnDefaultClientGate gate, String name, String version) {
    request(name, version);
    assertDoesNotThrow(() -> gate.check(null, OPTED_IN));
  }

  private static void assertRefused(ColumnDefaultClientGate gate, String name, String version) {
    request(name, version);
    assertThrows(ColumnDefaultClientRejectedException.class, () -> gate.check(null, OPTED_IN));
  }

  @Test
  void defaultListAdmitsSparkOnly() {
    ColumnDefaultClientGate gate = gate("spark");
    assertAdmitted(gate, "spark", MINIMUM);
    assertRefused(gate, "ddm-replication", MINIMUM);
    assertRefused(gate, null, MINIMUM);
  }

  @Test
  void configuredSecondNameIsAdmittedAtMinimumAndRefusedBelow() {
    ColumnDefaultClientGate gate = gate("spark,ddm-replication");
    assertAdmitted(gate, "spark", MINIMUM);
    assertAdmitted(gate, "ddm-replication", MINIMUM);
    assertAdmitted(gate, "ddm-replication", "4.2.168");
    assertRefused(gate, "ddm-replication", "4.2.149");
    assertRefused(gate, "ddm-replication", "4.2.150-SNAPSHOT");
    assertRefused(gate, "spark", "4.2.149");
  }

  @Test
  void nameNotOnListIsRefused() {
    ColumnDefaultClientGate gate = gate("spark,ddm-replication");
    assertRefused(gate, "trino", MINIMUM);
    assertRefused(gate, "ddm", MINIMUM);
    assertRefused(gate, "SPARK", MINIMUM);
  }

  @Test
  void entriesAreTrimmedAndEmptyEntriesIgnored() {
    ColumnDefaultClientGate gate = gate(" spark ,, ddm-replication ,  ,");
    assertAdmitted(gate, "spark", MINIMUM);
    assertAdmitted(gate, "ddm-replication", MINIMUM);
    assertRefused(gate, "", MINIMUM);
    assertRefused(gate, " ", MINIMUM);
  }

  @Test
  void emptyListAdmitsNoClient() {
    ColumnDefaultClientGate gate = gate(" , ");
    assertRefused(gate, "spark", MINIMUM);
  }

  @Test
  void tablesNotOptedInAreNeverChecked() {
    request("trino", "0.0.1");
    assertDoesNotThrow(() -> gate("spark").check(null, Collections.emptyMap()));
  }
}
