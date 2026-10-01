package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Locale;
import org.junit.jupiter.api.Test;

public class ViewsFeatureGateTest {

  @Test
  public void gateUsesLowercaseDatabaseLiteralRuleAndValidProbeKey() {
    RecordingToggle toggle = new RecordingToggle(true);
    ViewsFeatureGate gate = new ViewsFeatureGate(toggle);

    assertTrue(gate.isEnabled("DbA"));

    assertEquals("dba", toggle.databaseId);
    assertEquals(
        "DbA".toLowerCase(Locale.ROOT),
        toggle.databaseId,
        "The rule matcher is case-sensitive, so the gate must canonicalize with Locale.ROOT.");
    assertEquals(ViewsFeatureGate.GATE_PROBE_TABLE_ID, toggle.tableId);
    assertTrue(
        toggle.tableId.matches("[A-Za-z0-9_]+"),
        "The probe id must satisfy the existing HTS toggle status key charset.");
    assertEquals("views", toggle.featureId);
  }

  @Test
  public void defaultLocaleCannotChangeDatabaseCanonicalization() {
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      RecordingToggle toggle = new RecordingToggle(true);
      ViewsFeatureGate gate = new ViewsFeatureGate(toggle);

      assertTrue(gate.isEnabled("IDB"));

      assertEquals("idb", toggle.databaseId);
    } finally {
      Locale.setDefault(original);
    }
  }

  @Test
  public void missingRuleDefaultsOff() {
    assertFalse(new ViewsFeatureGate(new RecordingToggle(false)).isEnabled("dba"));
  }

  @Test
  public void gateNeverAllowsUserWritableTablePropertyOverride() {
    TableFeatureToggle overrideExplodes =
        new TableFeatureToggle() {
          @Override
          public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
            return false;
          }

          @Override
          public boolean isFeatureActivatedWithOverride(TableDto tableDto, String featureId) {
            throw new AssertionError("views.enabled must not be honored from table properties");
          }
        };

    assertFalse(new ViewsFeatureGate(overrideExplodes).isEnabled("dba"));
  }

  @Test
  public void nullDatabaseIdIsRejectedBeforeCallingToggleBackend() {
    RecordingToggle toggle = new RecordingToggle(true);
    ViewsFeatureGate gate = new ViewsFeatureGate(toggle);

    assertThrows(NullPointerException.class, () -> gate.isEnabled(null));
    assertEquals(0, toggle.calls);
  }

  private static class RecordingToggle implements TableFeatureToggle {
    private final boolean enabled;
    private int calls;
    private String databaseId;
    private String tableId;
    private String featureId;

    private RecordingToggle(boolean enabled) {
      this.enabled = enabled;
    }

    @Override
    public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
      calls++;
      this.databaseId = databaseId;
      this.tableId = tableId;
      this.featureId = featureId;
      return enabled;
    }
  }
}
