package com.linkedin.openhouse.tables.toggle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableFeatureToggleTest {
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String FEATURE = "feature";
  private static final String PROPERTY = "feature.enabled";

  private TestTableFeatureToggle featureToggle;

  @BeforeEach
  void setUp() {
    featureToggle = new TestTableFeatureToggle();
  }

  @Test
  void testUsesServerToggleWhenPropertyIsAbsent() {
    TableDto tableDto = tableWithProperties(Collections.emptyMap());
    featureToggle.serverDecision = true;

    assertTrue(featureToggle.isFeatureActivatedWithOverride(tableDto, FEATURE));
    assertEquals(1, featureToggle.serverInvocationCount);
  }

  @Test
  void testUsesServerToggleWhenPropertiesAreNull() {
    TableDto tableDto = tableWithProperties(null);
    featureToggle.serverDecision = false;

    assertFalse(featureToggle.isFeatureActivatedWithOverride(tableDto, FEATURE));
    assertEquals(1, featureToggle.serverInvocationCount);
  }

  @Test
  void testTruePropertyOptsInWithoutServerToggle() {
    TableDto tableDto = tableWithProperties(Collections.singletonMap(PROPERTY, "true"));

    assertTrue(featureToggle.isFeatureActivatedWithOverride(tableDto, FEATURE));
    assertEquals(0, featureToggle.serverInvocationCount);
  }

  @Test
  void testFalsePropertyOptsOutWithoutServerToggle() {
    TableDto tableDto = tableWithProperties(Collections.singletonMap(PROPERTY, "false"));

    assertFalse(featureToggle.isFeatureActivatedWithOverride(tableDto, FEATURE));
    assertEquals(0, featureToggle.serverInvocationCount);
  }

  @Test
  void testPropertyParsingIgnoresCaseAndWhitespace() {
    TableDto tableDto = tableWithProperties(Collections.singletonMap(PROPERTY, " TRUE "));

    assertTrue(featureToggle.isFeatureActivatedWithOverride(tableDto, FEATURE));
    assertEquals(0, featureToggle.serverInvocationCount);
  }

  @Test
  void testUnparseablePropertyFailsClosed() {
    TableDto tableDto = tableWithProperties(Collections.singletonMap(PROPERTY, "sometimes"));
    featureToggle.serverDecision = true;

    assertFalse(featureToggle.isFeatureActivatedWithOverride(tableDto, FEATURE));
    assertEquals(0, featureToggle.serverInvocationCount);
  }

  private static TableDto tableWithProperties(Map<String, String> properties) {
    return TableDto.builder()
        .databaseId(DATABASE)
        .tableId(TABLE)
        .tableProperties(properties)
        .build();
  }

  private static class TestTableFeatureToggle implements TableFeatureToggle {
    private boolean serverDecision;
    private int serverInvocationCount;

    @Override
    public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
      assertEquals(DATABASE, databaseId);
      assertEquals(TABLE, tableId);
      assertEquals(FEATURE, featureId);
      serverInvocationCount++;
      return serverDecision;
    }
  }
}
