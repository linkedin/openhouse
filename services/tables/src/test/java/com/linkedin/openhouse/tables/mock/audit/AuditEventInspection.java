package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Inspects every readable property an audit event exposes, rather than relying on {@code
 * toString()}, which may be partial or overridden.
 */
public final class AuditEventInspection {

  private AuditEventInspection() {}

  /** Returns each readable JavaBean property of {@code event} by name. */
  public static Map<String, Object> properties(Object event) {
    Map<String, Object> values = new LinkedHashMap<>();
    try {
      for (PropertyDescriptor descriptor :
          Introspector.getBeanInfo(event.getClass(), Object.class).getPropertyDescriptors()) {
        if (descriptor.getReadMethod() != null) {
          values.put(descriptor.getName(), descriptor.getReadMethod().invoke(event));
        }
      }
    } catch (IntrospectionException | IllegalAccessException | InvocationTargetException e) {
      throw new AssertionError("Unable to read audit event properties", e);
    }
    assertFalse(values.isEmpty(), "Audit event must expose readable properties");
    return values;
  }

  /**
   * Asserts no exposed property carries a throwable, a populated cause/stacktrace field, or any of
   * the forbidden fragments.
   */
  public static void assertNoSensitiveProperties(Object event, String... forbidden) {
    for (Map.Entry<String, Object> property : properties(event).entrySet()) {
      String name = property.getKey();
      Object value = property.getValue();
      assertFalse(
          value instanceof Throwable, "Audit property '" + name + "' must not carry a throwable");
      String lowerName = name.toLowerCase(Locale.ROOT);
      if (lowerName.contains("cause") || lowerName.contains("stacktrace")) {
        assertNull(value, "Audit property '" + name + "' must be absent");
      }
      String rendered = String.valueOf(value);
      for (String fragment : forbidden) {
        assertFalse(
            rendered.contains(fragment),
            "Audit property '" + name + "' leaked a sensitive fragment: " + rendered);
      }
    }
  }

  /** Asserts the named property exists on the event, so a renamed field cannot pass silently. */
  public static void assertHasProperty(Object event, String propertyName) {
    assertTrue(
        properties(event).containsKey(propertyName),
        "Audit event must expose property '" + propertyName + "'");
  }
}
