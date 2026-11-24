package com.linkedin.openhouse.optimizer.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TemporaryTableTest {

  @Test
  void flaggedTableWithValidTimeHasThatExpirationTime() {
    Map<String, String> properties = temporaryProperties("2026-01-01T00:00:00Z");
    assertThat(TemporaryTable.isTemporary(properties)).isTrue();
    assertThat(TemporaryTable.expirationTime(properties))
        .contains(Instant.parse("2026-01-01T00:00:00Z"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"TRUE", "false", ""})
  void flagValuesOtherThanExactTrueAreNotTemporary(String flagValue) {
    Map<String, String> properties = temporaryProperties("2026-01-01T00:00:00Z");
    properties.put(TemporaryTable.TEMPORARY_PROPERTY, flagValue);
    assertThat(TemporaryTable.isTemporary(properties)).isFalse();
    assertThat(TemporaryTable.expirationTime(properties)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "2026-01-01T00:00:00.000Z",
        "2026-01-01T00:00:00+00:00",
        "1700000000000",
        "2026-02-30T00:00:00Z",
        "+2026-01-01T00:00:00Z",
        "+02026-01-01T00:00:00Z",
        "-2026-01-01T00:00:00Z",
        "12026-01-01T00:00:00Z"
      })
  void flaggedTableWithMalformedTimeIsTemporaryWithoutExpirationTime(String expirationTime) {
    Map<String, String> properties = temporaryProperties(expirationTime);
    assertThat(TemporaryTable.isTemporary(properties)).isTrue();
    assertThat(TemporaryTable.expirationTime(properties)).isEmpty();
  }

  @Test
  void flaggedTableWithoutTimeHasNoExpirationTime() {
    Map<String, String> properties =
        Collections.singletonMap(TemporaryTable.TEMPORARY_PROPERTY, "true");
    assertThat(TemporaryTable.isTemporary(properties)).isTrue();
    assertThat(TemporaryTable.expirationTime(properties)).isEmpty();
  }

  @Test
  void timeWithoutFlagGivesNoExpirationTime() {
    Map<String, String> properties =
        Collections.singletonMap(TemporaryTable.EXPIRATION_TIME_PROPERTY, "2026-01-01T00:00:00Z");
    assertThat(TemporaryTable.isTemporary(properties)).isFalse();
    assertThat(TemporaryTable.expirationTime(properties)).isEmpty();
  }

  private static Map<String, String> temporaryProperties(String expirationTime) {
    Map<String, String> properties = new HashMap<>();
    properties.put(TemporaryTable.TEMPORARY_PROPERTY, "true");
    properties.put(TemporaryTable.EXPIRATION_TIME_PROPERTY, expirationTime);
    return properties;
  }
}
