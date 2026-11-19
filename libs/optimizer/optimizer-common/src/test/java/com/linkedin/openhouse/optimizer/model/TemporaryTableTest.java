package com.linkedin.openhouse.optimizer.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TemporaryTableTest {

  @Test
  void expirationTimeParsesEpochMillis() {
    assertThat(
            TemporaryTable.expirationTime(
                Collections.singletonMap(TemporaryTable.EXPIRATION_TIME_PROPERTY, "1700000000000")))
        .contains(Instant.ofEpochMilli(1700000000000L));
  }

  @Test
  void expirationTimeIsEmptyWithoutTheProperty() {
    assertThat(TemporaryTable.expirationTime(Collections.emptyMap())).isEmpty();
  }

  @Test
  void expirationTimeIsEmptyForAnUnparsableValue() {
    assertThat(
            TemporaryTable.expirationTime(
                Collections.singletonMap(TemporaryTable.EXPIRATION_TIME_PROPERTY, "tomorrow")))
        .isEmpty();
  }
}
