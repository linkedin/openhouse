package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ColumnDefaultClientGateTest {

  /** A hotfix such as {@code 4.1.324.0} is newer than the release {@code 4.1.324} it patches. */
  @Test
  void hotfixesRankAboveTheReleaseTheyPatch() {
    Assertions.assertFalse(gate("4.1.324.0", "4.1.324").admitsClient());
    Assertions.assertTrue(gate("4.1.324.0", "4.1.324.0").admitsClient());
    Assertions.assertTrue(gate("4.1.324", "4.1.324.0").admitsClient());
  }

  private static ColumnDefaultClientGate gate(String minimum, String release) {
    return new ColumnDefaultClientGate(
        minimum,
        new CatalogRequestContext() {
          @Override
          public Optional<String> clientName() {
            return Optional.of("spark");
          }

          @Override
          public Optional<String> userAgent() {
            return Optional.of("openhouse-java-client/" + release);
          }
        });
  }
}
