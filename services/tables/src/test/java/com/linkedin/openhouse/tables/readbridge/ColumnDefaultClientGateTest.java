package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ColumnDefaultClientGateTest {

  private static final TableDto OPTED_IN =
      TableDto.builder()
          .tableProperties(
              Collections.singletonMap(
                  ReadBridgeConfigResolver.COLUMN_DEFAULTS_CAPABILITY_PROP, "true"))
          .build();

  /** A hotfix such as {@code 4.1.324.0} is newer than the release {@code 4.1.324} it patches. */
  @Test
  void hotfixesRankAboveTheReleaseTheyPatch() {
    Assertions.assertFalse(gate("4.1.324.0", "openhouse-java-client/4.1.324").admitsClient());
    Assertions.assertTrue(gate("4.1.324.0", "openhouse-java-client/4.1.324.0").admitsClient());
    Assertions.assertTrue(gate("4.1.324", "openhouse-java-client/4.1.324.0").admitsClient());
  }

  /** Only the fixed client's User-Agent form is admitted, even at a sufficient version. */
  @Test
  void otherUserAgentFormsAreRejectedAsClientErrors() {
    for (String userAgent :
        new String[] {
          "ReactorNetty/4.2.163", "openhouse-java-client/unknown", "ReactorNetty/dev"
        }) {
      Assertions.assertThrows(
          ColumnDefaultClientRejectedException.class,
          () -> gate("4.2.150", userAgent).checkLoad(OPTED_IN),
          userAgent);
    }
  }

  /** An unset minimum is the server's misconfiguration, not the client's: a 500, not a 422. */
  @Test
  void unsetMinimumFailsAsServerError() {
    Assertions.assertThrows(
        TableConfigUnavailableException.class,
        () -> gate("", "openhouse-java-client/4.2.176").checkLoad(OPTED_IN));
    Assertions.assertThrows(
        TableConfigUnavailableException.class,
        () ->
            gate("", "openhouse-java-client/4.2.176")
                .checkWrite(Optional.empty(), OPTED_IN.getTableProperties()));
  }

  @Test
  void invalidMinimumFailsConstruction() {
    for (String minimum : new String[] {"4.2", "4.2.150-SNAPSHOT", "latest", " 4.2.150"}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> gate(minimum, "openhouse-java-client/4.2.176"),
          minimum);
    }
  }

  private static ColumnDefaultClientGate gate(String minimum, String userAgent) {
    return new ColumnDefaultClientGate(
        minimum,
        new CatalogRequestContext() {
          @Override
          public Optional<String> clientName() {
            return Optional.of("spark");
          }

          @Override
          public Optional<String> userAgent() {
            return Optional.of(userAgent);
          }
        });
  }
}
