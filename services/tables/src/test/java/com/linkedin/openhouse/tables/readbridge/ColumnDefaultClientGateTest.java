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
          .databaseId("db")
          .tableId("tbl")
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

  /**
   * A rejected client learns which table, what it sent, which clients are supported and where to
   * read more; the bypass stays out of the message.
   */
  @Test
  void rejectionNamesTheSupportedClientsAndTheConfiguredLink() {
    ColumnDefaultClientRejectedException rejected =
        Assertions.assertThrows(
            ColumnDefaultClientRejectedException.class,
            () ->
                gate("4.2.150", "go/oh/column-defaults", "trino", "openhouse-java-client/4.2.100")
                    .checkLoad(OPTED_IN));
    Assertions.assertEquals(
        "CLIENT_CAPABILITY_MISSING: db.tbl requires the column-defaults capability, which this"
            + " client does not have (X-Client-Name: trino, User-Agent:"
            + " openhouse-java-client/4.2.100). Update the client, or use a supported client:"
            + " spark with openhouse-java-client 4.2.150 or newer. More information:"
            + " go/oh/column-defaults",
        rejected.getMessage());
    String withoutLink =
        Assertions.assertThrows(
                ColumnDefaultClientRejectedException.class,
                () ->
                    gate("4.2.150", "", "trino", "openhouse-java-client/4.2.100")
                        .checkWrite(
                            "db", "new_tbl", Optional.empty(), OPTED_IN.getTableProperties()))
            .getMessage();
    Assertions.assertTrue(withoutLink.startsWith("CLIENT_CAPABILITY_MISSING: db.new_tbl "));
    Assertions.assertTrue(withoutLink.endsWith("4.2.150 or newer."), withoutLink);
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
                .checkWrite("db", "tbl", Optional.empty(), OPTED_IN.getTableProperties()));
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
    return gate(minimum, "", "spark", userAgent);
  }

  private static ColumnDefaultClientGate gate(
      String minimum, String helpUrl, String clientName, String userAgent) {
    return new ColumnDefaultClientGate(
        minimum,
        helpUrl,
        new CatalogRequestContext() {
          @Override
          public Optional<String> clientName() {
            return Optional.of(clientName);
          }

          @Override
          public Optional<String> userAgent() {
            return Optional.of(userAgent);
          }
        });
  }
}
