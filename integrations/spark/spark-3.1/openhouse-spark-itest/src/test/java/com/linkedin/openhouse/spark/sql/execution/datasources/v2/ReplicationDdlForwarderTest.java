package com.linkedin.openhouse.spark.sql.execution.datasources.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

public class ReplicationDdlForwarderTest {

  @Test
  public void cascadeCanBeDisabled() {
    assertTrue(ReplicationDdlForwarder$.MODULE$.cascadeEnabled("true"));
    assertFalse(ReplicationDdlForwarder$.MODULE$.cascadeEnabled("false"));
  }

  @Test
  public void removesSourceCatalogFromDestinationRenameIdentifier() {
    Identifier sourceQualified = Identifier.of(new String[] {"openhouse_a", "db"}, "renamed");

    Identifier destinationIdentifier =
        ReplicationDdlForwarder$.MODULE$.identifierForDestination("openhouse_a", sourceQualified);

    assertArrayEquals(new String[] {"db"}, destinationIdentifier.namespace());
    assertEquals("renamed", destinationIdentifier.name());
  }

  @Test
  public void extractsUniqueReplicationDestinations() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        "policies",
        "{\"replication\":{\"config\":["
            + "{\"destination\":\"'clusterB'\"},"
            + "{\"destination\":\"clusterC\"},"
            + "{\"destination\":\"clusterB\"}]}}");

    scala.collection.Seq<String> destinations =
        ReplicationDdlForwarder$.MODULE$.replicationDestinations(properties);

    assertEquals(2, destinations.size());
    assertEquals("clusterB", destinations.head());
    assertEquals("clusterC", destinations.last());
  }

  @Test
  public void doesNotForwardFromReplicaTables() {
    Map<String, String> properties = new HashMap<>();
    properties.put("openhouse.isTableReplicated", "TRUE");
    properties.put("policies", "{\"replication\":{\"config\":[{\"destination\":\"clusterB\"}]}}");

    assertEquals(0, ReplicationDdlForwarder$.MODULE$.replicationDestinations(properties).size());
  }

  @Test
  public void rejectsMalformedReplicationConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("policies", "{\"replication\":{\"config\":{\"destination\":\"clusterB\"}}}");

    assertThrows(
        IllegalArgumentException.class,
        () -> ReplicationDdlForwarder$.MODULE$.replicationDestinations(properties));
  }
}
