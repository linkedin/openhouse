package com.linkedin.openhouse.tables.config;

import java.net.URI;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Replication peer endpoints configured in the cluster YAML. */
@Getter
@Setter
@ConfigurationProperties(prefix = "cluster.replication")
public class ReplicationProperties implements InitializingBean {

  private Map<String, Peer> peers = new LinkedHashMap<>();

  @Override
  public void afterPropertiesSet() {
    Set<String> normalizedClusterIds = new HashSet<>();
    peers.forEach(
        (clusterId, peer) -> {
          if (clusterId == null || clusterId.trim().isEmpty() || peer == null) {
            throw new IllegalStateException("Replication peers require a cluster ID and settings");
          }
          if (!normalizedClusterIds.add(clusterId.toLowerCase(Locale.ROOT))) {
            throw new IllegalStateException(
                String.format("Replication peer IDs must be unique ignoring case: %s", clusterId));
          }
          validateTablesApiBaseUri(clusterId, peer.getTablesApiBaseUri());
          if (peer.getCascadeSigningKey() == null
              || peer.getCascadeSigningKey()
                      .getBytes(java.nio.charset.StandardCharsets.UTF_8)
                      .length
                  < 32) {
            throw new IllegalStateException(
                String.format(
                    "Replication peer %s requires a cascade-signing-key of at least 32 bytes",
                    clusterId));
          }
        });
  }

  public Peer getPeer(String clusterId) {
    Peer match = null;
    for (Map.Entry<String, Peer> entry : peers.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(clusterId)) {
        if (match != null) {
          throw new IllegalStateException(
              String.format("Replication peer ID %s is ambiguous", clusterId));
        }
        match = entry.getValue();
      }
    }
    return match;
  }

  private void validateTablesApiBaseUri(String clusterId, String baseUri) {
    if (baseUri == null || baseUri.trim().isEmpty()) {
      throw new IllegalStateException(
          String.format("Replication peer %s requires tables-api-base-uri", clusterId));
    }

    URI uri;
    try {
      uri = URI.create(baseUri);
    } catch (IllegalArgumentException exception) {
      throw new IllegalStateException(
          String.format("Replication peer %s has an invalid tables-api-base-uri", clusterId),
          exception);
    }

    if (!uri.isAbsolute()
        || uri.getHost() == null
        || uri.getUserInfo() != null
        || uri.getQuery() != null
        || uri.getFragment() != null
        || !("http".equalsIgnoreCase(uri.getScheme())
            || "https".equalsIgnoreCase(uri.getScheme()))) {
      throw new IllegalStateException(
          String.format("Replication peer %s has an invalid tables-api-base-uri", clusterId));
    }
  }

  @Getter
  @Setter
  public static class Peer {
    private String tablesApiBaseUri;
    private String cascadeSigningKey;
  }
}
