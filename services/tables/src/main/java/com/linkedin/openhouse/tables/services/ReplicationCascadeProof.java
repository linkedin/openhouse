package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.config.ReplicationProperties;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/** Signs peer cascade requests and verifies that only a configured peer can delegate a DDL call. */
@Component
public class ReplicationCascadeProof {

  public static final String SOURCE_HEADER = "X-OpenHouse-Replication-Source";
  public static final String TIMESTAMP_HEADER = "X-OpenHouse-Replication-Timestamp";
  public static final String TABLE_UUID_HEADER = "X-OpenHouse-Replication-Table-UUID";
  public static final String TABLE_CREATION_TIME_HEADER =
      "X-OpenHouse-Replication-Table-Creation-Time";
  public static final String SIGNATURE_HEADER = "X-OpenHouse-Replication-Signature";
  public static final String AUTHORIZATION_HEADER = "Authorization";
  private static final long MAX_CLOCK_SKEW_MILLIS = 5 * 60 * 1000L;
  private static final String HMAC_ALGORITHM = "HmacSHA256";

  @Autowired private ReplicationProperties replicationProperties;

  @Autowired private ClusterProperties clusterProperties;

  public SignedHeaders sign(
      ReplicationProperties.Peer peer,
      String operation,
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String tableUUID,
      long tableCreationTime,
      String actingPrincipal) {
    String authorization = currentAuthorizationHeader();
    String sourceCluster = clusterProperties.getClusterName();
    String timestamp = Long.toString(System.currentTimeMillis());
    String payload =
        payload(
            sourceCluster,
            operation,
            fromDatabaseId,
            fromTableId,
            toDatabaseId,
            toTableId,
            tableUUID,
            tableCreationTime,
            actingPrincipal,
            authorization,
            timestamp);
    return new SignedHeaders(
        authorization,
        sourceCluster,
        timestamp,
        tableUUID,
        Long.toString(tableCreationTime),
        hmac(peer.getCascadeSigningKey(), payload));
  }

  /**
   * Validates the peer assertion against the authenticated caller and exact DDL request. Absence of
   * an assertion is not itself an error; an empty result identifies a direct operation.
   */
  public Optional<TrustedCascade> getTrustedCascade(
      String operation,
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String actingPrincipal) {
    HttpServletRequest request = currentRequest();
    if (request == null) {
      return Optional.empty();
    }
    String sourceCluster = request.getHeader(SOURCE_HEADER);
    String signature = request.getHeader(SIGNATURE_HEADER);
    if (sourceCluster == null && signature == null) {
      return Optional.empty();
    }
    if (sourceCluster == null || signature == null) {
      throw new AccessDeniedException("Invalid replication cascade assertion");
    }

    ReplicationProperties.Peer sourcePeer = replicationProperties.getPeer(sourceCluster);
    if (sourcePeer == null) {
      throw new AccessDeniedException("Replication cascade source is not a configured peer");
    }

    String timestamp = request.getHeader(TIMESTAMP_HEADER);
    String tableUUID = request.getHeader(TABLE_UUID_HEADER);
    String tableCreationTime = request.getHeader(TABLE_CREATION_TIME_HEADER);
    if (timestamp == null
        || tableUUID == null
        || tableUUID.trim().isEmpty()
        || tableCreationTime == null) {
      throw new AccessDeniedException("Incomplete replication cascade assertion");
    }
    long timestampMillis;
    long tableCreationTimeMillis;
    try {
      timestampMillis = Long.parseLong(timestamp);
      tableCreationTimeMillis = Long.parseLong(tableCreationTime);
    } catch (NumberFormatException exception) {
      throw new AccessDeniedException("Invalid replication cascade timestamp");
    }
    long currentTime = System.currentTimeMillis();
    if (timestampMillis < currentTime - MAX_CLOCK_SKEW_MILLIS
        || timestampMillis > currentTime + MAX_CLOCK_SKEW_MILLIS) {
      throw new AccessDeniedException("Expired replication cascade assertion");
    }

    String authorization = request.getHeader(AUTHORIZATION_HEADER);
    String expected =
        hmac(
            sourcePeer.getCascadeSigningKey(),
            payload(
                sourceCluster,
                operation,
                fromDatabaseId,
                fromTableId,
                toDatabaseId,
                toTableId,
                tableUUID,
                tableCreationTimeMillis,
                actingPrincipal,
                authorization,
                timestamp));
    if (!constantTimeEquals(expected, signature)) {
      throw new AccessDeniedException("Invalid replication cascade signature");
    }
    return Optional.of(new TrustedCascade(sourceCluster, tableUUID, tableCreationTimeMillis));
  }

  private static String payload(
      String sourceCluster,
      String operation,
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String tableUUID,
      long tableCreationTime,
      String actingPrincipal,
      String authorization,
      String timestamp) {
    List<String> fields = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    fields.add(sourceCluster);
    fieldNames.add("source cluster");
    fields.add(operation);
    fieldNames.add("operation");
    fields.add(fromDatabaseId);
    fieldNames.add("source database");
    fields.add(fromTableId);
    fieldNames.add("source table");
    fields.add(toDatabaseId == null ? "" : toDatabaseId);
    fieldNames.add("destination database");
    fields.add(toTableId == null ? "" : toTableId);
    fieldNames.add("destination table");
    fields.add(tableUUID);
    fieldNames.add("table UUID");
    fields.add(Long.toString(tableCreationTime));
    fieldNames.add("table creation time");
    fields.add(actingPrincipal);
    fieldNames.add("authenticated principal");
    fields.add(sha256(authorization == null ? "" : authorization));
    fieldNames.add("credential fingerprint");
    fields.add(timestamp);
    fieldNames.add("timestamp");
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      String field = fields.get(i);
      if (field == null) {
        throw new IllegalStateException(
            String.format("Replication cascade assertion is missing %s", fieldNames.get(i)));
      }
      if (result.length() > 0) {
        result.append('.');
      }
      result.append(encoder.encodeToString(field.getBytes(StandardCharsets.UTF_8)));
    }
    return result.toString();
  }

  private static String hmac(String key, String payload) {
    if (key == null || key.getBytes(StandardCharsets.UTF_8).length < 32) {
      throw new IllegalStateException(
          "Replication peer cascade signing key must be 32 bytes or more");
    }
    try {
      Mac mac = Mac.getInstance(HMAC_ALGORITHM);
      mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM));
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException | InvalidKeyException exception) {
      throw new IllegalStateException("Unable to sign replication cascade request", exception);
    }
  }

  private static String sha256(String value) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("Unable to hash replication cascade credential", exception);
    }
  }

  private static boolean constantTimeEquals(String expected, String actual) {
    return MessageDigest.isEqual(
        expected.getBytes(StandardCharsets.UTF_8), actual.getBytes(StandardCharsets.UTF_8));
  }

  private static String currentAuthorizationHeader() {
    HttpServletRequest request = currentRequest();
    String authorization = request == null ? null : request.getHeader(AUTHORIZATION_HEADER);
    if (authorization == null || !authorization.startsWith("Bearer ")) {
      throw new IllegalStateException(
          "A bearer credential is required to preserve the caller identity across replication");
    }
    return authorization;
  }

  private static HttpServletRequest currentRequest() {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    return attributes instanceof ServletRequestAttributes
        ? ((ServletRequestAttributes) attributes).getRequest()
        : null;
  }

  public static final class SignedHeaders {
    private final String authorization;
    private final String sourceCluster;
    private final String timestamp;
    private final String tableUUID;
    private final String tableCreationTime;
    private final String signature;

    SignedHeaders(
        String authorization,
        String sourceCluster,
        String timestamp,
        String tableUUID,
        String tableCreationTime,
        String signature) {
      this.authorization = authorization;
      this.sourceCluster = sourceCluster;
      this.timestamp = timestamp;
      this.tableUUID = tableUUID;
      this.tableCreationTime = tableCreationTime;
      this.signature = signature;
    }

    public String getAuthorization() {
      return authorization;
    }

    public String getSourceCluster() {
      return sourceCluster;
    }

    public String getTimestamp() {
      return timestamp;
    }

    public String getTableUUID() {
      return tableUUID;
    }

    public String getTableCreationTime() {
      return tableCreationTime;
    }

    public String getSignature() {
      return signature;
    }
  }

  public static final class TrustedCascade {
    private final String sourceCluster;
    private final String sourceTableUUID;
    private final long sourceCreationTime;

    public TrustedCascade(String sourceCluster, String sourceTableUUID, long sourceCreationTime) {
      this.sourceCluster = sourceCluster;
      this.sourceTableUUID = sourceTableUUID;
      this.sourceCreationTime = sourceCreationTime;
    }

    public String getSourceCluster() {
      return sourceCluster;
    }

    public String getSourceTableUUID() {
      return sourceTableUUID;
    }

    public long getSourceCreationTime() {
      return sourceCreationTime;
    }
  }
}
