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
            actingPrincipal,
            authorization,
            timestamp);
    return new SignedHeaders(
        authorization,
        sourceCluster,
        timestamp,
        tableUUID,
        hmac(peer.getCascadeSigningKey(), payload));
  }

  /**
   * Validates the peer assertion against the authenticated caller and exact DDL request. Absence of
   * an assertion is not itself an error; callers use the return value to distinguish a peer cascade
   * from locally initiated DDL, which follows normal ACL checks and does not fan out.
   */
  public boolean isTrustedCascade(
      String operation,
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String actingPrincipal) {
    HttpServletRequest request = currentRequest();
    if (request == null) {
      return false;
    }
    String sourceCluster = request.getHeader(SOURCE_HEADER);
    String signature = request.getHeader(SIGNATURE_HEADER);
    if (sourceCluster == null && signature == null) {
      return false;
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
    if (timestamp == null || tableUUID == null) {
      throw new AccessDeniedException("Incomplete replication cascade assertion");
    }
    long timestampMillis;
    try {
      timestampMillis = Long.parseLong(timestamp);
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
                actingPrincipal,
                authorization,
                timestamp));
    if (!constantTimeEquals(expected, signature)) {
      throw new AccessDeniedException("Invalid replication cascade signature");
    }
    return true;
  }

  private static String payload(
      String sourceCluster,
      String operation,
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String tableUUID,
      String actingPrincipal,
      String authorization,
      String timestamp) {
    List<String> fields = new ArrayList<>();
    fields.add(sourceCluster);
    fields.add(operation);
    fields.add(fromDatabaseId);
    fields.add(fromTableId);
    fields.add(toDatabaseId == null ? "" : toDatabaseId);
    fields.add(toTableId == null ? "" : toTableId);
    fields.add(tableUUID);
    fields.add(actingPrincipal);
    fields.add(sha256(authorization == null ? "" : authorization));
    fields.add(timestamp);
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    StringBuilder result = new StringBuilder();
    for (String field : fields) {
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
    private final String signature;

    SignedHeaders(
        String authorization,
        String sourceCluster,
        String timestamp,
        String tableUUID,
        String signature) {
      this.authorization = authorization;
      this.sourceCluster = sourceCluster;
      this.timestamp = timestamp;
      this.tableUUID = tableUUID;
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

    public String getSignature() {
      return signature;
    }
  }
}
