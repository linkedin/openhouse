package com.linkedin.openhouse.tables.services;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.CRC32;

/**
 * Encodes/decodes the opaque {@link ViewPageCursor} token (plan &sect;8, R3).
 *
 * <p>The token is a URL-safe Base64 encoding of a delimited field list plus a trailing checksum.
 * The checksum lets {@link #decode(String)} reject a tampered or otherwise malformed token
 * deterministically, rather than risk silently parsing corrupted field boundaries into a
 * different-looking but structurally valid cursor.
 */
public class ViewPageTokenCodec {

  private static final char FIELD_SEPARATOR = '\u0001';

  public String encode(ViewPageCursor cursor) {
    String payload =
        String.join(
            String.valueOf(FIELD_SEPARATOR),
            cursor.getDatabaseId(),
            cursor.getSortBy(),
            Integer.toString(cursor.getPageSize()),
            Integer.toString(cursor.getSourcePageIndex()),
            Integer.toString(cursor.getOffset()));
    return encodeFields(payload, checksum(payload));
  }

  private static String encodeFields(String payload, String checksum) {
    String withChecksum = payload + FIELD_SEPARATOR + checksum;
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(withChecksum.getBytes(StandardCharsets.UTF_8));
  }

  public ViewPageCursor decode(String token) {
    try {
      String decoded = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
      String[] fields = splitExact(decoded, 6);
      String payload =
          String.join(
              String.valueOf(FIELD_SEPARATOR),
              fields[0],
              fields[1],
              fields[2],
              fields[3],
              fields[4]);
      String expectedChecksum = checksum(payload);
      if (!expectedChecksum.equals(fields[5])) {
        throw new IllegalArgumentException("View page token failed integrity check");
      }
      // A checksum computed over the decoded payload cannot by itself detect every single-
      // character edit to a padding-less Base64 token: Base64's final character in a partial
      // 4-character group carries some bits that never map into any decoded byte, so a flip
      // confined to those don't-care bits can leave the decoded bytes (and therefore this
      // checksum) unchanged. Re-deriving the canonical encoding and requiring an exact match
      // against the supplied token closes that gap, since any edit the decoder cannot ignore
      // changes the decoded bytes (and so the checksum above), while any edit it can ignore
      // still changes the token's own characters away from the one canonical encoding.
      String canonical = encodeFields(payload, expectedChecksum);
      if (!canonical.equals(token)) {
        throw new IllegalArgumentException("View page token failed integrity check");
      }
      return new ViewPageCursor(
          fields[0],
          fields[1],
          Integer.parseInt(fields[2]),
          Integer.parseInt(fields[3]),
          Integer.parseInt(fields[4]));
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Malformed view page token", e);
    }
  }

  private static String[] splitExact(String decoded, int expectedFieldCount) {
    String[] fields = decoded.split(String.valueOf(FIELD_SEPARATOR), -1);
    if (fields.length != expectedFieldCount) {
      throw new IllegalArgumentException("Malformed view page token");
    }
    return fields;
  }

  private static String checksum(String payload) {
    CRC32 crc32 = new CRC32();
    crc32.update(payload.getBytes(StandardCharsets.UTF_8));
    return Long.toHexString(crc32.getValue());
  }
}
