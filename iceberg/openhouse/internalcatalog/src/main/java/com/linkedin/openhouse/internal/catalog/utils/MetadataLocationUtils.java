package com.linkedin.openhouse.internal.catalog.utils;

import java.util.UUID;
import org.apache.iceberg.TableMetadataParser;

/**
 * Shared naming for the root metadata file, used by both the table and view commit paths.
 *
 * <p>The codec is supplied by the caller rather than resolved here because Iceberg's table and view
 * compression defaults differ ({@code none} vs {@code gzip}).
 */
public final class MetadataLocationUtils {

  private MetadataLocationUtils() {
    // no-op for util class constructor
  }

  /**
   * The UUID lets concurrent writers at the same version stage metadata side by side; the
   * zero-padded version keeps lexical ordering aligned with numeric ordering.
   */
  public static String rootMetadataFileLocation(
      String rootLocation, String codecName, int newVersion) {
    return String.format(
        "%s/%s",
        rootLocation,
        String.format(
            "%05d-%s%s",
            newVersion, UUID.randomUUID(), TableMetadataParser.getFileExtension(codecName)));
  }
}
