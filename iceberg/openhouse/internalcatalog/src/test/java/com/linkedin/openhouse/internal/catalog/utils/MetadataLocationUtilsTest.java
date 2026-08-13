package com.linkedin.openhouse.internal.catalog.utils;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.view.ViewProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * {@code rootMetadataFileLocation} is extracted out of {@link
 * com.linkedin.openhouse.internal.catalog.OpenHouseInternalTableOperations} so the table commit
 * path and the sibling view commit path can share metadata-file naming without sharing a codec
 * default. The helper must stay metadata-type neutral: the caller supplies the codec.
 */
public class MetadataLocationUtilsTest {

  private static final String UUID_REGEX =
      "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

  private static final Pattern UNCOMPRESSED =
      Pattern.compile("^root/00007-" + UUID_REGEX + "\\.metadata\\.json$");

  private static final Pattern GZIPPED =
      Pattern.compile("^root/00007-" + UUID_REGEX + "\\.gz\\.metadata\\.json$");

  @Test
  public void rootMetadataFileLocationUsesFiveDigitVersionAndUuid() {
    String location = MetadataLocationUtils.rootMetadataFileLocation("root", "none", 7);

    Assertions.assertTrue(
        UNCOMPRESSED.matcher(location).matches(),
        "Expected <root>/00007-<uuid>.metadata.json but was: " + location);

    // Version padding must be five digits so lexical ordering matches numeric ordering.
    Assertions.assertTrue(
        MetadataLocationUtils.rootMetadataFileLocation("root", "none", 1)
            .startsWith("root/00001-"));
    Assertions.assertTrue(
        MetadataLocationUtils.rootMetadataFileLocation("root", "none", 12345)
            .startsWith("root/12345-"));

    // The UUID is what lets concurrent writers stage the same version safely; it must differ.
    Set<String> generated = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      generated.add(MetadataLocationUtils.rootMetadataFileLocation("root", "none", 7));
    }
    Assertions.assertEquals(5, generated.size(), "Each call must produce a distinct file name");
  }

  @Test
  public void rootMetadataFileLocationUsesGzipExtension() {
    String location = MetadataLocationUtils.rootMetadataFileLocation("root", "gzip", 7);

    Assertions.assertTrue(
        GZIPPED.matcher(location).matches(),
        "Expected <root>/00007-<uuid>.gz.metadata.json but was: " + location);
  }

  /**
   * Pinned deliberately: pinned Iceberg uses {@code none} as the table metadata-compression default
   * but {@code gzip} as the view default. A helper that hard-coded the table default would silently
   * change the view file extension, so the codec must be resolved by each caller and passed in.
   */
  @Test
  public void tableAndViewDefaultsArePassedExplicitly() {
    Assertions.assertNotEquals(
        TableProperties.METADATA_COMPRESSION_DEFAULT,
        ViewProperties.METADATA_COMPRESSION_DEFAULT,
        "This test is only meaningful while the table and view codec defaults differ");

    String tableDefaultLocation =
        MetadataLocationUtils.rootMetadataFileLocation(
            "root", TableProperties.METADATA_COMPRESSION_DEFAULT, 7);
    Assertions.assertTrue(
        UNCOMPRESSED.matcher(tableDefaultLocation).matches(),
        "Table default codec must yield .metadata.json but was: " + tableDefaultLocation);

    String viewDefaultLocation =
        MetadataLocationUtils.rootMetadataFileLocation(
            "root", ViewProperties.METADATA_COMPRESSION_DEFAULT, 7);
    Assertions.assertTrue(
        GZIPPED.matcher(viewDefaultLocation).matches(),
        "View default codec must yield .gz.metadata.json but was: " + viewDefaultLocation);
  }
}
