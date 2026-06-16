package com.linkedin.openhouse.jobs.spark;

import java.nio.file.Paths;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pure-Java unit tests for the backup-manifest path derivation in {@link Operations}. No Spark
 * session, no filesystem — exercises the scheme/authority handling that broke retention backups
 * when data files are stored as fully-qualified {@code hdfs://authority/...} URIs.
 *
 * <p>These guard against a regression where {@code java.nio.file.Paths} was used to derive parent
 * paths: it is not URI-aware and collapses {@code hdfs://authority} into {@code hdfs:/authority},
 * dropping the authority. The mangled path later resolves against the default filesystem root and
 * fails with {@code Permission denied ... inode="/"}.
 */
public class OperationsBackupPathTest {

  @Test
  public void dataFileParentPreservesSchemeAndAuthority() {
    String dataFile =
        "hdfs://ltx1-holdem/data/openhouse/db/tbl-uuid/data/datepartition=2026-03-26-00/f.orc";

    String parent = Operations.dataFileParent(dataFile);

    // Authority (//ltx1-holdem) must be retained; the old Paths.get path produced
    // "hdfs:/ltx1-holdem/..." (single slash, authority dropped).
    Assertions.assertEquals(
        "hdfs://ltx1-holdem/data/openhouse/db/tbl-uuid/data/datepartition=2026-03-26-00", parent);
  }

  @Test
  public void dataFileParentPreservesBarePath() {
    String dataFile = "/data/openhouse/db/tbl-uuid/data/datepartition=2026-03-06-00/f.orc";

    Assertions.assertEquals(
        "/data/openhouse/db/tbl-uuid/data/datepartition=2026-03-06-00",
        Operations.dataFileParent(dataFile));
  }

  @Test
  public void manifestDestPathForQualifiedDataFileKeepsAuthorityAndInsertsBackupDir() {
    // Mirrors the production chain in prepareBackupDataManifests + writeBackupDataManifests.
    // Iceberg stores table.location() bare (no scheme), but newer data files are fully qualified.
    String tableLocation = "/data/openhouse/db/tbl-uuid";
    String dataFile =
        "hdfs://ltx1-holdem/data/openhouse/db/tbl-uuid/data/datepartition=2026-03-26-00/f.orc";

    String partitionPath = Operations.dataFileParent(dataFile);
    String manifestSource = new Path(partitionPath, "data_manifest_1.json").toString();
    Path dest = Operations.getTrashPath(tableLocation, manifestSource, ".backup");

    Assertions.assertEquals(
        "hdfs://ltx1-holdem/data/openhouse/db/tbl-uuid/.backup/data/"
            + "datepartition=2026-03-26-00/data_manifest_1.json",
        dest.toString());
  }

  @Test
  public void manifestDestPathForBareDataFileInsertsBackupDir() {
    String tableLocation = "/data/openhouse/db/tbl-uuid";
    String dataFile = "/data/openhouse/db/tbl-uuid/data/datepartition=2026-03-06-00/f.orc";

    String partitionPath = Operations.dataFileParent(dataFile);
    String manifestSource = new Path(partitionPath, "data_manifest_1.json").toString();
    Path dest = Operations.getTrashPath(tableLocation, manifestSource, ".backup");

    Assertions.assertEquals(
        "/data/openhouse/db/tbl-uuid/.backup/data/datepartition=2026-03-06-00/data_manifest_1.json",
        dest.toString());
  }

  /**
   * Negative test pinning the regression: deriving the parent with {@link java.nio.file.Paths}
   * (instead of Hadoop {@link Path}) collapses the {@code //} authority separator, so the authority
   * is dropped and leaks into the path as a bogus top-level directory. The resulting backup
   * destination resolves under the filesystem root, which is what produced the {@code Permission
   * denied ... inode="/"} failure in production.
   */
  @Test
  public void nioPathsDropsAuthorityAndProducesWrongDestPath() {
    String tableLocation = "/data/openhouse/db/tbl-uuid";
    String dataFile =
        "hdfs://ltx1-holdem/data/openhouse/db/tbl-uuid/data/datepartition=2026-03-26-00/f.orc";

    // Buggy derivation: java.nio.file.Paths is not URI-aware and collapses "hdfs://" into "hdfs:/".
    String nioParent = Paths.get(dataFile).getParent().toString();
    Assertions.assertEquals(
        "hdfs:/ltx1-holdem/data/openhouse/db/tbl-uuid/data/datepartition=2026-03-26-00",
        nioParent,
        "java.nio.file.Paths drops the // authority separator");
    Assertions.assertNotEquals(
        Operations.dataFileParent(dataFile),
        nioParent,
        "nio-derived parent must differ from the correct Hadoop Path parent");

    String manifestSource = new Path(nioParent, "data_manifest_1.json").toString();
    Path wrongDest = Operations.getTrashPath(tableLocation, manifestSource, ".backup");

    // The authority is gone and has leaked into the path, so the file would be created under the
    // filesystem root ("/ltx1-holdem/...") instead of under the table location.
    Assertions.assertNull(
        wrongDest.toUri().getAuthority(), "authority is lost in the nio-derived path");
    Assertions.assertTrue(
        wrongDest.toUri().getPath().startsWith("/ltx1-holdem/"),
        "authority leaks into the path, rooting it at the filesystem root");
    Assertions.assertNotEquals(
        "hdfs://ltx1-holdem/data/openhouse/db/tbl-uuid/.backup/data/"
            + "datepartition=2026-03-26-00/data_manifest_1.json",
        wrongDest.toString(),
        "nio-derived destination differs from the correct backup path");
  }
}
