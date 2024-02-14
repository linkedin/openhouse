package com.linkedin.openhouse.internal.catalog;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.Snapshot;

public final class IcebergTestUtil {
  private static final String SNAPSHOTS_FILE = "serialized_snapshots.json";
  private static final String EXTRA_SNAPSHOTS_FILE = "extra_serialized_snapshots.json";

  private IcebergTestUtil() {}

  public static List<Snapshot> getSnapshots() throws IOException {
    return loadSnapshots(SNAPSHOTS_FILE);
  }

  public static List<Snapshot> getExtraSnapshots() throws IOException {
    return loadSnapshots(EXTRA_SNAPSHOTS_FILE);
  }

  private static List<Snapshot> loadSnapshots(String snapshotFile) throws IOException {
    InputStream inputStream =
        IcebergTestUtil.class.getClassLoader().getResourceAsStream(snapshotFile);
    String data =
        IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8.name());
    return SnapshotsUtil.parseSnapshots(null, data);
  }
}
