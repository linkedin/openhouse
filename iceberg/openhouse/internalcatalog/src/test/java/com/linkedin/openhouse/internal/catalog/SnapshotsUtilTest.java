package com.linkedin.openhouse.internal.catalog;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.util.Pair;

public class SnapshotsUtilTest {
  private static List<Snapshot> testSnapshots;

  @BeforeAll
  static void setup() throws IOException {
    testSnapshots = IcebergTestUtil.getSnapshots();
  }

  @Test
  public void testSnapshotsSerDe() {
    List<String> listOfSerializedSnapshots =
        testSnapshots.stream().map(SnapshotParser::toJson).collect(Collectors.toList());
    String serializedSnapshots = SnapshotsUtil.serializeList(listOfSerializedSnapshots);
    List<Snapshot> snapshots = SnapshotsUtil.parseSnapshots(null, serializedSnapshots);
    Assertions.assertEquals(testSnapshots, snapshots);
  }

  @Test
  public void testSymmetricDifference() {
    Assertions.assertEquals(
        Pair.of(Collections.emptyList(), Collections.emptyList()),
        SnapshotsUtil.symmetricDifferenceSplit(testSnapshots, testSnapshots));
    Assertions.assertEquals(
        Pair.of(Collections.emptyList(), testSnapshots),
        SnapshotsUtil.symmetricDifferenceSplit(Collections.emptyList(), testSnapshots));
    Assertions.assertEquals(
        Pair.of(testSnapshots, Collections.emptyList()),
        SnapshotsUtil.symmetricDifferenceSplit(testSnapshots, Collections.emptyList()));
    Assertions.assertEquals(
        Pair.of(testSnapshots.subList(0, 1), testSnapshots.subList(3, 4)),
        SnapshotsUtil.symmetricDifferenceSplit(
            testSnapshots.subList(0, 3), testSnapshots.subList(1, 4)));
  }
}
