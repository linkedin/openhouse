package com.linkedin.openhouse.internal.catalog;

import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.io.FileIO;
import org.springframework.data.util.Pair;

/** Utility class to handle {@link Snapshot}'s parsing and manipulation. */
public final class SnapshotsUtil {
  private SnapshotsUtil() {}

  /** Serialize list of strings as json */
  public static String serializeList(List<String> list) {
    return new GsonBuilder().create().toJson(list);
  }

  /** Serialize list of snapshots as json */
  public static String serializedSnapshots(List<Snapshot> snapshots) {
    return serializeList(
        snapshots.stream().map(SnapshotParser::toJson).collect(Collectors.toList()));
  }

  /** Parse serialized list of serialized snapshots */
  public static List<Snapshot> parseSnapshots(FileIO io, String data) {
    List<String> jsonSnapshots =
        new GsonBuilder().create().fromJson(data, new TypeToken<ArrayList<String>>() {}.getType());
    return parseSnapshots(io, jsonSnapshots);
  }

  /** Parse list of serialized snapshots */
  public static List<Snapshot> parseSnapshots(FileIO io, List<String> jsonSnapshots) {
    return parse(io, jsonSnapshots);
  }

  /** Parse list of serialized snapshots */
  public static List<Snapshot> parse(FileIO io, List<String> jsonSnapshots) {
    return jsonSnapshots.stream().map(s -> SnapshotParser.fromJson(s)).collect(Collectors.toList());
  }

  /**
   * Finds symmetric difference between two collections and splits the difference.
   *
   * @param first
   * @param second
   * @return (first - second, second - first)
   */
  public static Pair<List<Snapshot>, List<Snapshot>> symmetricDifferenceSplit(
      List<Snapshot> first, List<Snapshot> second) {
    Set<Long> firstSnapshotIds =
        first.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    Set<Long> secondSnapshotIds =
        second.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    return Pair.of(
        first.stream()
            .filter(s -> !secondSnapshotIds.contains(s.snapshotId()))
            .collect(Collectors.toList()),
        second.stream()
            .filter(s -> !firstSnapshotIds.contains(s.snapshotId()))
            .collect(Collectors.toList()));
  }

  /** Serialize map of strings as json */
  public static String serializeMap(Map<String, String> map) {
    return new GsonBuilder().create().toJson(map);
  }

  /** Parse serialized map of serialized snapshotRefs */
  public static Map<String, SnapshotRef> parseSnapshotRefs(String data) {
    Map<String, String> jsonSnapshotRefs =
        new GsonBuilder()
            .create()
            .fromJson(data, new TypeToken<Map<String, String>>() {}.getType());
    return parseSnapshotRefs(jsonSnapshotRefs);
  }

  /** Parse map of serialized snapshotRef */
  public static Map<String, SnapshotRef> parseSnapshotRefs(Map<String, String> jsonSnapshotRefs) {
    return jsonSnapshotRefs.entrySet().stream()
        .collect(
            Collectors.toMap(Map.Entry::getKey, e -> SnapshotRefParser.fromJson(e.getValue())));
  }
}
