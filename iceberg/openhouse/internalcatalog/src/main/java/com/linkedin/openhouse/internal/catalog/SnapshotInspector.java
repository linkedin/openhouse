package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * A inspector class providing functionalities that inspect components of {@link Snapshot} provided
 * by clients and decide if OpenHouse need to take additional steps to incorporate it or decide
 * whether to incorporate at all.
 *
 * <p>Instance of this class will be injected into {@link OpenHouseInternalTableOperations} in
 * runtime.
 */
@Component
public class SnapshotInspector {
  @Autowired private FsStorageProvider fsStorageProvider;

  @Autowired private Consumer<Supplier<Path>> fileSecurer;
  /**
   * TODO: ADD Validation for snapshot: Sequence-number based, schema-id based, see iceberg spec for
   * details. Throwing exceptions when failures occurred.
   *
   * @param providedSnapshot deserialized {@link Snapshot} object that clients provided.
   * @throws InvalidIcebergSnapshotException Exception thrown from the process validating the
   *     snapshot provided by client.
   */
  void validateSnapshot(Snapshot providedSnapshot) throws InvalidIcebergSnapshotException {
    // TODO: Fill this method.
  }

  void validateSnapshotsUpdate(
      TableMetadata metadata, List<Snapshot> addedSnapshots, List<Snapshot> deletedSnapshots) {
    if (metadata.currentSnapshot() == null) {
      // no need to verify attempt to delete current snapshot if it doesn't exist
      // deletedSnapshots is necessarily empty when original snapshots list is empty
      return;
    }
    if (!addedSnapshots.isEmpty()) {
      // latest snapshot can be deleted if new snapshots are added.
      return;
    }
    long latestSnapshotId = metadata.currentSnapshot().snapshotId();
    if (!deletedSnapshots.isEmpty()
        && deletedSnapshots.get(deletedSnapshots.size() - 1).snapshotId() == latestSnapshotId) {
      throw new InvalidIcebergSnapshotException(
          String.format("Cannot delete the latest snapshot %s", latestSnapshotId));
    }
  }

  /**
   * A sister method to {@link #validateSnapshot(Snapshot)} that change the file-level permission to
   * be OpenHouse exclusive to avoid unexpected changes from unauthorized parties. Throwing
   * exceptions when failures occurred.
   *
   * @param providedSnapshot deserialized {@link Snapshot} object that clients provided.
   * @param fileIO {@link FileIO} object
   * @throws UncheckedIOException Exception thrown from the process securing the files associated
   *     with {@param providedSnapshot}.
   */
  @VisibleForTesting
  void secureSnapshot(Snapshot providedSnapshot, FileIO fileIO) throws UncheckedIOException {
    secureDataFile(providedSnapshot.addedDataFiles(fileIO));
    secureDeleteFile(providedSnapshot.addedDeleteFiles(fileIO));
    secureManifestFile(providedSnapshot.allManifests(fileIO));
  }

  private void secureDataFile(Iterable<DataFile> dataFiles) {
    StreamSupport.stream(dataFiles.spliterator(), false)
        .map(x -> (Supplier<Path>) (() -> new Path(x.path().toString())))
        .forEach(fileSecurer);
  }

  private void secureDeleteFile(Iterable<DeleteFile> deleteFiles) {
    StreamSupport.stream(deleteFiles.spliterator(), false)
        .map(x -> (Supplier<Path>) (() -> new Path(x.path().toString())))
        .forEach(fileSecurer);
  }

  private void secureManifestFile(List<ManifestFile> manifestFiles) throws UncheckedIOException {
    manifestFiles.stream()
        .map(x -> (Supplier<Path>) (() -> new Path(x.path())))
        .forEach(fileSecurer);
  }
}
