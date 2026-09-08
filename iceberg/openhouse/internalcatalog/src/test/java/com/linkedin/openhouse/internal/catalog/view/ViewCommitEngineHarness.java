package com.linkedin.openhouse.internal.catalog.view;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;

/**
 * Real files, real Iceberg parsing, and a faithful House Table double, so a commit is asserted end
 * to end rather than through stubs.
 *
 * <p>No {@code Storage} or {@code StorageSelector} is wired, so a harness cannot hide a regression
 * that reintroduced one. Two storage types are wired to their own real {@code HadoopFileIO}, so a
 * test can prove the engine used the type it was handed rather than defaulting to LOCAL.
 */
@Getter
public class ViewCommitEngineHarness {

  /** A second, deliberately non-default storage type, wired to its own FileIO instance. */
  public static final String ALTERNATE_STORAGE_TYPE = "hdfs";

  private final Path root;
  private final FileIO fileIO;
  private final FileIO alternateFileIO;
  private final FileIOManager fileIOManager;
  private final ViewMetadataCodec codec;
  private final ViewMetadataCodec recordingCodec;
  private final List<String> events;
  private final InMemoryViewHouseTableRepository houseTableRepository;
  private final ViewCommitEngine viewCommitEngine;

  public ViewCommitEngineHarness(Path root) {
    this.root = root;
    this.fileIO = new HadoopFileIO(new Configuration());
    this.alternateFileIO = new HadoopFileIO(new Configuration());
    this.fileIOManager = mock(FileIOManager.class);
    this.codec = spy(new IcebergViewMetadataCodec());
    this.events = Collections.synchronizedList(new ArrayList<>());
    this.recordingCodec = new RecordingViewMetadataCodec(codec, events);
    this.houseTableRepository = new InMemoryViewHouseTableRepository(events);

    when(fileIOManager.getFileIO(eq(StorageType.LOCAL))).thenReturn(fileIO);
    when(fileIOManager.getFileIO(eq(StorageType.HDFS))).thenReturn(alternateFileIO);

    this.viewCommitEngine = newEngineInstance();
  }

  /** A new instance over the same rows, so a load cannot come from process state. */
  public ViewCommitEngine newEngineInstance() {
    return new ViewCommitEngineImpl(
        houseTableRepository, fileIOManager, recordingCodec, new StorageType());
  }

  /** The single ordered log of codec and House Table interactions. */
  public List<String> events() {
    synchronized (events) {
      return new ArrayList<>(events);
    }
  }

  public void clearEvents() {
    events.clear();
  }

  /** Metadata files the codec was asked to write, losing candidates included. */
  public int codecWrites() {
    return (int)
        events().stream()
            .filter(event -> event.startsWith(RecordingViewMetadataCodec.WRITE))
            .count();
  }

  public ViewMetadata readMetadata(String metadataLocation) {
    return ViewMetadataParser.read(fileIO.newInputFile(metadataLocation));
  }

  /** Every metadata file physically under the storage root, candidates included. */
  public List<Path> metadataFiles() {
    try (Stream<Path> paths = Files.walk(root)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".metadata.json"))
          .sorted()
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
