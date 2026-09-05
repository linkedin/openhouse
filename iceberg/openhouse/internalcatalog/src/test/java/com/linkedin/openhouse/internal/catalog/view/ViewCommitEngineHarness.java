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
 * <p>There is deliberately no {@code Storage} and no {@code StorageSelector} here. The engine has
 * no seam left to select storage or allocate a root, and it must not recover the storage type by
 * asking which storage a {@code FileIO} belongs to, so a harness that still offered either could
 * hide a regression that reintroduced one.
 *
 * <p>Two storage types are wired, each to its own real local {@code HadoopFileIO}, so a test can
 * supply a non-default type and prove the engine used the type it was handed rather than defaulting
 * to LOCAL. {@code StorageType} is real rather than mocked, so {@code fromString} performs exactly
 * the conversion production performs.
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

  /** A new instance over the same rows and storage, so a load cannot come from process state. */
  public ViewCommitEngine newEngineInstance() {
    return new ViewCommitEngineImpl(
        houseTableRepository, fileIOManager, recordingCodec, new StorageType());
  }

  /** The single ordered log of codec and House Table interactions, in the order they happened. */
  public List<String> events() {
    synchronized (events) {
      return new ArrayList<>(events);
    }
  }

  public void clearEvents() {
    events.clear();
  }

  /** How many metadata files the codec was asked to write, candidates that lost included. */
  public int codecWrites() {
    return (int)
        events().stream()
            .filter(event -> event.startsWith(RecordingViewMetadataCodec.WRITE))
            .count();
  }

  /** Reads a metadata file back through the real Iceberg parser. */
  public ViewMetadata readMetadata(String metadataLocation) {
    return ViewMetadataParser.read(fileIO.newInputFile(metadataLocation));
  }

  /** Every metadata file that physically exists under the storage root, candidates included. */
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
