package com.linkedin.openhouse.internal.catalog.view;

import java.util.List;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.view.ViewMetadata;

/** Logs each parse and write so "write then publish" is assertable as a sequence. */
public class RecordingViewMetadataCodec implements ViewMetadataCodec {

  public static final String READ = "codec.read";
  public static final String WRITE = "codec.write";

  private final ViewMetadataCodec delegate;

  private final List<String> events;

  public RecordingViewMetadataCodec(ViewMetadataCodec delegate, List<String> events) {
    this.delegate = delegate;
    this.events = events;
  }

  @Override
  public ViewMetadata read(InputFile inputFile) {
    events.add(READ + "(" + (inputFile == null ? "null" : inputFile.location()) + ")");
    return delegate.read(inputFile);
  }

  @Override
  public void write(ViewMetadata metadata, OutputFile outputFile) {
    events.add(WRITE + "(" + (outputFile == null ? "null" : outputFile.location()) + ")");
    delegate.write(metadata, outputFile);
  }
}
