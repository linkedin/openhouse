package com.linkedin.openhouse.internal.catalog.view;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;

/** {@link ViewMetadataCodec} backed by Iceberg's {@code ViewMetadataParser}. */
public class IcebergViewMetadataCodec implements ViewMetadataCodec {

  @Override
  public ViewMetadata read(InputFile inputFile) {
    return ViewMetadataParser.read(inputFile);
  }

  /** Create, not overwrite: a colliding file name must fail loudly rather than replace. */
  @Override
  public void write(ViewMetadata metadata, OutputFile outputFile) {
    ViewMetadataParser.write(metadata, outputFile);
  }
}
