package com.linkedin.openhouse.internal.catalog.view;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.view.ViewMetadata;

/**
 * Injectable seam over the static {@code ViewMetadataParser}, so a test can assert that a failed
 * probe never reads a metadata file. Names Iceberg 1.5 types, so it is registered conditionally.
 */
public interface ViewMetadataCodec {

  ViewMetadata read(InputFile inputFile);

  void write(ViewMetadata metadata, OutputFile outputFile);
}
