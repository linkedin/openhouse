package com.linkedin.openhouse.internal.catalog.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.Value;
import org.apache.iceberg.TableMetadata;

/** Metadata and ordered ref transitions produced by applying one complete update list. */
@Value
public class MetadataUpdateResult {
  TableMetadata metadata;
  List<SnapshotRefChange> refChanges;

  @Builder
  public MetadataUpdateResult(TableMetadata metadata, List<SnapshotRefChange> refChanges) {
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.refChanges = Collections.unmodifiableList(new ArrayList<>(refChanges));
  }
}
