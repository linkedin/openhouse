package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import java.time.Clock;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * Iceberg-1.5 implementation of {@link ViewCommitEngine}. Registered only from {@link
 * ViewCommitEngineConfiguration}, so an Iceberg-1.2 runtime never introspects it. Every commit
 * captures the base, builds the metadata, writes the immutable file, then performs exactly one
 * House Table compare-and-swap on the captured token; the swap is the sole arbiter of a race and is
 * never retried, since a second attempt could double-apply.
 *
 * <p>The pointer row is built here rather than through {@code HouseTableMapper}, because that
 * mapper recovers the storage type by asking {@link FileIOManager#getStorage} which storage a
 * {@code FileIO} belongs to. That reverse lookup is lossy — HDFS and LOCAL can be configured with
 * equal {@code HadoopFileIO} instances — so it would silently replace the storage fact the caller
 * supplied, or the one the published row already carries, with whichever storage matches first.
 */
@AllArgsConstructor
@Slf4j
public class ViewCommitEngineImpl implements ViewCommitEngine {

  private final HouseTableRepository houseTableRepository;

  private final FileIOManager fileIOManager;

  private final ViewMetadataCodec viewMetadataCodec;

  private final StorageType storageType;

  @Override
  public ViewCommitResult commit(ViewCommitIntent intent) {
    throw new UnsupportedOperationException("commit is not implemented yet");
  }

  @Override
  public LoadedView loadView(String databaseId, String viewId) {
    throw new UnsupportedOperationException("loadView is not implemented yet");
  }

  @Override
  public Page<ViewPointer> listViews(String databaseId, Pageable pageable) {
    throw new UnsupportedOperationException("listViews is not implemented yet");
  }

  @Override
  public boolean dropView(String databaseId, String viewId) {
    throw new UnsupportedOperationException("dropView is not implemented yet");
  }

  @Override
  public void renameView(String databaseId, String fromViewId, String toViewId) {
    throw new UnsupportedOperationException(
        "Renaming a view is not supported: " + databaseId + "." + fromViewId);
  }

  /** Overridable so a test can pin it. */
  protected long nowMillis() {
    return Instant.now(Clock.systemUTC()).toEpochMilli();
  }
}
