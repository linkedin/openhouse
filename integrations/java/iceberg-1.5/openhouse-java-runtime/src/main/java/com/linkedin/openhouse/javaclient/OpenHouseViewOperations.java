package com.linkedin.openhouse.javaclient;

import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;

/**
 * OpenHouse {@link org.apache.iceberg.view.ViewOperations}, active only when {@code
 * spark.sql.catalog.<name>.iceberg-views-enabled=true}. Extends {@link BaseViewOperations} for the
 * {@code base == current} commit protocol and {@code current()}/{@code refresh()} caching; the
 * catalog-specific {@link #doCommit} / {@link #doRefresh} are left to implement, similar to how
 * tables implement them in {@link OpenHouseTableOperations}.
 *
 * <p>Backend today is an in-memory (placeholder) store ({@code inMemoryViewStore}): {@link
 * #doCommit} stores {@link ViewMetadata} and reads are served by the {@link #current()}/{@link
 * #refresh()} overrides -- no server call, no {@code metadata.json} written. A Views-service-backed
 * implementation is expected to be merged in future: fill {@link #doCommit}/{@link #doRefresh} with
 * a {@code ViewApi} create/update + get (the service assigns the real location, replacing the
 * placeholder set in {@code OpenHouseCatalog.buildView}), after which the {@code current()}/{@code
 * refresh()} overrides below are removed.
 */
@Builder
@Slf4j
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class OpenHouseViewOperations extends BaseViewOperations {

  @Getter(AccessLevel.PROTECTED)
  private TableIdentifier viewIdentifier;

  @Getter(AccessLevel.PROTECTED)
  private FileIO fileIO;

  @Getter(AccessLevel.PROTECTED)
  private Map<TableIdentifier, ViewMetadata> inMemoryViewStore;

  /**
   * In-memory-only override: serve reads straight from the in-memory store, bypassing the
   * file-backed refresh machinery (there is no {@code metadata.json} until the server writes one).
   *
   * <p>TODO(views): delete this override once {@link #doRefresh} loads server-written metadata via
   * {@code refreshFromMetadataLocation(...)}; the inherited {@link BaseViewOperations#current()}
   * then works unchanged.
   */
  @Override
  public ViewMetadata current() {
    return inMemoryViewStore.get(viewIdentifier);
  }

  /**
   * In-memory-only override; see {@link #current()}. TODO(views): delete once {@link #doRefresh} is
   * service-backed.
   */
  @Override
  public ViewMetadata refresh() {
    return inMemoryViewStore.get(viewIdentifier);
  }

  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata metadata) {
    // TODO(views): POST to the ViewApi (create/update); the service writes metadata.json and
    // assigns the real location. Until then, persist in-memory only.
    log.warn(
        "OpenHouse in-memory view commit for {} (not persisted to any service)", viewIdentifier);
    inMemoryViewStore.put(viewIdentifier, metadata);
  }

  @Override
  protected void doRefresh() {
    // TODO(views): GET the view from ViewApi, then refreshFromMetadataLocation(serverLocation) to
    // load the server-written metadata.json. No-op today: the in-memory store serves reads via the
    // current()/refresh() overrides above.
  }

  @Override
  protected String viewName() {
    return viewIdentifier.toString();
  }

  @Override
  protected FileIO io() {
    return fileIO;
  }
}
