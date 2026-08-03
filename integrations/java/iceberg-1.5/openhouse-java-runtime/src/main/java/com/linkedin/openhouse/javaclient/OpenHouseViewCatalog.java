package com.linkedin.openhouse.javaclient;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;

/**
 * View-capable {@link OpenHouseCatalog}. Table operations are inherited unchanged; view operations
 * are added by implementing Iceberg's {@link ViewCatalog}. This makes a single catalog object serve
 * both tables and views, which is what Iceberg's Spark {@code SparkCatalog} requires to wire {@code
 * asViewCatalog} (it only enables views when the configured {@code catalog-impl} is {@code
 * instanceof ViewCatalog}).
 *
 * <p>Because it already {@code extends OpenHouseCatalog} (for tables), Java single inheritance
 * means it cannot also {@code extends BaseMetastoreViewCatalog}; instead it {@code implements
 * ViewCatalog} and delegates the view calls to a composed view catalog. Today the composed backend
 * is a mock {@link InMemoryCatalog} (ephemeral, no persistence) so the create/read plumbing can be
 * exercised before the OpenHouse Views service and its generated client exist. The real backend
 * will talk to the OpenHouse Views service the same way {@link OpenHouseCatalog} talks to the
 * Tables service — a {@code newViewOps() -> OpenHouseViewOperations -> ViewApi} path mirroring
 * {@code newTableOps() -> OpenHouseTableOperations -> TableApi} — and swapping it in touches only
 * the composed field.
 *
 * <p>This class lives only in the iceberg-1.5 source set, so Iceberg's view classes (1.5+ only) are
 * never referenced from the shared iceberg-1.2 sources; the Spark 3.1 / Iceberg 1.2 runtime stays
 * table-only for now.
 *
 * <p><b>Wiring:</b> this class is <i>not</i> picked up automatically. Spark instantiates whatever
 * {@code spark.sql.catalog.<name>.catalog-impl} names — today the Spark wrapper {@code
 * com.linkedin.openhouse.spark.OpenHouseCatalog}, which is not a {@code ViewCatalog} — and
 * Iceberg's {@code SparkCatalog} enables views only when that configured instance is {@code
 * instanceof ViewCatalog}. So {@code OpenHouseViewCatalog} is reached only by either (a) repointing
 * {@code catalog-impl} at a thin Spark wrapper over this class (the config change doubles as a
 * prototype gate), or (b) making the configured {@code OpenHouseCatalog} itself implement {@code
 * ViewCatalog} (Spark-3.5 / iceberg-1.5 build only). For prototyping, for noww, (b) has been
 * adopted to minimize any interference with production code paths.
 */
@Slf4j
public class OpenHouseViewCatalog extends OpenHouseCatalog implements ViewCatalog {

  // MOCK view backend: ephemeral and no persistence. Stand-in until the OpenHouse Views
  // service and its generated client exist.
  // Evolution: swap this InMemoryCatalog for an OpenHouse Views-service-backed view catalog that
  // mirrors how OpenHouseCatalog talks to the Tables service — i.e. a BaseMetastoreViewCatalog
  // whose
  // newViewOps() returns an OpenHouseViewOperations calling a generated ViewApi, exactly as
  // OpenHouseCatalog.newTableOps() returns OpenHouseTableOperations calling TableApi. Only this
  // field's type + init change; the ViewCatalog delegation below stays identical.
  // TODO(views-prototype): swap mock -> OH Views service client once the server API + ViewApi land.
  private final InMemoryCatalog mockViewCatalog = new InMemoryCatalog();

  /**
   * Explicit override to satisfy {@link ViewCatalog#name()} unambiguously. {@code Catalog#name()}
   * is a {@code default} method whereas {@code ViewCatalog#name()} is abstract; the inherited
   * {@link OpenHouseCatalog#name()} already satisfies both for {@code javac}, but declaring it here
   * avoids a false "must implement name()" flag under the shared iceberg-1.2/1.5 source setup.
   */
  @Override
  public String name() {
    return super.name();
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    mockViewCatalog.initialize(name, properties);
    log.warn(
        "OpenHouseViewCatalog initialized with an in-memory MOCK view backend; "
            + "views are not persisted and are visible only within this catalog instance.");
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    return mockViewCatalog.namespaceExists(namespace)
        ? mockViewCatalog.listViews(namespace)
        : Collections.emptyList();
  }

  @Override
  public View loadView(TableIdentifier identifier) {
    return mockViewCatalog.loadView(identifier);
  }

  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    // The mock backend rejects creation into a missing namespace; auto-create so a bare
    // CREATE VIEW works without a prior CREATE NAMESPACE (real backend will own this policy).
    Namespace namespace = identifier.namespace();
    if (!mockViewCatalog.namespaceExists(namespace)) {
      mockViewCatalog.createNamespace(namespace);
    }
    return mockViewCatalog.buildView(identifier);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    return mockViewCatalog.dropView(identifier);
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    Namespace namespace = to.namespace();
    if (!mockViewCatalog.namespaceExists(namespace)) {
      mockViewCatalog.createNamespace(namespace);
    }
    mockViewCatalog.renameView(from, to);
  }
}
