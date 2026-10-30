package com.linkedin.openhouse.javaclient;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.javaclient.precommit.PreCommitHook;
import com.linkedin.openhouse.javaclient.precommit.PreCommitHookContext;
import java.lang.reflect.Field;
import java.util.Collections;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Registering a pre-commit hook on the catalog instead of on each {@link OpenHouseTableOperations}.
 *
 * <p>The catalogs here are never initialized, so the operations they build have no API to send to.
 * That is what the tests run on: the hook is called before anything is sent, so it records the
 * commit and the send fails afterwards.
 */
public class OpenHouseCatalogPreCommitTest {

  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");

  /** Records whether it was called, and lets the commit through. */
  private static final class RecordingHook implements PreCommitHook {
    private boolean called;

    @Override
    public void evaluate(PreCommitHookContext context) {
      this.called = true;
    }
  }

  /**
   * A catalog with just enough state to build operations and run a commit through them. {@code
   * initialize} would build real service clients, so the one field the commit path reads before the
   * hook is set directly, as {@code OpenHouseCatalogViewsTest} does.
   */
  private static OpenHouseCatalog catalog() {
    OpenHouseCatalog catalog = new OpenHouseCatalog();
    try {
      Field cluster = OpenHouseCatalog.class.getDeclaredField("cluster");
      cluster.setAccessible(true);
      cluster.set(catalog, "local");
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to set cluster", e);
    }
    return catalog;
  }

  /** Runs a commit far enough to call the hook. Sending it afterwards always fails. */
  private static void commit(OpenHouseTableOperations ops) {
    TableMetadata base = mock(TableMetadata.class);
    TableMetadata metadata = mock(TableMetadata.class);
    when(base.schema()).thenReturn(mock(Schema.class));
    when(metadata.schema()).thenReturn(mock(Schema.class));
    when(base.properties()).thenReturn(ImmutableMap.of());
    when(metadata.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(base.snapshots()).thenReturn(Collections.emptyList());
    when(metadata.snapshots()).thenReturn(Collections.emptyList());
    when(base.refs()).thenReturn(Collections.emptyMap());
    when(metadata.refs()).thenReturn(Collections.emptyMap());

    try {
      ops.doCommit(base, metadata);
      Assertions.fail("Expected sending the commit to fail: this catalog has no service");
    } catch (RuntimeException sendFailed) {
      // Expected, and it happens after the hook has had its say.
    }
  }

  /** Registration is fluent, so it chains onto a freshly built catalog. */
  @Test
  public void testWithPreCommitReturnsTheSameCatalog() {
    OpenHouseCatalog catalog = catalog();

    Assertions.assertSame(catalog, catalog.withPreCommit(PreCommitHook.NOOP));
  }

  /** The point of registering on the catalog: every table loaded through it gets the hook. */
  @Test
  public void testTheCatalogHookRunsForTablesLoadedThroughIt() {
    RecordingHook hook = new RecordingHook();
    OpenHouseCatalog catalog = catalog().withPreCommit(hook);

    commit(catalog.newTableOps(TABLE));

    Assertions.assertTrue(hook.called);
  }

  /** The hook is handed over when the operations are built, so earlier ones keep what they got. */
  @Test
  public void testTablesLoadedBeforeRegistrationAreUnaffected() {
    RecordingHook hook = new RecordingHook();
    OpenHouseCatalog catalog = catalog();
    OpenHouseTableOperations loadedFirst = catalog.newTableOps(TABLE);

    catalog.withPreCommit(hook);
    commit(loadedFirst);

    Assertions.assertFalse(hook.called);
  }

  /** Registering on one instance replaces what the catalog gave it, for that instance only. */
  @Test
  public void testAHookOnTheOperationsReplacesTheCatalogHook() {
    RecordingHook catalogHook = new RecordingHook();
    RecordingHook operationsHook = new RecordingHook();
    OpenHouseCatalog catalog = catalog().withPreCommit(catalogHook);

    commit(catalog.newTableOps(TABLE).withPreCommit(operationsHook));

    Assertions.assertTrue(operationsHook.called);
    Assertions.assertFalse(catalogHook.called);
  }

  /** Storing null would turn checking off without saying so. */
  @Test
  public void testARegisteredHookCannotBeNull() {
    OpenHouseCatalog catalog = catalog();

    Assertions.assertThrows(NullPointerException.class, () -> catalog.withPreCommit(null));
  }
}
