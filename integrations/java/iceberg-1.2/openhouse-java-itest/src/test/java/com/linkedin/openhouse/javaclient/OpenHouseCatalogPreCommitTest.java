package com.linkedin.openhouse.javaclient;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.javaclient.precommit.PreCommitHook;
import com.linkedin.openhouse.javaclient.precommit.PreCommitHookBlockedException;
import com.linkedin.openhouse.javaclient.precommit.PreCommitHookContext;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
import java.lang.reflect.Field;
import java.util.Collections;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
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

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private static final String LOCATION = "file:///tmp/openhouse-precommit-test";

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
    set(catalog, "cluster", "local");
    return catalog;
  }

  private static void set(OpenHouseCatalog catalog, String name, Object value) {
    try {
      Field field = OpenHouseCatalog.class.getDeclaredField(name);
      field.setAccessible(true);
      field.set(catalog, value);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to set " + name, e);
    }
  }

  /** Blocks every commit, under a name an error message can be checked for. */
  private static final class BlockingHook implements PreCommitHook {
    private final String name;

    private BlockingHook(String name) {
      this.name = name;
    }

    @Override
    public void evaluate(PreCommitHookContext context) throws PreCommitHookBlockedException {
      throw PreCommitHookBlockedException.validation("blocked", "nope");
    }

    @Override
    public String getName() {
      return name;
    }
  }

  /**
   * Runs a commit far enough to call the hooks.
   *
   * @return what stopped it: a block from a hook, or the failure to send
   */
  private static RuntimeException commit(OpenHouseTableOperations ops) {
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
      return Assertions.fail("Expected sending the commit to fail: this catalog has no service");
    } catch (RuntimeException stopped) {
      return stopped;
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

  /** Registering on one instance adds to the catalog's hook rather than taking its place. */
  @Test
  public void testAHookOnTheOperationsRunsAlongsideTheCatalogHook() {
    RecordingHook catalogHook = new RecordingHook();
    RecordingHook operationsHook = new RecordingHook();
    OpenHouseCatalog catalog = catalog().withPreCommit(catalogHook);

    commit(catalog.newTableOps(TABLE).withPreCommit(operationsHook));

    Assertions.assertTrue(catalogHook.called);
    Assertions.assertTrue(operationsHook.called);
  }

  /**
   * A hook registered on one table cannot switch off a catalog-wide one. The catalog's hook is
   * registered first, so it blocks before the table's hook runs.
   */
  @Test
  public void testATableHookCannotSwitchOffTheCatalogHook() {
    RecordingHook operationsHook = new RecordingHook();
    OpenHouseCatalog catalog = catalog().withPreCommit(new BlockingHook("catalog-hook"));

    RuntimeException stopped = commit(catalog.newTableOps(TABLE).withPreCommit(operationsHook));

    Assertions.assertTrue(stopped instanceof ValidationException);
    Assertions.assertTrue(stopped.getMessage().contains("catalog-hook"));
    Assertions.assertFalse(operationsHook.called);
  }

  /** An error names the hook that blocked, not whichever ran first. */
  @Test
  public void testTheBlockingHookIsTheOneReported() {
    OpenHouseCatalog catalog = catalog().withPreCommit(new RecordingHook());

    RuntimeException stopped =
        commit(catalog.newTableOps(TABLE).withPreCommit(new BlockingHook("second-hook")));

    Assertions.assertTrue(stopped.getMessage().contains("second-hook"));
  }

  /** Storing null would turn checking off without saying so. */
  @Test
  public void testARegisteredHookCannotBeNull() {
    OpenHouseCatalog catalog = catalog();

    Assertions.assertThrows(NullPointerException.class, () -> catalog.withPreCommit(null));
  }

  /** A catalog whose table lookups come back empty, so a create runs on to the commit. */
  private static OpenHouseCatalog catalogWithNoTables() {
    OpenHouseCatalog catalog = catalog();
    TableApi tableApi = mock(TableApi.class);
    when(tableApi.getTableV1(anyString(), anyString())).thenReturn(Mono.empty());
    set(catalog, "tableApi", tableApi);
    set(catalog, "properties", ImmutableMap.of());
    return catalog;
  }

  /** Blocks every commit the way a hook whose service is down would. */
  private static final class UnreachableHook implements PreCommitHook {
    @Override
    public void evaluate(PreCommitHookContext context) throws PreCommitHookBlockedException {
      throw PreCommitHookBlockedException.operational("service-down", "cannot reach checker", null);
    }

    @Override
    public String getName() {
      return "unreachable-hook";
    }
  }

  /**
   * Iceberg's table builder catches {@code CommitFailedException} from a create and rethrows {@code
   * AlreadyExistsException("Table was created concurrently")} without the cause. A hook that could
   * not run must not come out that way: the table does not exist, and the reason is the only thing
   * the caller can act on.
   */
  @Test
  public void testACreateBlockedByAnUnreachableHookSaysWhyRatherThanAlreadyExists() {
    OpenHouseCatalog catalog = catalogWithNoTables().withPreCommit(new UnreachableHook());

    ServiceUnavailableException thrown =
        Assertions.assertThrows(
            ServiceUnavailableException.class,
            () -> catalog.buildTable(TABLE, SCHEMA).withLocation(LOCATION).create());

    Assertions.assertTrue(thrown.getMessage().contains("unreachable-hook"));
    Assertions.assertTrue(thrown.getMessage().contains("service-down"));
    Assertions.assertTrue(thrown.getCause() instanceof PreCommitHookBlockedException);
  }

  /**
   * The create path only mistranslates {@code CommitFailedException}, so a hook that ran and
   * blocked needs no special case and keeps reporting the same way it does on a write.
   */
  @Test
  public void testACreateBlockedByAHookThatRanReportsTheBlock() {
    OpenHouseCatalog catalog = catalogWithNoTables().withPreCommit(new BlockingHook("create-hook"));

    ValidationException thrown =
        Assertions.assertThrows(
            ValidationException.class,
            () -> catalog.buildTable(TABLE, SCHEMA).withLocation(LOCATION).create());

    Assertions.assertTrue(thrown.getMessage().contains("create-hook"));
  }
}
