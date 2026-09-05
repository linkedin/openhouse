package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

/**
 * Faithful in-memory stand-in for the House Table client, reproducing both production mechanisms
 * the engine depends on: the three typed finders (table = TABLE or legacy null, view = VIEW only,
 * neutral = any occupant) and the converter's legacy-null-to-TABLE hydration. A double that skipped
 * either one would let a view leak into a table read, or make a legitimate legacy row look like a
 * contract violation.
 *
 * <p>{@link #peek(String, String)} is deliberately raw, so a test can assert what is actually
 * stored rather than what a read would report.
 */
public class InMemoryViewHouseTableRepository implements HouseTableRepository {

  public static final String FIND_ENTITY = "findEntityById";
  public static final String FIND_VIEW = "findViewById";
  public static final String LIST_VIEWS = "findAllViewsByDatabaseId";
  public static final String SAVE_VIEW = "saveView";
  public static final String DELETE_VIEW = "deleteViewById";

  private static final String ENTITY_TYPE_VIEW = "VIEW";
  private static final String ENTITY_TYPE_TABLE = "TABLE";

  private final Map<String, HouseTable> rows = new ConcurrentHashMap<>();

  private final List<String> events;

  private final AtomicInteger saveViewCalls = new AtomicInteger();

  private final AtomicInteger findEntityByIdCalls = new AtomicInteger();

  private final AtomicInteger findViewByIdCalls = new AtomicInteger();

  private final AtomicInteger deleteViewByIdCalls = new AtomicInteger();

  private volatile Runnable beforeCas = () -> {};

  private final AtomicReference<Runnable> beforeNextCas = new AtomicReference<>();

  private final AtomicReference<RuntimeException> nextSaveViewFailure = new AtomicReference<>();

  private final AtomicReference<RuntimeException> nextDeleteViewFailure = new AtomicReference<>();

  private final AtomicReference<RuntimeException> nextFindEntityFailure = new AtomicReference<>();

  private final Object casLock = new Object();

  public InMemoryViewHouseTableRepository() {
    this(Collections.synchronizedList(new ArrayList<>()));
  }

  /** Shared with the codec so write-before-publish is one ordered sequence, not two facts. */
  public InMemoryViewHouseTableRepository(List<String> events) {
    this.events = events;
  }

  public void setBeforeCas(Runnable beforeCas) {
    this.beforeCas = beforeCas;
  }

  /** Runs once inside the next swap, after the caller has written its candidate file. */
  public void runOnceBeforeNextCas(Runnable action) {
    beforeNextCas.set(action);
  }

  /** Makes the next publish fail without touching stored state, to test error translation. */
  public void failNextSaveViewWith(RuntimeException failure) {
    nextSaveViewFailure.set(failure);
  }

  public void failNextDeleteViewWith(RuntimeException failure) {
    nextDeleteViewFailure.set(failure);
  }

  /** Makes the next neutral occupancy read fail, so a transport error cannot read as "free". */
  public void failNextFindEntityWith(RuntimeException failure) {
    nextFindEntityFailure.set(failure);
  }

  /** Ordered log of every repository call this fake received. */
  public List<String> getEvents() {
    synchronized (events) {
      return new ArrayList<>(events);
    }
  }

  public void clearEvents() {
    events.clear();
  }

  public int getSaveViewCalls() {
    return saveViewCalls.get();
  }

  public int getFindEntityByIdCalls() {
    return findEntityByIdCalls.get();
  }

  public int getFindViewByIdCalls() {
    return findViewByIdCalls.get();
  }

  public int getDeleteViewByIdCalls() {
    return deleteViewByIdCalls.get();
  }

  /** Seeds a row without going through the swap, for arranging a starting state. */
  public void seed(HouseTable row) {
    rows.put(id(row.getDatabaseId(), row.getTableId()), row);
  }

  /** Raw stored state, unhydrated, so a fixture assertion sees the legacy null it seeded. */
  public Optional<HouseTable> peek(String databaseId, String tableId) {
    return Optional.ofNullable(rows.get(id(databaseId, tableId)));
  }

  private static String id(String databaseId, String tableId) {
    return databaseId + "." + tableId;
  }

  /** Mirrors {@code EntityTypeConverter}: a stored null is read back as canonical TABLE. */
  private static HouseTable hydrate(HouseTable row) {
    return row.getEntityType() == null ? row.toBuilder().entityType(ENTITY_TYPE_TABLE).build() : row;
  }

  private static boolean isTableOrLegacy(HouseTable row) {
    return row.getEntityType() == null || ENTITY_TYPE_TABLE.equalsIgnoreCase(row.getEntityType());
  }

  private static boolean isView(HouseTable row) {
    return ENTITY_TYPE_VIEW.equalsIgnoreCase(row.getEntityType());
  }

  @Override
  public Optional<HouseTable> findEntityById(HouseTablePrimaryKey key) {
    findEntityByIdCalls.incrementAndGet();
    events.add(FIND_ENTITY + "(" + id(key.getDatabaseId(), key.getTableId()) + ")");
    RuntimeException failure = nextFindEntityFailure.getAndSet(null);
    if (failure != null) {
      throw failure;
    }
    return Optional.ofNullable(rows.get(id(key.getDatabaseId(), key.getTableId())))
        .map(InMemoryViewHouseTableRepository::hydrate);
  }

  @Override
  public Optional<HouseTable> findViewById(HouseTablePrimaryKey key) {
    findViewByIdCalls.incrementAndGet();
    events.add(FIND_VIEW + "(" + id(key.getDatabaseId(), key.getTableId()) + ")");
    return Optional.ofNullable(rows.get(id(key.getDatabaseId(), key.getTableId())))
        .filter(InMemoryViewHouseTableRepository::isView);
  }

  @Override
  public Page<HouseTable> findAllViewsByDatabaseId(String databaseId, Pageable pageable) {
    events.add(LIST_VIEWS + "(" + databaseId + ")");
    // Filtered before the page is cut, exactly as the server's VIEW predicate is applied in SQL.
    List<HouseTable> views =
        rows.values().stream()
            .filter(row -> databaseId.equals(row.getDatabaseId()))
            .filter(InMemoryViewHouseTableRepository::isView)
            .sorted((left, right) -> left.getTableId().compareTo(right.getTableId()))
            .collect(Collectors.toList());
    int from = Math.min(pageable.getPageNumber() * pageable.getPageSize(), views.size());
    int to = Math.min(from + pageable.getPageSize(), views.size());
    return new PageImpl<>(views.subList(from, to), pageable, views.size());
  }

  @Override
  public HouseTable saveView(HouseTable pointer) {
    saveViewCalls.incrementAndGet();
    events.add(
        SAVE_VIEW
            + "("
            + id(pointer.getDatabaseId(), pointer.getTableId())
            + ",expected="
            + pointer.getTableVersion()
            + ",location="
            + pointer.getTableLocation()
            + ")");

    RuntimeException failure = nextSaveViewFailure.getAndSet(null);
    if (failure != null) {
      throw failure;
    }

    Runnable once = beforeNextCas.getAndSet(null);
    if (once != null) {
      once.run();
    }
    beforeCas.run();

    synchronized (casLock) {
      String key = id(pointer.getDatabaseId(), pointer.getTableId());
      HouseTable current = rows.get(key);
      String expected = pointer.getTableVersion();
      if (CatalogConstants.INITIAL_VERSION.equals(expected)) {
        if (current != null) {
          throw new HouseTableConcurrentUpdateException(
              "Key already occupied: " + key, new RuntimeException("409"));
        }
      } else {
        if (current == null || !current.getTableLocation().equals(expected)) {
          throw new HouseTableConcurrentUpdateException(
              "Expected version does not match current pointer: " + expected,
              new RuntimeException("409"));
        }
      }
      HouseTable saved = pointer.toBuilder().entityType(ENTITY_TYPE_VIEW).build();
      rows.put(key, saved);
      return saved;
    }
  }

  @Override
  public boolean deleteViewById(HouseTablePrimaryKey key) {
    deleteViewByIdCalls.incrementAndGet();
    events.add(DELETE_VIEW + "(" + id(key.getDatabaseId(), key.getTableId()) + ")");
    RuntimeException failure = nextDeleteViewFailure.getAndSet(null);
    if (failure != null) {
      throw failure;
    }
    String rowKey = id(key.getDatabaseId(), key.getTableId());
    HouseTable current = rows.get(rowKey);
    if (current == null || !isView(current)) {
      return false;
    }
    rows.remove(rowKey);
    return true;
  }

  /** The table point read: TABLE or legacy null only, and a legacy row hydrates to TABLE. */
  @Override
  public Optional<HouseTable> findById(HouseTablePrimaryKey key) {
    return Optional.ofNullable(rows.get(id(key.getDatabaseId(), key.getTableId())))
        .filter(InMemoryViewHouseTableRepository::isTableOrLegacy)
        .map(InMemoryViewHouseTableRepository::hydrate);
  }

  @Override
  public Iterable<HouseTable> findAll() {
    return rows.values().stream()
        .map(InMemoryViewHouseTableRepository::hydrate)
        .collect(Collectors.toList());
  }

  /* ---- Unused by the view commit path; fail loudly if a change starts relying on them. ---- */

  @Override
  public List<HouseTable> findAllByDatabaseId(String databaseId) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public Page<HouseTable> findAllByDatabaseId(String databaseId, Pageable pageable) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public void deleteById(HouseTablePrimaryKey key, boolean purge) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public void rename(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String metadataLocation) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public Page<HouseTable> searchSoftDeletedTables(
      String databaseId, String tableId, Pageable pageable) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public void purgeSoftDeletedTables(String databaseId, String tableId, long purgeAfterMs) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public void restoreTable(String databaseId, String tableId, long deletedAtMs) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public <S extends HouseTable> S save(S entity) {
    throw new UnsupportedOperationException("views must publish through saveView");
  }

  @Override
  public <S extends HouseTable> Iterable<S> saveAll(Iterable<S> entities) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public boolean existsById(HouseTablePrimaryKey key) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public Iterable<HouseTable> findAllById(Iterable<HouseTablePrimaryKey> keys) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public long count() {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public void deleteById(HouseTablePrimaryKey key) {
    throw new UnsupportedOperationException("views must drop through deleteViewById");
  }

  @Override
  public void delete(HouseTable entity) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public void deleteAllById(Iterable<? extends HouseTablePrimaryKey> keys) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public void deleteAll(Iterable<? extends HouseTable> entities) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public void deleteAll() {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public Iterable<HouseTable> findAll(Sort sort) {
    throw new UnsupportedOperationException("not used by the view commit path");
  }

  @Override
  public Page<HouseTable> findAll(Pageable pageable) {
    List<HouseTable> all =
        rows.values().stream()
            .map(InMemoryViewHouseTableRepository::hydrate)
            .collect(Collectors.toList());
    return new PageImpl<>(all, PageRequest.of(0, Math.max(all.size(), 1)), all.size());
  }
}
