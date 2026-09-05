package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.SqlViewRepresentationIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewProperties;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;
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

  /** The dialect the caller authored in, recorded with the version rather than as a property. */
  private static final String SOURCE_DIALECT_SUMMARY_KEY = "sourceDialect";

  private static final String OPERATION_SUMMARY_KEY = "operation";
  private static final String CREATE_OPERATION = "create";
  private static final String REPLACE_OPERATION = "replace";

  private static final String ENTITY_TYPE_VIEW = "VIEW";

  private static final String METADATA_FILE_EXTENSION = ".metadata.json";

  private final HouseTableRepository houseTableRepository;

  private final FileIOManager fileIOManager;

  private final ViewMetadataCodec viewMetadataCodec;

  private final StorageType storageType;

  @Override
  public ViewCommitResult commit(ViewCommitIntent intent) {
    rejectServerOwnedProperties(intent);
    rejectDuplicateDialects(intent);
    return intent.getBaseViewVersion() == null ? create(intent) : replace(intent);
  }

  private ViewCommitResult create(ViewCommitIntent intent) {
    requireCreateInput(intent, intent.getViewUuid(), "viewUuid");
    requireCreateInput(intent, intent.getViewLocation(), "viewLocation");
    requireCreateInput(intent, intent.getStorageType(), "storageType");

    // Advisory only: it picks the collision message. The swap below is what prevents two creates.
    houseTableRepository
        .findEntityById(keyOf(intent.getDatabaseId(), intent.getViewId()))
        .ifPresent(occupant -> rejectOccupiedName(intent, occupant));

    String viewUuid = intent.getViewUuid();
    String viewLocation = intent.getViewLocation();
    FileIO fileIO = fileIOManager.getFileIO(storageType.fromString(intent.getStorageType()));

    String newMetadataLocation = metadataFileLocation(viewLocation, 1);
    String now = String.valueOf(nowMillis());

    Map<String, String> properties = new LinkedHashMap<>(userPropertiesOf(intent));
    properties.put(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "false");
    properties.put(getCanonicalFieldName("tableUUID"), viewUuid);
    properties.put(getCanonicalFieldName("tableId"), intent.getViewId());
    properties.put(getCanonicalFieldName("databaseId"), intent.getDatabaseId());
    properties.put(getCanonicalFieldName("tableCreator"), intent.getCreator());
    properties.put(getCanonicalFieldName("tableVersion"), CatalogConstants.INITIAL_VERSION);
    properties.put(getCanonicalFieldName("tableLocation"), newMetadataLocation);
    properties.put(getCanonicalFieldName("creationTime"), now);
    properties.put(getCanonicalFieldName("lastModifiedTime"), now);

    ViewMetadata metadata =
        ViewMetadata.builder()
            .assignUUID(viewUuid)
            .setLocation(viewLocation)
            .setCurrentVersion(
                candidateVersion(intent, 1, CREATE_OPERATION, Long.parseLong(now)),
                intent.getSchema())
            .setProperties(properties)
            .build();

    return writeThenPublish(
        intent,
        metadata,
        fileIO,
        intent.getStorageType(),
        newMetadataLocation,
        viewUuid,
        Long.parseLong(now),
        true);
  }

  /**
   * The engine allocates nothing, so a missing create-side value is a caller error rather than
   * something to substitute for. This mirrors {@code
   * OpenHouseInternalCatalog.defaultWarehouseLocation}, which throws instead of choosing a root.
   */
  private static void requireCreateInput(ViewCommitIntent intent, String value, String field) {
    if (value == null || value.trim().isEmpty()) {
      throw new BadRequestException(
          "Cannot create view %s.%s: %s is required and is not supplied by this layer",
          intent.getDatabaseId(), intent.getViewId(), field);
    }
  }

  /**
   * The occupant's type arrives already resolved: House Table converts a legacy null to TABLE at
   * its own parse boundary, and every test double reproduces that, so there is no null to
   * normalize.
   */
  private void rejectOccupiedName(ViewCommitIntent intent, HouseTable occupant) {
    if (isView(occupant.getEntityType())) {
      throw new AlreadyExistsException(
          "View already exists: %s.%s", intent.getDatabaseId(), intent.getViewId());
    }
    throw new ViewNameOccupiedException(
        intent.getDatabaseId(), intent.getViewId(), occupant.getEntityType());
  }

  /** Exact spelling only: a differently-cased value is a corrupted row, not a view. */
  private static boolean isView(String entityType) {
    return ENTITY_TYPE_VIEW.equals(entityType);
  }

  private ViewCommitResult replace(ViewCommitIntent intent) {
    HouseTable row = requireViewRow(intent.getDatabaseId(), intent.getViewId());
    // The row's own storage, never the incoming one: a replace does not re-place anything.
    FileIO fileIO = fileIOManager.getFileIO(storageType.fromString(row.getStorageType()));
    ViewMetadata current = viewMetadataCodec.read(fileIO.newInputFile(row.getTableLocation()));

    String capturedBase = intent.getBaseViewVersion();
    if (!capturedBase.equals(row.getTableLocation())) {
      throw new CommitFailedException(
          "Cannot replace view %s.%s: base version %s is not the current version %s",
          intent.getDatabaseId(), intent.getViewId(), capturedBase, row.getTableLocation());
    }

    Map<String, String> currentUserProperties = userPropertiesOf(current);
    Map<String, String> userProperties = new LinkedHashMap<>(currentUserProperties);
    userProperties.putAll(userPropertiesOf(intent));

    if (isUnchanged(intent, current, userProperties, currentUserProperties)) {
      // Nothing observable changes, so do not manufacture a version or a last-modified time.
      return ViewCommitResult.builder()
          .pointer(pointerOf(row))
          .viewUuid(current.uuid())
          .lastModifiedTime(longProperty(current, "lastModifiedTime"))
          .created(false)
          .metadataChanged(false)
          .build();
    }

    String newMetadataLocation =
        metadataFileLocation(current.location(), current.history().size() + 1);
    String now = String.valueOf(advanceLastModified(longProperty(current, "lastModifiedTime")));

    Map<String, String> properties = new LinkedHashMap<>(userProperties);
    properties.put(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "false");
    properties.put(getCanonicalFieldName("tableUUID"), current.uuid());
    properties.put(getCanonicalFieldName("tableId"), intent.getViewId());
    properties.put(getCanonicalFieldName("databaseId"), intent.getDatabaseId());
    properties.put(
        getCanonicalFieldName("tableCreator"),
        current
            .properties()
            .getOrDefault(getCanonicalFieldName("tableCreator"), intent.getCreator()));
    properties.put(getCanonicalFieldName("tableVersion"), capturedBase);
    properties.put(getCanonicalFieldName("tableLocation"), newMetadataLocation);
    properties.put(
        getCanonicalFieldName("creationTime"),
        current.properties().getOrDefault(getCanonicalFieldName("creationTime"), now));
    properties.put(getCanonicalFieldName("lastModifiedTime"), now);

    // buildFrom preserves identity and history; Iceberg assigns the resulting version id.
    ViewMetadata metadata =
        ViewMetadata.buildFrom(current)
            .setCurrentVersion(
                candidateVersion(
                    intent, current.currentVersionId() + 1, REPLACE_OPERATION, Long.parseLong(now)),
                intent.getSchema())
            .setProperties(properties)
            .build();

    return writeThenPublish(
        intent,
        metadata,
        fileIO,
        row.getStorageType(),
        newMetadataLocation,
        current.uuid(),
        Long.parseLong(now),
        false);
  }

  /**
   * Engine-owned: Iceberg's {@code sameViewVersion} compares the whole summary map, and every
   * submission carries a fresh timestamp and operation, so it treats each one as new.
   */
  private boolean isUnchanged(
      ViewCommitIntent intent,
      ViewMetadata current,
      Map<String, String> mergedUserProperties,
      Map<String, String> currentUserProperties) {
    ViewVersion version = current.currentVersion();
    // sameSchema, not asStruct: the struct ignores identifier-field ids.
    return current.schema().sameSchema(intent.getSchema())
        && representationsOf(version).equals(representationsOf(intent.getRepresentations()))
        && Objects.equals(
            version.summary().get(SOURCE_DIALECT_SUMMARY_KEY), intent.getSourceDialect())
        && Objects.equals(version.defaultCatalog(), intent.getDefaultCatalog())
        // The same normalization the metadata was built with: comparing a persisted empty namespace
        // against a raw null would make every namespace-less replace look like a change.
        && Objects.equals(
            version.defaultNamespace(), normalizedNamespace(intent.getDefaultNamespace()))
        && mergedUserProperties.equals(currentUserProperties);
  }

  /**
   * Writes the file before publishing, so a swap loser leaves an unreachable file rather than a
   * pointer to nothing. The expected version travels inside the metadata as {@code
   * openhouse.tableVersion}; re-deriving it here would turn a conditional write into a blind one.
   */
  private ViewCommitResult writeThenPublish(
      ViewCommitIntent intent,
      ViewMetadata metadata,
      FileIO fileIO,
      String storageTypeValue,
      String newMetadataLocation,
      String viewUuid,
      long lastModifiedTime,
      boolean created) {
    viewMetadataCodec.write(metadata, fileIO.newOutputFile(newMetadataLocation));

    HouseTable pointer = pointerRowOf(metadata, storageTypeValue);

    HouseTable saved;
    try {
      saved = houseTableRepository.saveView(pointer);
    } catch (HouseTableConcurrentUpdateException e) {
      // A create lost a name; a replace lost a commit. The caller acts on each differently.
      if (created) {
        throw new AlreadyExistsException(
            e, "View already exists: %s.%s", intent.getDatabaseId(), intent.getViewId());
      }
      throw new CommitFailedException(
          e,
          "Cannot replace view %s.%s: it was modified concurrently",
          intent.getDatabaseId(),
          intent.getViewId());
    } catch (HouseTableRepositoryStateUnknownException e) {
      // Not retried, not re-read, and the candidate is not deleted: the write may well have landed.
      throw new CommitStateUnknownException(e);
    }

    return ViewCommitResult.builder()
        .pointer(pointerOf(saved))
        .viewUuid(viewUuid)
        .lastModifiedTime(lastModifiedTime)
        .created(created)
        .metadataChanged(true)
        .build();
  }

  @Override
  public LoadedView loadView(String databaseId, String viewId) {
    HouseTable row = requireViewRow(databaseId, viewId);
    FileIO fileIO = fileIOManager.getFileIO(storageType.fromString(row.getStorageType()));
    ViewMetadata metadata = viewMetadataCodec.read(fileIO.newInputFile(row.getTableLocation()));
    ViewVersion version = metadata.currentVersion();

    return LoadedView.builder()
        .pointer(pointerOf(row))
        .viewUuid(metadata.uuid())
        .schema(metadata.schema())
        .representations(representationIntentsOf(version))
        .sourceDialect(version.summary().get(SOURCE_DIALECT_SUMMARY_KEY))
        .defaultCatalog(version.defaultCatalog())
        .defaultNamespace(version.defaultNamespace())
        .properties(metadata.properties())
        .lastModifiedTime(longProperty(metadata, "lastModifiedTime"))
        .currentVersionId(metadata.currentVersionId())
        .build();
  }

  @Override
  public Page<ViewPointer> listViews(String databaseId, Pageable pageable) {
    return houseTableRepository
        .findAllViewsByDatabaseId(databaseId, pageable)
        .map(ViewCommitEngineImpl::pointerOf);
  }

  @Override
  public boolean dropView(String databaseId, String viewId) {
    try {
      return houseTableRepository.deleteViewById(keyOf(databaseId, viewId));
    } catch (HouseTableRepositoryStateUnknownException e) {
      throw new CommitStateUnknownException(e);
    }
  }

  @Override
  public void renameView(String databaseId, String fromViewId, String toViewId) {
    throw new UnsupportedOperationException(
        "Renaming a view is not supported: " + databaseId + "." + fromViewId);
  }

  /**
   * Absent and non-view are the same answer, because the typed finder already filters a non-view
   * out as absent. Reporting a table as absent would tell a later create the name is free.
   */
  private HouseTable requireViewRow(String databaseId, String viewId) {
    return houseTableRepository
        .findViewById(keyOf(databaseId, viewId))
        .orElseThrow(
            () -> new NoSuchViewException("View does not exist: %s.%s", databaseId, viewId));
  }

  /**
   * Reject duplicate dialects (compared case-insensitively) before no-op detection, so a duplicate
   * is always a caller error instead of short-circuiting into a no-op that never reaches Iceberg's
   * check.
   */
  private void rejectDuplicateDialects(ViewCommitIntent intent) {
    if (intent.getRepresentations() == null) {
      return;
    }
    Set<String> dialects = new HashSet<>();
    for (SqlViewRepresentationIntent representation : intent.getRepresentations()) {
      String dialect = representation.getDialect();
      if (dialect != null && !dialects.add(dialect.toLowerCase(Locale.ROOT))) {
        throw new BadRequestException("Cannot add multiple queries for dialect %s", dialect);
      }
    }
  }

  /** Overridable so a test can pin it. */
  protected long nowMillis() {
    return Instant.now(Clock.systemUTC()).toEpochMilli();
  }

  /**
   * Keep a changed commit observably newer than the version it replaced: two commits in the same
   * millisecond, or a backward clock, would otherwise leave last-modified equal or lower.
   */
  private long advanceLastModified(long previousLastModified) {
    long now = nowMillis();
    if (previousLastModified == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return Math.max(now, previousLastModified + 1);
  }

  private void rejectServerOwnedProperties(ViewCommitIntent intent) {
    for (String key : userPropertiesOf(intent).keySet()) {
      if (HouseTableSerdeUtils.IS_OH_PREFIXED.test(key)
          || ViewProperties.REPLACE_DROP_DIALECT_ALLOWED.equals(key)) {
        throw new BadRequestException(
            "Property %s is owned by OpenHouse and cannot be set by a caller", key);
      }
    }
  }

  /** One definition of the empty namespace, used to build metadata and to compare it. */
  private static Namespace normalizedNamespace(Namespace defaultNamespace) {
    return defaultNamespace == null ? Namespace.empty() : defaultNamespace;
  }

  private static HouseTablePrimaryKey keyOf(String databaseId, String viewId) {
    return HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(viewId).build();
  }

  /**
   * Carries the storage type it was given rather than one recovered from the {@code FileIO}: that
   * reverse lookup cannot distinguish two storages sharing a file-IO implementation.
   */
  private static HouseTable pointerRowOf(ViewMetadata metadata, String storageTypeValue) {
    Map<String, String> properties = metadata.properties();
    return HouseTable.builder()
        .databaseId(properties.get(getCanonicalFieldName("databaseId")))
        .tableId(properties.get(getCanonicalFieldName("tableId")))
        .tableUUID(properties.get(getCanonicalFieldName("tableUUID")))
        .tableVersion(properties.get(getCanonicalFieldName("tableVersion")))
        .tableLocation(properties.get(getCanonicalFieldName("tableLocation")))
        .tableCreator(properties.get(getCanonicalFieldName("tableCreator")))
        .creationTime(longProperty(metadata, "creationTime"))
        .storageType(storageTypeValue)
        .build();
  }

  private static ViewPointer pointerOf(HouseTable row) {
    return ViewPointer.builder()
        .databaseId(row.getDatabaseId())
        .viewId(row.getTableId())
        .metadataLocation(row.getTableLocation())
        .storageType(row.getStorageType())
        .creationTime(row.getCreationTime())
        .build();
  }

  private static Map<String, String> userPropertiesOf(ViewCommitIntent intent) {
    return intent.getViewProperties() == null ? Collections.emptyMap() : intent.getViewProperties();
  }

  /** Everything the server did not stamp, which is exactly what structural equality compares. */
  private static Map<String, String> userPropertiesOf(ViewMetadata metadata) {
    Map<String, String> userProperties = new LinkedHashMap<>();
    metadata
        .properties()
        .forEach(
            (key, value) -> {
              if (!HouseTableSerdeUtils.IS_OH_PREFIXED.test(key)
                  && !ViewProperties.REPLACE_DROP_DIALECT_ALLOWED.equals(key)) {
                userProperties.put(key, value);
              }
            });
    return userProperties;
  }

  private static long longProperty(ViewMetadata metadata, String htsField) {
    String value = metadata.properties().get(getCanonicalFieldName(htsField));
    return value == null ? 0L : Long.parseLong(value);
  }

  /** The submitted version id is a placeholder; Iceberg reassigns it. */
  private static ViewVersion candidateVersion(
      ViewCommitIntent intent,
      int candidateVersionId,
      String operation,
      long candidateTimestampMillis) {
    List<ViewRepresentation> representations = new ArrayList<>();
    if (intent.getRepresentations() != null) {
      for (SqlViewRepresentationIntent representation : intent.getRepresentations()) {
        representations.add(
            ImmutableSQLViewRepresentation.builder()
                .sql(representation.getSql())
                .dialect(representation.getDialect())
                .build());
      }
    }

    ImmutableViewVersion.Builder builder =
        ImmutableViewVersion.builder()
            .versionId(candidateVersionId)
            .timestampMillis(candidateTimestampMillis)
            .schemaId(Optional.ofNullable(intent.getSchema()).map(Schema::schemaId).orElse(0))
            .defaultNamespace(normalizedNamespace(intent.getDefaultNamespace()))
            .putSummary(OPERATION_SUMMARY_KEY, operation)
            .addAllRepresentations(representations);
    if (intent.getDefaultCatalog() != null) {
      builder.defaultCatalog(intent.getDefaultCatalog());
    }
    if (intent.getSourceDialect() != null) {
      builder.putSummary(SOURCE_DIALECT_SUMMARY_KEY, intent.getSourceDialect());
    }
    return builder.build();
  }

  /**
   * Sorted pairs, not a map: a map drops all but the last entry for a repeated dialect, letting an
   * invalid submission compare equal to a valid stored definition. Order carries no meaning.
   */
  private static List<String> representationsOf(ViewVersion version) {
    List<String> pairs = new ArrayList<>();
    for (ViewRepresentation representation : version.representations()) {
      if (representation instanceof SQLViewRepresentation) {
        SQLViewRepresentation sql = (SQLViewRepresentation) representation;
        pairs.add(representationKey(sql.dialect(), sql.sql()));
      }
    }
    Collections.sort(pairs);
    return pairs;
  }

  private static List<String> representationsOf(List<SqlViewRepresentationIntent> representations) {
    List<String> pairs = new ArrayList<>();
    if (representations != null) {
      for (SqlViewRepresentationIntent representation : representations) {
        pairs.add(representationKey(representation.getDialect(), representation.getSql()));
      }
    }
    Collections.sort(pairs);
    return pairs;
  }

  private static String representationKey(String dialect, String sql) {
    return dialect + '\u0000' + sql;
  }

  private static List<SqlViewRepresentationIntent> representationIntentsOf(ViewVersion version) {
    List<SqlViewRepresentationIntent> representations = new ArrayList<>();
    for (ViewRepresentation representation : version.representations()) {
      if (representation instanceof SQLViewRepresentation) {
        SQLViewRepresentation sql = (SQLViewRepresentation) representation;
        representations.add(
            SqlViewRepresentationIntent.builder().sql(sql.sql()).dialect(sql.dialect()).build());
      }
    }
    return representations;
  }

  /** The embedded UUID lets concurrent writers each write a candidate without colliding. */
  private static String metadataFileLocation(String viewLocation, int version) {
    return String.format(
        "%s/%05d-%s%s", viewLocation, version, UUID.randomUUID(), METADATA_FILE_EXTENSION);
  }
}
