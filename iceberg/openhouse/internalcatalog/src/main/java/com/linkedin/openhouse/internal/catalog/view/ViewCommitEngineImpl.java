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
 * Iceberg-1.5 implementation of {@link ViewCommitEngine}: build metadata, write the file, then one
 * House Table compare-and-swap, never retried.
 *
 * <p>The pointer row is built here rather than via {@code HouseTableMapper}, whose {@link
 * FileIOManager#getStorage} reverse lookup is lossy — HDFS and LOCAL can share a {@code
 * HadoopFileIO} — and would overwrite the supplied or published storage fact.
 */
@AllArgsConstructor
@Slf4j
public class ViewCommitEngineImpl implements ViewCommitEngine {

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
    requireCreateFlag(intent);
    rejectServerOwnedProperties(intent);
    rejectDuplicateDialects(intent);
    return intent.getIsCreate() ? create(intent) : replace(intent);
  }

  @Override
  public LoadedView loadView(String databaseId, String viewId) {
    HouseTable row = loadRequiredViewRow(databaseId, viewId);
    FileIO fileIO = fileIOManager.getFileIO(storageType.fromString(row.getStorageType()));
    ViewMetadata metadata = viewMetadataCodec.read(fileIO.newInputFile(row.getTableLocation()));
    ViewVersion version = metadata.currentVersion();

    return LoadedView.builder()
        .pointer(toViewPointer(row))
        .viewUuid(metadata.uuid())
        .schema(metadata.schema())
        .representations(toRepresentationIntents(version))
        .sourceDialect(version.summary().get(SOURCE_DIALECT_SUMMARY_KEY))
        .defaultCatalog(version.defaultCatalog())
        .defaultNamespace(version.defaultNamespace())
        .properties(metadata.properties())
        .lastModifiedTime(readLongProperty(metadata, "lastModifiedTime"))
        .currentVersionId(metadata.currentVersionId())
        .build();
  }

  @Override
  public Page<ViewPointer> listViews(String databaseId, Pageable pageable) {
    return houseTableRepository
        .findAllViewsByDatabaseId(databaseId, pageable)
        .map(ViewCommitEngineImpl::toViewPointer);
  }

  @Override
  public boolean dropView(String databaseId, String viewId) {
    try {
      return houseTableRepository.deleteViewById(buildPrimaryKey(databaseId, viewId));
    } catch (HouseTableRepositoryStateUnknownException e) {
      throw new CommitStateUnknownException(e);
    }
  }

  @Override
  public void renameView(String databaseId, String fromViewId, String toViewId) {
    throw new UnsupportedOperationException(
        "Renaming a view is not supported: " + databaseId + "." + fromViewId);
  }

  private void rejectServerOwnedProperties(ViewCommitIntent intent) {
    for (String key : getUserPropertiesOrEmpty(intent).keySet()) {
      if (HouseTableSerdeUtils.IS_OH_PREFIXED.test(key)
          || ViewProperties.REPLACE_DROP_DIALECT_ALLOWED.equals(key)) {
        throw new BadRequestException(
            "Property %s is owned by OpenHouse and cannot be set by a caller", key);
      }
    }
  }

  /** Rejects an intent that lists the same dialect twice, before no-op detection can mask it. */
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

  private ViewCommitResult create(ViewCommitIntent intent) {
    requireCreateInput(intent, intent.getViewUuid(), "viewUuid");
    requireCreateInput(intent, intent.getViewLocation(), "viewLocation");
    requireCreateInput(intent, intent.getStorageType(), "storageType");

    // Classify the caller-supplied snapshot: null is a completed lookup that found the name free.
    // The swap below, not this check, is what ultimately prevents two creates.
    HouseTable occupant = intent.getBaseRow();
    if (occupant != null) {
      requireRowTargetsIntent(intent, occupant);
      rejectOccupiedName(intent, occupant);
    }

    String viewUuid = intent.getViewUuid();
    String viewLocation = intent.getViewLocation();
    FileIO fileIO = fileIOManager.getFileIO(storageType.fromString(intent.getStorageType()));

    String newMetadataLocation = generateMetadataFileLocation(viewLocation, 1);
    String now = String.valueOf(nowMillis());

    Map<String, String> properties = new LinkedHashMap<>(getUserPropertiesOrEmpty(intent));
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
                buildCandidateVersion(intent, 1, CREATE_OPERATION, Long.parseLong(now)),
                intent.getSchema())
            .setProperties(properties)
            .build();

    return writeThenPublish(metadata, fileIO, intent.getStorageType(), newMetadataLocation, true);
  }

  /** Throws BadRequestException if a required create input is null or blank. */
  private static void requireCreateInput(ViewCommitIntent intent, String value, String field) {
    if (value == null || value.trim().isEmpty()) {
      throw new BadRequestException(
          "Cannot create view %s.%s: %s is required and is not supplied by this layer",
          intent.getDatabaseId(), intent.getViewId(), field);
    }
  }

  /** A missing create flag is a bad invocation; it must fail at commit, before any effect. */
  private static void requireCreateFlag(ViewCommitIntent intent) {
    if (intent.getIsCreate() == null) {
      throw new BadRequestException(
          "Cannot commit view %s.%s: isCreate is required",
          intent.getDatabaseId(), intent.getViewId());
    }
  }

  /**
   * A supplied row must name the same logical target as the intent (case-insensitively, per the
   * table-operations identifier convention). A wrong or blank key is a bad invocation, not a
   * concurrency conflict.
   */
  private static void requireRowTargetsIntent(ViewCommitIntent intent, HouseTable row) {
    if (isBlank(row.getDatabaseId())
        || isBlank(row.getTableId())
        || !row.getDatabaseId().equalsIgnoreCase(intent.getDatabaseId())
        || !row.getTableId().equalsIgnoreCase(intent.getViewId())) {
      throw new BadRequestException(
          "Captured row %s.%s does not name the requested view %s.%s",
          row.getDatabaseId(), row.getTableId(), intent.getDatabaseId(), intent.getViewId());
    }
  }

  /**
   * A canonical VIEW snapshot must carry a usable pointer and storage; a blank one is corrupt
   * server state, not absence or a concurrency conflict. Location is checked before storage.
   */
  private static void requireViewPointerAndStorage(ViewCommitIntent intent, HouseTable row) {
    if (isBlank(row.getTableLocation())) {
      throw new IllegalStateException(
          String.format(
              "Corrupt view row %s.%s: tableLocation is missing",
              intent.getDatabaseId(), intent.getViewId()));
    }
    if (isBlank(row.getStorageType())) {
      throw new IllegalStateException(
          String.format(
              "Corrupt view row %s.%s: storageType is missing",
              intent.getDatabaseId(), intent.getViewId()));
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  /**
   * Throws when the name is already taken: AlreadyExists for a view, else
   * ViewNameOccupiedException.
   */
  private void rejectOccupiedName(ViewCommitIntent intent, HouseTable occupant) {
    if (isView(occupant.getEntityType())) {
      throw new AlreadyExistsException(
          "View already exists: %s.%s", intent.getDatabaseId(), intent.getViewId());
    }
    throw new ViewNameOccupiedException(
        intent.getDatabaseId(), intent.getViewId(), occupant.getEntityType());
  }

  /**
   * True only if entityType is exactly "VIEW" (a differently-cased value is a corrupt row, not a
   * view).
   */
  private static boolean isView(String entityType) {
    return ENTITY_TYPE_VIEW.equals(entityType);
  }

  private ViewCommitResult replace(ViewCommitIntent intent) {
    HouseTable row = intent.getBaseRow();
    if (row == null || !isView(row.getEntityType())) {
      // A completed lookup that found absence, or a non-view: never turned into a create.
      throw new NoSuchViewException(
          "View does not exist: %s.%s", intent.getDatabaseId(), intent.getViewId());
    }
    requireRowTargetsIntent(intent, row);
    requireViewPointerAndStorage(intent, row);

    // The captured row's own storage and its exact metadata path, never re-read from House Table.
    String capturedBase = row.getTableLocation();
    FileIO fileIO = fileIOManager.getFileIO(storageType.fromString(row.getStorageType()));
    ViewMetadata current = viewMetadataCodec.read(fileIO.newInputFile(capturedBase));

    Map<String, String> currentUserProperties = extractUserPropertiesFromMetadata(current);
    Map<String, String> userProperties = new LinkedHashMap<>(currentUserProperties);
    userProperties.putAll(getUserPropertiesOrEmpty(intent));

    if (isUnchanged(intent, current, userProperties, currentUserProperties)) {
      // Nothing observable changed, so do not manufacture a version or timestamp.
      return ViewCommitResult.builder()
          .pointer(toViewPointer(row))
          .viewUuid(current.uuid())
          .lastModifiedTime(readLongProperty(current, "lastModifiedTime"))
          .created(false)
          .metadataChanged(false)
          .build();
    }

    String newMetadataLocation =
        generateMetadataFileLocation(current.location(), current.history().size() + 1);
    String now = String.valueOf(advanceLastModified(readLongProperty(current, "lastModifiedTime")));

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

    // buildFrom preserves identity and history; Iceberg assigns the version id.
    ViewMetadata metadata =
        ViewMetadata.buildFrom(current)
            .setCurrentVersion(
                buildCandidateVersion(
                    intent, current.currentVersionId() + 1, REPLACE_OPERATION, Long.parseLong(now)),
                intent.getSchema())
            .setProperties(properties)
            .build();

    return writeThenPublish(metadata, fileIO, row.getStorageType(), newMetadataLocation, false);
  }

  private boolean isUnchanged(
      ViewCommitIntent intent,
      ViewMetadata current,
      Map<String, String> mergedUserProperties,
      Map<String, String> currentUserProperties) {
    ViewVersion version = current.currentVersion();
    // sameSchema, not asStruct: the struct ignores identifier-field ids.
    return current.schema().sameSchema(intent.getSchema())
        && buildSortedRepresentationKeys(version)
            .equals(buildSortedRepresentationKeys(intent.getRepresentations()))
        && Objects.equals(
            version.summary().get(SOURCE_DIALECT_SUMMARY_KEY), intent.getSourceDialect())
        && Objects.equals(version.defaultCatalog(), intent.getDefaultCatalog())
        // Same normalization used to build it, or every namespace-less replace looks changed.
        && Objects.equals(
            version.defaultNamespace(), normalizeNamespace(intent.getDefaultNamespace()))
        && mergedUserProperties.equals(currentUserProperties);
  }

  private ViewCommitResult writeThenPublish(
      ViewMetadata metadata,
      FileIO fileIO,
      String storageTypeValue,
      String newMetadataLocation,
      boolean isCreate) {
    long lastModifiedTime = readLongProperty(metadata, "lastModifiedTime");
    viewMetadataCodec.write(metadata, fileIO.newOutputFile(newMetadataLocation));

    HouseTable pointer = buildPointerRow(metadata, storageTypeValue);

    HouseTable saved;
    try {
      saved = houseTableRepository.saveView(pointer);
    } catch (HouseTableConcurrentUpdateException e) {
      // A create lost a name; a replace lost a commit.
      if (isCreate) {
        throw new AlreadyExistsException(
            e, "View already exists: %s.%s", pointer.getDatabaseId(), pointer.getTableId());
      } else {
        throw new CommitFailedException(
            e,
            "Cannot replace view %s.%s: it was modified concurrently",
            pointer.getDatabaseId(),
            pointer.getTableId());
      }
    } catch (HouseTableRepositoryStateUnknownException e) {
      // Not retried, re-read or cleaned up: the write may have landed.
      throw new CommitStateUnknownException(e);
    }

    return ViewCommitResult.builder()
        .pointer(toViewPointer(saved))
        .viewUuid(metadata.uuid())
        .lastModifiedTime(lastModifiedTime)
        .created(isCreate)
        .metadataChanged(true)
        .build();
  }

  private static HouseTable buildPointerRow(ViewMetadata metadata, String storageTypeValue) {
    Map<String, String> properties = metadata.properties();
    return HouseTable.builder()
        .databaseId(properties.get(getCanonicalFieldName("databaseId")))
        .tableId(properties.get(getCanonicalFieldName("tableId")))
        .tableUUID(properties.get(getCanonicalFieldName("tableUUID")))
        .tableVersion(properties.get(getCanonicalFieldName("tableVersion")))
        .tableLocation(properties.get(getCanonicalFieldName("tableLocation")))
        .tableCreator(properties.get(getCanonicalFieldName("tableCreator")))
        .creationTime(readLongProperty(metadata, "creationTime"))
        .storageType(storageTypeValue)
        .build();
  }

  /** The version id is a placeholder that Iceberg reassigns. */
  private static ViewVersion buildCandidateVersion(
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
            .defaultNamespace(normalizeNamespace(intent.getDefaultNamespace()))
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

  /** Embeds a random UUID so concurrent writers don't collide. */
  private static String generateMetadataFileLocation(String viewLocation, int version) {
    return String.format(
        "%s/%05d-%s%s", viewLocation, version, UUID.randomUUID(), METADATA_FILE_EXTENSION);
  }

  /** Throws NoSuchViewException when absent; a non-view (table) row reads as absent here. */
  private HouseTable loadRequiredViewRow(String databaseId, String viewId) {
    return houseTableRepository
        .findViewById(buildPrimaryKey(databaseId, viewId))
        .orElseThrow(
            () -> new NoSuchViewException("View does not exist: %s.%s", databaseId, viewId));
  }

  private static ViewPointer toViewPointer(HouseTable row) {
    return ViewPointer.builder()
        .databaseId(row.getDatabaseId())
        .viewId(row.getTableId())
        .metadataLocation(row.getTableLocation())
        .storageType(row.getStorageType())
        .creationTime(row.getCreationTime())
        .build();
  }

  private static Map<String, String> getUserPropertiesOrEmpty(ViewCommitIntent intent) {
    return intent.getViewProperties() == null ? Collections.emptyMap() : intent.getViewProperties();
  }

  /** Drops every server-stamped (openhouse-prefixed or dialect-policy) key. */
  private static Map<String, String> extractUserPropertiesFromMetadata(ViewMetadata metadata) {
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

  private static List<String> buildSortedRepresentationKeys(ViewVersion version) {
    List<String> pairs = new ArrayList<>();
    for (ViewRepresentation representation : version.representations()) {
      if (representation instanceof SQLViewRepresentation) {
        SQLViewRepresentation sql = (SQLViewRepresentation) representation;
        pairs.add(buildRepresentationKey(sql.dialect(), sql.sql()));
      }
    }
    Collections.sort(pairs);
    return pairs;
  }

  private static List<String> buildSortedRepresentationKeys(
      List<SqlViewRepresentationIntent> representations) {
    List<String> pairs = new ArrayList<>();
    if (representations != null) {
      for (SqlViewRepresentationIntent representation : representations) {
        pairs.add(buildRepresentationKey(representation.getDialect(), representation.getSql()));
      }
    }
    Collections.sort(pairs);
    return pairs;
  }

  /** One comparable key per representation: dialect and SQL joined on a NUL delimiter. */
  private static String buildRepresentationKey(String dialect, String sql) {
    return dialect + '\u0000' + sql;
  }

  private static List<SqlViewRepresentationIntent> toRepresentationIntents(ViewVersion version) {
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

  /** Reads a numeric metadata property, treating an absent value as 0. */
  private static long readLongProperty(ViewMetadata metadata, String htsField) {
    String value = metadata.properties().get(getCanonicalFieldName(htsField));
    return value == null ? 0L : Long.parseLong(value);
  }

  private static HouseTablePrimaryKey buildPrimaryKey(String databaseId, String viewId) {
    return HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(viewId).build();
  }

  /** One canonical form (Namespace.empty() for null) so build and comparison agree. */
  private static Namespace normalizeNamespace(Namespace defaultNamespace) {
    return defaultNamespace == null ? Namespace.empty() : defaultNamespace;
  }

  /** Returns the current time in epoch millis (overridable so a test can pin it). */
  protected long nowMillis() {
    return Instant.now(Clock.systemUTC()).toEpochMilli();
  }

  /** Advances against the clock when possible, saturating at Long.MAX_VALUE. */
  private long advanceLastModified(long previousLastModified) {
    long now = nowMillis();
    if (previousLastModified == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return Math.max(now, previousLastModified + 1);
  }
}
