package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.common.schema.IcebergSchemaHelper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.view.ViewCommitEngine;
import com.linkedin.openhouse.internal.catalog.view.model.SqlViewRepresentationIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalViewRepository;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

/**
 * Default {@link OpenHouseInternalViewRepository}, mirroring {@code
 * OpenHouseInternalRepositoryImpl} (tables) without reusing it (plan &sect;6). Gated to the
 * Iceberg-view-capable runtime via the {@link ViewsRepositoryConfig} factory, since a single {@link
 * Storage} dependency cannot be constructor-autowired unconditionally when a cluster configures
 * more than one storage (see {@code ViewsDualStorageH2IntegrationTest}).
 */
public class OpenHouseInternalViewRepositoryImpl implements OpenHouseInternalViewRepository {

  private static final String VIEW_ENTITY_TYPE = "VIEW";

  private final HouseTableRepository houseTableRepository;

  private final ViewCommitEngine viewCommitEngine;

  private final StorageSelector storageSelector;

  /**
   * The cluster's default storage. Reserved for storage-boundary checks that do not yet have a
   * concrete M1 use (mirrors the unused {@code storageManager} field already present on {@code
   * OpenHouseInternalRepositoryImpl}); create-path storage is always selected per-request via
   * {@link #storageSelector}, never from this field.
   */
  private final Storage defaultStorage;

  public OpenHouseInternalViewRepositoryImpl(
      HouseTableRepository houseTableRepository,
      ViewCommitEngine viewCommitEngine,
      StorageSelector storageSelector,
      Storage defaultStorage) {
    this.houseTableRepository = houseTableRepository;
    this.viewCommitEngine = viewCommitEngine;
    this.storageSelector = storageSelector;
    this.defaultStorage = defaultStorage;
  }

  @Override
  public PreparedViewOperation prepareWrite(String databaseId, String viewId) {
    Optional<HouseTable> occupant =
        houseTableRepository.findEntityById(primaryKey(databaseId, viewId));
    if (!occupant.isPresent()) {
      return PreparedViewOperation.observedAbsence();
    }
    HouseTable row = occupant.get();
    return VIEW_ENTITY_TYPE.equals(row.getEntityType())
        ? PreparedViewOperation.view(row)
        : PreparedViewOperation.tableOccupant(row);
  }

  @Override
  public PreparedViewOperation prepareDelete(String databaseId, String viewId) {
    Optional<HouseTable> viewRow =
        houseTableRepository.findViewById(primaryKey(databaseId, viewId));
    return viewRow
        .map(PreparedViewOperation::view)
        .orElseGet(PreparedViewOperation::observedAbsence);
  }

  @Override
  public ViewDto findById(String databaseId, String viewId) {
    HouseTable viewRow =
        houseTableRepository
            .findViewById(primaryKey(databaseId, viewId))
            .orElseThrow(() -> noSuchView(databaseId, viewId));
    return toPointerDto(viewRow);
  }

  @Override
  public Page<ViewDto> searchViews(String databaseId, Pageable pageable) {
    Page<ViewPointer> enginePage = viewCommitEngine.listViews(databaseId, pageable);
    List<ViewDto> dtos =
        enginePage.getContent().stream()
            .map(
                pointer ->
                    ViewDto.builder()
                        .viewId(pointer.getViewId())
                        .databaseId(pointer.getDatabaseId())
                        .build())
            .collect(Collectors.toList());
    return new PageImpl<>(dtos, pageable, enginePage.getTotalElements());
  }

  @Override
  public ViewCommitOutcome commitCreate(
      CreateUpdateViewRequestBody requestBody,
      PreparedViewOperation prepared,
      String actingPrincipal) {
    String databaseId = requestBody.getDatabaseId();
    String viewId = requestBody.getViewId();
    String viewUuid = UUID.randomUUID().toString();
    Storage storage = storageSelector.selectStorage(databaseId, viewId);
    String storageType = storage.getType().getValue();
    // The bare allocated root, passed verbatim: the engine stores it as ViewMetadata.location()
    // and lays metadata files out flat directly under it (no /metadata subdirectory of its own),
    // per its frozen contract.
    String viewLocation =
        storage.allocateTableLocation(
            databaseId, viewId, viewUuid, actingPrincipal, viewPropertiesOrEmpty(requestBody));

    ViewCommitIntent intent =
        baseIntentBuilder(requestBody, actingPrincipal)
            .isCreate(true)
            .baseRow(prepared.getViewBaseRow().orElse(null))
            .viewUuid(viewUuid)
            .viewLocation(viewLocation)
            .storageType(storageType)
            .build();

    ViewCommitResult result = viewCommitEngine.commit(intent);
    return ViewCommitOutcome.builder()
        .dto(toCommittedDto(databaseId, viewId, result))
        .committedViewUuid(result.getViewUuid())
        .created(result.isCreated())
        .build();
  }

  @Override
  public ViewCommitOutcome commitReplace(
      CreateUpdateViewRequestBody requestBody,
      PreparedViewOperation prepared,
      String actingPrincipal) {
    String databaseId = requestBody.getDatabaseId();
    String viewId = requestBody.getViewId();

    ViewCommitIntent intent =
        baseIntentBuilder(requestBody, actingPrincipal)
            .isCreate(false)
            .baseRow(prepared.getViewBaseRow().orElse(null))
            .build();

    ViewCommitResult result = viewCommitEngine.commit(intent);
    return ViewCommitOutcome.builder()
        .dto(toCommittedDto(databaseId, viewId, result))
        .committedViewUuid(result.getViewUuid())
        .created(result.isCreated())
        .build();
  }

  @Override
  public void deleteById(String databaseId, String viewId) {
    boolean dropped = viewCommitEngine.dropView(databaseId, viewId);
    if (!dropped) {
      // A permitted concurrent writer dropped this view (or replaced it with a differently-typed
      // occupant) between this operation's capture and the one attempt the frozen name-based
      // engine makes here: no refresh, no retry, no new UUID-conditional-delete contract. The key
      // no longer names a view, which is exactly the existing typed NO_SUCH_VIEW contract
      // findById already uses for the same observation, not a server fault.
      throw noSuchView(databaseId, viewId);
    }
  }

  private ViewCommitIntent.ViewCommitIntentBuilder baseIntentBuilder(
      CreateUpdateViewRequestBody requestBody, String actingPrincipal) {
    Schema schema = IcebergSchemaHelper.getSchemaFromSchemaJson(requestBody.getSchema());
    return ViewCommitIntent.builder()
        .databaseId(requestBody.getDatabaseId())
        .viewId(requestBody.getViewId())
        .schema(schema)
        .representations(toEngineRepresentations(requestBody.getRepresentations()))
        .sourceDialect(requestBody.getSourceDialect())
        .defaultCatalog(requestBody.getDefaultCatalog())
        .defaultNamespace(toNamespace(requestBody.getDefaultNamespace()))
        .viewProperties(requestBody.getViewProperties())
        .creator(actingPrincipal);
  }

  private static Map<String, String> viewPropertiesOrEmpty(
      CreateUpdateViewRequestBody requestBody) {
    return requestBody.getViewProperties() == null
        ? Collections.emptyMap()
        : requestBody.getViewProperties();
  }

  private static List<SqlViewRepresentationIntent> toEngineRepresentations(
      List<ViewRepresentation> representations) {
    if (representations == null) {
      return Collections.emptyList();
    }
    return representations.stream()
        .map(
            representation ->
                SqlViewRepresentationIntent.builder()
                    .sql(representation.getSql())
                    .dialect(representation.getDialect())
                    .build())
        .collect(Collectors.toList());
  }

  private static Namespace toNamespace(List<String> defaultNamespace) {
    if (defaultNamespace == null || defaultNamespace.isEmpty()) {
      return Namespace.empty();
    }
    return Namespace.of(defaultNamespace.toArray(new String[0]));
  }

  private static ViewDto toCommittedDto(String databaseId, String viewId, ViewCommitResult result) {
    ViewPointer pointer = result.getPointer();
    return ViewDto.builder()
        .databaseId(databaseId)
        .viewId(viewId)
        .metadataLocation(pointer.getMetadataLocation())
        .viewVersion(pointer.getMetadataLocation())
        .lastModifiedTime(result.getLastModifiedTime())
        .build();
  }

  private static ViewDto toPointerDto(HouseTable row) {
    return ViewDto.builder()
        .viewId(row.getTableId())
        .databaseId(row.getDatabaseId())
        .metadataLocation(row.getTableLocation())
        .viewVersion(row.getTableLocation())
        .viewCreator(row.getTableCreator())
        .creationTime(row.getCreationTime())
        .lastModifiedTime(row.getLastModifiedTime())
        .build();
  }

  private static HouseTablePrimaryKey primaryKey(String databaseId, String viewId) {
    return HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(viewId).build();
  }

  private static ViewApiException noSuchView(String databaseId, String viewId) {
    return new ViewApiException(
        ViewErrorCode.NO_SUCH_VIEW, "No such view: " + databaseId + "." + viewId);
  }

  /**
   * Gated bean factory: a bare {@link Storage} cannot be constructor-autowired unconditionally once
   * more than one storage is configured, so this explicitly resolves the cluster's default storage
   * via {@link StorageManager} rather than relying on by-type injection.
   */
  @Configuration
  @ConditionalOnClass(name = "org.apache.iceberg.view.ViewMetadata")
  public static class ViewsRepositoryConfig {

    @Bean
    public OpenHouseInternalViewRepository openHouseInternalViewRepository(
        HouseTableRepository houseTableRepository,
        ViewCommitEngine viewCommitEngine,
        StorageSelector storageSelector,
        StorageManager storageManager) {
      return new OpenHouseInternalViewRepositoryImpl(
          houseTableRepository,
          viewCommitEngine,
          storageSelector,
          storageManager.getDefaultStorage());
    }
  }
}
