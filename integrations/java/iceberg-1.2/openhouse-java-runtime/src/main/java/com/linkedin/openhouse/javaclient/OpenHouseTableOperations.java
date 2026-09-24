package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.linkedin.openhouse.javaclient.builder.ClusteringSpecBuilder;
import com.linkedin.openhouse.javaclient.builder.TimePartitionSpecBuilder;
import com.linkedin.openhouse.javaclient.exception.WebClientRequestWithMessageException;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.client.model.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.client.model.Policies;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

@Builder
@Slf4j
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class OpenHouseTableOperations extends BaseMetastoreTableOperations {

  @Getter(AccessLevel.PROTECTED)
  private TableIdentifier tableIdentifier;

  @Getter(AccessLevel.PROTECTED)
  private FileIO fileIO;

  @Getter(AccessLevel.PROTECTED)
  private TableApi tableApi;

  @Getter(AccessLevel.PROTECTED)
  private SnapshotApi snapshotApi;

  @Getter(AccessLevel.PROTECTED)
  private String cluster;

  /**
   * The per-table client {@code config} (Iceberg REST {@code LoadTableResponse.config} convention)
   * the OH server stamped onto the most recent table-load response (or {@code null} if none / not
   * yet refreshed). A final holder keeps Lombok's all-args constructor unchanged.
   */
  private final AtomicReference<Map<String, String>> config = new AtomicReference<>();

  // Replacement deltas are tied to the base used for staging, not a later transaction refresh.
  private final AtomicReference<TableMetadata> replacementBase = new AtomicReference<>();

  // Parsed staging metadata has no changes; retain initialization separately from user deltas.
  private final AtomicReference<List<MetadataUpdate>> createUpdates = new AtomicReference<>();

  void beginCreate(TableMetadata staged, Map<String, String> properties) {
    List<MetadataUpdate> updates = new ArrayList<>();
    // This is initial-version selection, not an upgrade from the client's default format.
    updates.add(new MetadataUpdate.UpgradeFormatVersion(staged.formatVersion()));
    updates.add(new MetadataUpdate.AssignUUID(staged.uuid()));
    staged
        .schemas()
        .forEach(
            schema -> updates.add(new MetadataUpdate.AddSchema(schema, staged.lastColumnId())));
    updates.add(new MetadataUpdate.SetCurrentSchema(staged.currentSchemaId()));
    staged.specs().forEach(spec -> updates.add(new MetadataUpdate.AddPartitionSpec(spec)));
    updates.add(new MetadataUpdate.SetDefaultPartitionSpec(staged.defaultSpecId()));
    staged.sortOrders().forEach(order -> updates.add(new MetadataUpdate.AddSortOrder(order)));
    updates.add(new MetadataUpdate.SetDefaultSortOrder(staged.defaultSortOrderId()));
    updates.add(new MetadataUpdate.SetLocation(staged.location()));
    updates.add(new MetadataUpdate.SetProperties(properties));
    createUpdates.set(Collections.unmodifiableList(updates));
  }

  void beginReplace(TableMetadata base) {
    replacementBase.set(base);
  }

  /**
   * The server-stamped per-table client config from the last {@code doRefresh}, or {@code null}
   * when absent. Subclasses read it to gate read-time behavior.
   */
  protected Map<String, String> currentConfig() {
    return config.get();
  }

  @Override
  protected String tableName() {
    return tableIdentifier.toString();
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  static final String UPDATED_OPENHOUSE_POLICY_KEY = "updated.openhouse.policy";
  private static final String OPENHOUSE_TABLE_TYPE_KEY = "openhouse.tableType";
  private static final String OPENHOUSE_CLUSTER_ID_KEY = "openhouse.clusterId";
  private static final String OPENHOUSE_IS_TABLE_REPLICATED_KEY = "openhouse.isTableReplicated";
  static final String POLICIES_KEY = "policies";
  static final String INITIAL_TABLE_VERSION = "INITIAL_VERSION";

  private static final ObjectReader TABLE_UPDATE_READER =
      ApiClient.createDefaultObjectMapper(null)
          .readerFor(new TypeReference<Map<String, Object>>() {});

  @Override
  public void doRefresh() {
    log.info("Calling doRefresh for table: {}", tableName());
    Optional<GetTableResponseBody> tableResponse =
        tableApi
            .getTableV1(tableIdentifier.namespace().toString(), tableIdentifier.name())
            /*
             on 404 from table service, resume the stream as empty response.
             for any other error, surface it!
            */
            .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.empty())
            .onErrorResume(WebClientResponseException.BadRequest.class, e -> Mono.empty())
            .onErrorResume(
                WebClientResponseException.class,
                e -> Mono.error(new WebClientResponseWithMessageException(e)))
            .onErrorResume(
                WebClientRequestException.class,
                e -> Mono.error(new WebClientRequestWithMessageException(e)))
            .blockOptional();
    // Capture the server-stamped per-table config so subclasses can gate read-time behavior via
    // currentConfig(); absent => null. Side-channel only: never sent back on writes.
    this.config.set(tableResponse.map(GetTableResponseBody::getConfig).orElse(null));
    Optional<String> tableLocation = tableResponse.map(GetTableResponseBody::getTableLocation);
    if (!tableLocation.isPresent() && currentMetadataLocation() != null) {
      throw new NoSuchTableException(
          "Cannot find table %s after refresh, maybe another process deleted it", tableName());
    }
    // Route the parse through loadMetadata() so subclasses can transform metadata as it loads;
    // (null, 20) preserves the stock refresh behavior.
    super.refreshFromMetadataLocation(tableLocation.orElse(null), null, 20, this::loadMetadata);
    log.debug("Calling doRefresh succeeded");
  }

  /**
   * Loads the table metadata at the given location. Defaults to the stock parser; subclasses may
   * override to transform the metadata (e.g. attach column defaults) as it loads.
   */
  protected TableMetadata loadMetadata(String metadataLocation) {
    return TableMetadataParser.read(io(), metadataLocation);
  }

  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    log.info("Calling doCommit for table: {}", tableName());
    List<Map<String, Object>> updates = serializeCommitUpdates(metadata);
    TableMetadata stagedBase = replacementBase.get();
    if (stagedBase != null
        && (base == null
            || !Objects.equals(stagedBase.metadataFileLocation(), base.metadataFileLocation()))) {
      throw new CommitFailedException("Cannot replace table: metadata changed after staging");
    }
    try {
      if (base == null && metadata.location() == null) {
        // Plain CREATE TABLE has no server-assigned location yet. Staged CTAS already has one.
        createUpdateTable(null, metadata);
      } else {
        CreateUpdateTableRequestBody request = constructMetadataRequestBody(base, metadata);
        request.replaceCommit(stagedBase != null);
        commitSnapshots(base, metadata, request, updates);
      }
    } catch (RuntimeException e) {
      if (e.getCause() instanceof InterruptedException) {
        log.error(
            String.format(
                "Unexpected runtime error occurred during doCommit: %s, with stacktrace: ",
                e.getClass().getSimpleName()),
            e);
        throw new CommitStateUnknownException(e);
      } else {
        throw e;
      }
    }
    // Staging intent applies only to the transaction that just published successfully.
    createUpdates.set(null);
    replacementBase.set(null);
    log.debug("Calling doCommit succeeded");
  }

  /**
   * A wrapper for a remote REST call to create/update table metadata.
   *
   * @param metadata The new metadata used for creation/update.
   */
  private void createUpdateTable(TableMetadata base, TableMetadata metadata) {
    CreateUpdateTableRequestBody createUpdateTableRequestBody =
        constructMetadataRequestBody(base, metadata);

    tableApi
        .updateTableV1(
            createUpdateTableRequestBody.getDatabaseId(),
            createUpdateTableRequestBody.getTableId(),
            createUpdateTableRequestBody)
        .onErrorResume(
            e ->
                handleCreateUpdateHttpError(
                    e,
                    createUpdateTableRequestBody.getDatabaseId(),
                    createUpdateTableRequestBody.getTableId()))
        .block();
  }

  protected CreateUpdateTableRequestBody constructMetadataRequestBody(
      TableMetadata base, TableMetadata metadata) {
    CreateUpdateTableRequestBody createUpdateTableRequestBody = new CreateUpdateTableRequestBody();
    createUpdateTableRequestBody.setBaseTableVersion(
        base == null ? INITIAL_TABLE_VERSION : base.metadataFileLocation());
    createUpdateTableRequestBody.setTableId(tableIdentifier.name());
    createUpdateTableRequestBody.setDatabaseId(tableIdentifier.namespace().toString());
    createUpdateTableRequestBody.setClusterId(cluster);
    createUpdateTableRequestBody.setSchema(SchemaParser.toJson(metadata.schema(), false));
    createUpdateTableRequestBody.setTimePartitioning(
        TimePartitionSpecBuilder.builderFor(metadata.schema(), metadata.spec()).build());
    createUpdateTableRequestBody.setClustering(
        ClusteringSpecBuilder.builderFor(metadata.schema(), metadata.spec()).build());
    createUpdateTableRequestBody.setPolicies(buildUpdatedPolicies(metadata));
    createUpdateTableRequestBody.setTableProperties(
        metadata.properties().entrySet().stream()
            .filter(entry -> !UPDATED_OPENHOUSE_POLICY_KEY.equals(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    createUpdateTableRequestBody.setSortOrder(SortOrderParser.toJson(metadata.sortOrder()));
    // set tableType from incoming metadata to createUpdateTableRequestBody
    if (metadata.properties().containsKey(OPENHOUSE_TABLE_TYPE_KEY)) {
      createUpdateTableRequestBody.setTableType(getTableType(base, metadata));
    }
    // TODO: consider allowing this for any table type, not just replica tables
    if (isMultiSchemaUpdateCommit(base, metadata)
        && createUpdateTableRequestBody.getTableType()
            == CreateUpdateTableRequestBody.TableTypeEnum.REPLICA_TABLE) {
      List<String> newIntermediateSchemas = new ArrayList<>();
      int startSchemaId = base == null ? 0 : base.currentSchemaId() + 1;
      for (int i = startSchemaId; i < metadata.currentSchemaId(); i++) {
        newIntermediateSchemas.add(SchemaParser.toJson(metadata.schemasById().get(i), false));
      }
      createUpdateTableRequestBody.setNewIntermediateSchemas(newIntermediateSchemas);
    }
    // If base table is a replicated table, retain the property from base table
    if (base != null && base.properties().containsKey(OPENHOUSE_IS_TABLE_REPLICATED_KEY)) {
      Map<String, String> newTblProperties = createUpdateTableRequestBody.getTableProperties();
      newTblProperties.put(
          OPENHOUSE_IS_TABLE_REPLICATED_KEY,
          base.properties().get(OPENHOUSE_IS_TABLE_REPLICATED_KEY));
      createUpdateTableRequestBody.setTableProperties(newTblProperties);
    }
    return createUpdateTableRequestBody;
  }

  /**
   * If request is coming from replication process, createUpdateTableRequestBody.tableType should be
   * REPLICA_TABLE Replication process requests are identified based on difference between table
   * types and cluster_id between base, metadata
   */
  @VisibleForTesting
  CreateUpdateTableRequestBody.TableTypeEnum getTableType(
      TableMetadata base, TableMetadata metadata) {
    if (base != null) {
      CreateUpdateTableRequestBody.TableTypeEnum baseTableType =
          CreateUpdateTableRequestBody.TableTypeEnum.valueOf(
              base.properties().get(OPENHOUSE_TABLE_TYPE_KEY));
      CreateUpdateTableRequestBody.TableTypeEnum metadataTableType =
          CreateUpdateTableRequestBody.TableTypeEnum.valueOf(
              metadata.properties().get(OPENHOUSE_TABLE_TYPE_KEY));
      // check if commit request is from replication case
      if (baseTableType == CreateUpdateTableRequestBody.TableTypeEnum.REPLICA_TABLE
          && metadataTableType == CreateUpdateTableRequestBody.TableTypeEnum.PRIMARY_TABLE
          && !base.properties()
              .get(OPENHOUSE_CLUSTER_ID_KEY)
              .equals(metadata.properties().get(OPENHOUSE_CLUSTER_ID_KEY))) {
        return baseTableType;
      }
    }
    return CreateUpdateTableRequestBody.TableTypeEnum.valueOf(
        metadata.properties().get(OPENHOUSE_TABLE_TYPE_KEY));
  }

  @VisibleForTesting
  Policies buildUpdatedPolicies(TableMetadata metadata) {
    Map<String, String> properties = metadata.properties();
    Policies policies =
        properties.containsKey(POLICIES_KEY)
            ? toPoliciesObject(properties.get(POLICIES_KEY))
            : null;
    if (!properties.containsKey(UPDATED_OPENHOUSE_POLICY_KEY)) {
      return policies;
    }
    Policies patchUpdatedPolicy = toPoliciesObject(properties.get(UPDATED_OPENHOUSE_POLICY_KEY));

    // Nothing the patch, patchUpdatedPolicy is the new Policy.
    if (policies == null) {
      return patchUpdatedPolicy;
    }

    // Update retention config
    if (patchUpdatedPolicy.getRetention() != null) {
      policies.setRetention(patchUpdatedPolicy.getRetention());
    }

    // Update sharing config
    if (patchUpdatedPolicy.getSharingEnabled() != null) {
      policies.sharingEnabled(patchUpdatedPolicy.getSharingEnabled());
    }
    // Update column policy tag config
    if (patchUpdatedPolicy.getColumnTags() != null) {
      if (policies.getColumnTags() != null) {
        policies
            .getColumnTags()
            .forEach(
                (k, v) ->
                    patchUpdatedPolicy
                        .getColumnTags()
                        .merge(k, v, (updatedSet, oldSet) -> updatedSet));
      }
      policies.setColumnTags(patchUpdatedPolicy.getColumnTags());
    }
    // Update replication config
    if (patchUpdatedPolicy.getReplication() != null) {
      policies.replication(patchUpdatedPolicy.getReplication());
    }
    // Update history config
    if (patchUpdatedPolicy.getHistory() != null) {
      policies.setHistory(patchUpdatedPolicy.getHistory());
    }

    return policies;
  }

  private Policies toPoliciesObject(String policiesString) throws JsonParseException {
    if (policiesString.length() != 0) {
      try {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.fromJson(policiesString, Policies.class);
      } catch (JsonParseException e) {
        throw new JsonParseException(
            "OpenHouse: Cannot convert policies string to policies object");
      }
    }
    return null;
  }

  protected boolean isMultiSchemaUpdateCommit(TableMetadata base, TableMetadata newMetadata) {
    return (base == null && newMetadata.currentSchemaId() > 0)
        || (base != null && newMetadata.currentSchemaId() > base.currentSchemaId() + 1);
  }

  /**
   * Builds an {@link IcebergSnapshotsRequestBody} from the given metadata and sends it to the
   * snapshot API.
   *
   * @param base the metadata before the snapshot was created
   * @param newMetadata metadata containing a new snapshot
   * @param createUpdateTableRequestBody the request body for the table metadata
   */
  private void commitSnapshots(
      TableMetadata base,
      TableMetadata newMetadata,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      List<Map<String, Object>> updates) {
    IcebergSnapshotsRequestBody icebergSnapshotsRequestBody = new IcebergSnapshotsRequestBody();
    icebergSnapshotsRequestBody.baseTableVersion(
        base == null ? INITIAL_TABLE_VERSION : base.metadataFileLocation());
    icebergSnapshotsRequestBody.jsonSnapshots(
        newMetadata.snapshots().stream().map(SnapshotParser::toJson).collect(Collectors.toList()));
    icebergSnapshotsRequestBody.snapshotRefs(
        newMetadata.refs().entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, e -> SnapshotRefParser.toJson(e.getValue()))));
    icebergSnapshotsRequestBody.createUpdateTableRequestBody(createUpdateTableRequestBody);
    icebergSnapshotsRequestBody.updates(updates);

    snapshotApi
        .putSnapshotsV1(
            createUpdateTableRequestBody.getDatabaseId(),
            createUpdateTableRequestBody.getTableId(),
            icebergSnapshotsRequestBody)
        .onErrorResume(
            e ->
                handleCreateUpdateHttpError(
                    e,
                    createUpdateTableRequestBody.getDatabaseId(),
                    createUpdateTableRequestBody.getTableId()))
        .block();
  }

  private List<Map<String, Object>> serializeCommitUpdates(TableMetadata metadata) {
    List<MetadataUpdate> initialization = createUpdates.get();
    if (initialization == null) {
      return serializeMetadataUpdates(metadata);
    }
    List<Map<String, Object>> serialized =
        new ArrayList<>(initialization.size() + metadata.changes().size());
    for (MetadataUpdate update : initialization) {
      serialized.add(tableUpdateObject(MetadataUpdateParser.toJson(update)));
    }
    for (MetadataUpdate update : metadata.changes()) {
      serialized.add(tableUpdateObject(MetadataUpdateParser.toJson(update)));
    }
    return serialized;
  }

  /**
   * The deltas this commit applies, as Iceberg REST {@code CommitTableRequest.updates[]} items.
   *
   * <p>{@link TableMetadata#changes()} is the same list every Iceberg REST catalog sends, and
   * {@code MetadataUpdateParser} emits the spec {@code TableUpdate} object. Reifying that JSON as a
   * {@link Map} (not a string) is what makes the request field an object array — the REST envelope
   * — rather than {@code string[]} of serialized JSON.
   *
   * <p>A ref-only operation such as {@code CREATE BRANCH b} is visible as a lone {@code
   * set-snapshot-ref} naming {@code b}, which no amount of inspecting the resulting snapshot list
   * can recover.
   *
   * <p>The list is authoritative: every action is serialized in order, an empty list remains empty,
   * and an unrecognized action fails the entire commit before any HTTP request is sent.
   */
  @VisibleForTesting
  static List<Map<String, Object>> serializeMetadataUpdates(TableMetadata newMetadata) {
    List<MetadataUpdate> changes = newMetadata.changes();
    List<Map<String, Object>> serialized = new ArrayList<>(changes.size());
    for (MetadataUpdate change : changes) {
      serialized.add(tableUpdateObject(MetadataUpdateParser.toJson(change)));
    }
    return serialized;
  }

  /**
   * Parses canonical Iceberg JSON with the same Jackson mapper used by the HTTP client. JSON
   * integers remain exact and floating-point tokens (including 1.0) remain floating point.
   */
  @VisibleForTesting
  static Map<String, Object> tableUpdateObject(String json) {
    try {
      return TABLE_UPDATE_READER.readValue(json);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot serialize metadata update", e);
    }
  }

  static Mono<GetTableResponseBody> handleCreateUpdateHttpError(
      Throwable e, String databaseId, String tableId) {
    if (e instanceof WebClientResponseException.NotFound) {
      return Mono.error(
          new NoSuchTableException(
              String.format("Table %s.%s doesn't exist, ", databaseId, tableId), e));
    } else if (e instanceof WebClientResponseException.Conflict) {
      WebClientResponseException casted = (WebClientResponseException) e;
      return Mono.error(
          new CommitFailedException(
              casted, casted.getStatusCode().value() + " , " + casted.getResponseBodyAsString()));

    } else if (e instanceof WebClientResponseException.GatewayTimeout
        || e instanceof WebClientResponseException.ServiceUnavailable
        || e instanceof WebClientResponseException.InternalServerError) {
      /**
       * This is done to avoid any data loss that could occur when a commit aborts at the caller
       * leads to deletion of iceberg metadata files.
       */
      WebClientResponseException casted = (WebClientResponseException) e;
      return Mono.error(new CommitStateUnknownException(casted.getResponseBodyAsString(), casted));
    } else if (e instanceof WebClientResponseException.BadRequest) {
      WebClientResponseException casted = (WebClientResponseException) e;
      return Mono.error(
          new BadRequestException(
              casted, casted.getStatusCode().value() + " , " + casted.getResponseBodyAsString()));
    } else if (e instanceof WebClientResponseException.NotImplemented) {
      return Mono.error(new WebClientResponseWithMessageException((WebClientResponseException) e));
    } else if (e instanceof WebClientResponseException
        && ((WebClientResponseException) e).getStatusCode().is4xxClientError()) {
      return Mono.error(new WebClientResponseWithMessageException((WebClientResponseException) e));
    } else {
      /**
       * This serves as a catch-all for any unexpected exceptions that could occur during doCommit,
       * (i.e) exceptions that are not WebClientResponseException. This is a conservative approach
       * to skip any unexpected cleanup that could occur when a commit aborts at the caller, thus
       * avoiding any potential data loss. {@link WebClientRequestException} is caught here which is
       * thrown when response is not completely received (even if the response is successful).
       */
      log.error(
          String.format(
              "Unexpected exception occurred during doCommit: %s, with stacktrace: ",
              e.getClass().getSimpleName()),
          e);
      return Mono.error(new CommitStateUnknownException(e));
    }
  }
}
