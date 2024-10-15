package com.linkedin.openhouse.javaclient;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.linkedin.openhouse.javaclient.builder.ClusteringSpecBuilder;
import com.linkedin.openhouse.javaclient.builder.TimePartitionSpecBuilder;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.client.model.Policies;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
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

  @Override
  protected String tableName() {
    return tableIdentifier.toString();
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  private static final String UPDATED_OPENHOUSE_POLICY_KEY = "updated.openhouse.policy";
  private static final String OPENHOUSE_TABLE_TYPE_KEY = "openhouse.tableType";
  private static final String POLICIES_KEY = "policies";
  static final String INITIAL_TABLE_VERSION = "INITIAL_VERSION";

  @Override
  public void doRefresh() {
    log.info("Calling doRefresh for table: {}", tableName());
    Optional<String> tableLocation =
        tableApi
            .getTableV1(tableIdentifier.namespace().toString(), tableIdentifier.name())
            .mapNotNull(GetTableResponseBody::getTableLocation)
            /*
             on 404 from table service, resume the stream as empty response.
             for any other error, surface it!
            */
            .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.empty())
            .onErrorResume(WebClientResponseException.BadRequest.class, e -> Mono.empty())
            .onErrorResume(
                WebClientResponseException.class,
                e -> Mono.error(new WebClientResponseWithMessageException(e)))
            .blockOptional();
    if (!tableLocation.isPresent() && currentMetadataLocation() != null) {
      throw new NoSuchTableException(
          "Cannot find table %s after refresh, maybe another process deleted it", tableName());
    }
    super.refreshFromMetadataLocation(tableLocation.orElse(null));
    log.debug("Calling doRefresh succeeded");
  }

  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    log.info("Calling doCommit for table: {}", tableName());
    boolean metadataUpdated = isMetadataUpdated(base, metadata);
    if (areSnapshotsUpdated(base, metadata)) {
      putSnapshots(base, metadata);
    } else if (metadataUpdated) {
      createUpdateTable(base, metadata);
    }
    log.debug("Calling doCommit succeeded");
  }

  private Boolean isMetadataUpdated(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return true;
    } else {
      return !base.schema().sameSchema(metadata.schema())
          || !base.properties().equals(metadata.properties())
          || !base.spec().equals(metadata.spec());
    }
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
    // set tableType from incoming metadata to createUpdateTableRequestBody
    if (metadata.properties() != null
        && metadata.properties().containsKey(OPENHOUSE_TABLE_TYPE_KEY)) {
      createUpdateTableRequestBody.setTableType(
          CreateUpdateTableRequestBody.TableTypeEnum.valueOf(
              metadata.properties().get(OPENHOUSE_TABLE_TYPE_KEY)));
    }

    return createUpdateTableRequestBody;
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

  private boolean areSnapshotsUpdated(TableMetadata base, TableMetadata newMetadata) {
    if (base == null) {
      return !newMetadata.snapshots().isEmpty();
    }
    return !base.snapshots().equals(newMetadata.snapshots())
        || !base.refs().equals(newMetadata.refs());
  }

  /**
   * A wrapper for a remote REST call to put snapshot.
   *
   * @param base the metadata before the snapshot was created
   * @param newMetadata metadata containing a new snapshot
   */
  private void putSnapshots(TableMetadata base, TableMetadata newMetadata) {
    IcebergSnapshotsRequestBody icebergSnapshotsRequestBody = new IcebergSnapshotsRequestBody();
    CreateUpdateTableRequestBody createUpdateTableRequestBody =
        constructMetadataRequestBody(base, newMetadata);
    icebergSnapshotsRequestBody.baseTableVersion(
        base == null ? INITIAL_TABLE_VERSION : base.metadataFileLocation());
    icebergSnapshotsRequestBody.jsonSnapshots(
        newMetadata.snapshots().stream().map(SnapshotParser::toJson).collect(Collectors.toList()));
    icebergSnapshotsRequestBody.snapshotRefs(
        newMetadata.refs().entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, e -> SnapshotRefParser.toJson(e.getValue()))));
    icebergSnapshotsRequestBody.createUpdateTableRequestBody(createUpdateTableRequestBody);

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
    } else if (e instanceof WebClientResponseException) {
      return Mono.error(new WebClientResponseWithMessageException((WebClientResponseException) e));
    } else {
      /**
       * This serves as a catch-all for any unexpected exceptions that could occur during doCommit,
       * (i.e) exceptions that are not WebClientResponseException. This is a conservative approach
       * to skip any unexpected cleanup that could occur when a commit aborts at the caller, thus
       * avoiding any potential data loss.
       */
      return Mono.error(new CommitStateUnknownException(e));
    }
  }
}
