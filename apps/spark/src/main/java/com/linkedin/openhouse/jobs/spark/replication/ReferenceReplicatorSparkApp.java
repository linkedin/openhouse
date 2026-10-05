package com.linkedin.openhouse.jobs.spark.replication;

import com.linkedin.openhouse.client.ssl.TablesApiClientFactory;
import com.linkedin.openhouse.jobs.spark.replication.ReferenceReplicationDataPlane.CopyResult;
import com.linkedin.openhouse.jobs.spark.replication.ReferenceReplicationDataPlane.TableGeneration;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import com.linkedin.openhouse.tables.client.api.ReplicationStateControllerApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.ReplicationCheckpoint;
import com.linkedin.openhouse.tables.client.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.tables.client.model.ReplicationDestination;
import com.linkedin.openhouse.tables.client.model.ReplicationEdgeState;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/** One-shot OpenHouse-owned Spark replication reference job. */
@Slf4j
public final class ReferenceReplicatorSparkApp {
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(180);

  private final Config config;
  private final ReferenceReplicationDataPlane dataPlane;
  private final ReplicationStateControllerApi replicationApi;
  private final TableApi sourceTableApi;
  private final TableApi destinationTableApi;
  private final RetryTemplate retryTemplate = RetryUtil.getTablesApiRetryTemplate();

  ReferenceReplicatorSparkApp(
      Config config,
      ReferenceReplicationDataPlane dataPlane,
      ReplicationStateControllerApi replicationApi,
      TableApi sourceTableApi,
      TableApi destinationTableApi) {
    this.config = config;
    this.dataPlane = dataPlane;
    this.replicationApi = replicationApi;
    this.sourceTableApi = sourceTableApi;
    this.destinationTableApi = destinationTableApi;
  }

  public static void main(String[] args) {
    Config config = parseArgs(args);
    SparkSession spark =
        SparkSession.builder()
            .appName(ReferenceReplicatorSparkApp.class.getSimpleName())
            .getOrCreate();
    try {
      ApiClient sourceApiClient = createApiClient(config.sourceTablesApiUrl, config.token);
      ApiClient destinationApiClient =
          createApiClient(config.destinationTablesApiUrl, config.token);
      TableApi sourceTableApi = new TableApi(sourceApiClient);
      TableApi destinationTableApi = new TableApi(destinationApiClient);
      RetryTemplate metadataRetryTemplate = RetryUtil.getTablesApiRetryTemplate();
      ReferenceReplicationDataPlane.TableMetadataProvider metadataProvider =
          (catalogName, identifier) -> {
            TableApi tableApi =
                tableApiForCatalog(
                    catalogName,
                    config.sourceCatalog,
                    sourceTableApi,
                    config.destinationCatalog,
                    destinationTableApi);
            return metadataRetryTemplate.execute(
                (RetryCallback<GetTableResponseBody, RuntimeException>)
                    context ->
                        requireValue(
                            tableApi
                                .getTableV1(namespace(identifier), identifier.name())
                                .block(REQUEST_TIMEOUT),
                            "Tables API returned no catalog table"));
          };
      ReferenceReplicatorSparkApp app =
          new ReferenceReplicatorSparkApp(
              config,
              new ReferenceReplicationDataPlane(spark, metadataProvider),
              new ReplicationStateControllerApi(destinationApiClient),
              sourceTableApi,
              destinationTableApi);
      app.run();
    } finally {
      spark.stop();
    }
  }

  void run() {
    TableGeneration sourceGeneration =
        new TableGeneration(config.sourceTableUUID, config.sourceCreationTime);
    List<ReplicationEdgeState> edges =
        retry(
            context ->
                replicationApi
                    .getDestinationsV1(
                        config.sourceClusterId,
                        sourceGeneration.getTableUuid(),
                        sourceGeneration.getCreationTime())
                    .collectList()
                    .block(REQUEST_TIMEOUT));

    int processedEdges = 0;
    for (ReplicationEdgeState edge : edges) {
      ReplicationDestination destination =
          requireValue(edge.getDestination(), "Replication edge has no destination");
      if (!sameLocator(destination.getDestinationClusterId(), config.destinationClusterId)) {
        continue;
      }
      replicateEdge(sourceGeneration, destination);
      processedEdges++;
    }
    log.info(
        "Reference replication completed for source generation {}@{}; processed {} destination edges",
        sourceGeneration.getTableUuid(),
        sourceGeneration.getCreationTime(),
        processedEdges);
  }

  private void replicateEdge(TableGeneration sourceGeneration, ReplicationDestination destination) {
    requireEdgeIdentity(sourceGeneration, destination);
    TableGeneration destinationGeneration =
        new TableGeneration(
            requireValue(destination.getDestinationTableUUID(), "Destination UUID is missing"),
            requireValue(
                destination.getDestinationCreationTime(), "Destination creation time is missing"));

    TableIdentifier sourceIdentifier =
        resolveCurrentIdentifier(config.sourceCatalog, sourceGeneration, "source");
    TableIdentifier destinationIdentifier =
        resolveCurrentIdentifier(config.destinationCatalog, destinationGeneration, "destination");

    GetTableResponseBody sourceMetadata = getTable(sourceTableApi, sourceIdentifier, "source");
    requireTableGeneration(sourceMetadata, sourceGeneration, "source");
    requireTableCluster(sourceMetadata, config.sourceClusterId, "source");
    GetTableResponseBody destinationMetadata =
        getTable(destinationTableApi, destinationIdentifier, "destination");
    requireTableGeneration(destinationMetadata, destinationGeneration, "destination");
    requireTableCluster(destinationMetadata, config.destinationClusterId, "destination");
    requireReplicaTable(destinationMetadata, destinationIdentifier);

    ReplicationDestination currentDestination =
        updateLocatorsIfNeeded(
            destination,
            sourceIdentifier,
            destinationIdentifier,
            sourceGeneration,
            destinationGeneration);

    TableIdentifier desiredDestinationIdentifier =
        TableIdentifier.of(
            Namespace.of(destinationIdentifier.namespace().levels()), sourceIdentifier.name());
    if (!destinationIdentifier.equals(desiredDestinationIdentifier)) {
      dataPlane.renameReplica(
          config.destinationCatalog,
          destinationIdentifier,
          desiredDestinationIdentifier,
          destinationGeneration);
      destinationIdentifier = desiredDestinationIdentifier;
      currentDestination =
          updateLocatorsIfNeeded(
              currentDestination,
              sourceIdentifier,
              destinationIdentifier,
              sourceGeneration,
              destinationGeneration);
    }

    sourceMetadata = getTable(sourceTableApi, sourceIdentifier, "source");
    requireTableGeneration(sourceMetadata, sourceGeneration, "source");
    requireTableCluster(sourceMetadata, config.sourceClusterId, "source");
    destinationMetadata = getTable(destinationTableApi, destinationIdentifier, "destination");
    requireTableGeneration(destinationMetadata, destinationGeneration, "destination");
    requireTableCluster(destinationMetadata, config.destinationClusterId, "destination");
    requireReplicaTable(destinationMetadata, destinationIdentifier);
    String sourceTableVersion = requiredTableVersion(sourceMetadata, "source");

    Optional<ReplicationCheckpoint> checkpoint =
        getCheckpoint(sourceGeneration, destinationGeneration);
    long sourceSnapshotId = dataPlane.getCurrentSnapshotId(config.sourceCatalog, sourceIdentifier);
    if (isCheckpointCurrent(
        checkpoint,
        sourceTableVersion,
        sourceSnapshotId,
        destinationMetadata,
        destinationIdentifier)) {
      log.info(
          "Skipping replication for source {} because checkpoint revision {} is current",
          sourceIdentifier,
          checkpoint.get().getRevision());
      return;
    }

    CopyResult copyResult =
        dataPlane.copyLatestSnapshot(
            config.sourceCatalog,
            sourceIdentifier,
            config.destinationCatalog,
            destinationIdentifier,
            sourceGeneration,
            destinationGeneration);
    if (copyResult.getSourceSnapshotId() != sourceSnapshotId) {
      throw new IllegalStateException(
          "Source snapshot changed while replication was starting; checkpoint was not advanced");
    }

    GetTableResponseBody sourceAfterCopy = getTable(sourceTableApi, sourceIdentifier, "source");
    requireTableGeneration(sourceAfterCopy, sourceGeneration, "source after copy");
    requireTableCluster(sourceAfterCopy, config.sourceClusterId, "source after copy");
    if (!sourceTableVersion.equals(requiredTableVersion(sourceAfterCopy, "source after copy"))
        || dataPlane.getCurrentSnapshotId(config.sourceCatalog, sourceIdentifier)
            != copyResult.getSourceSnapshotId()) {
      throw new IllegalStateException(
          "Source changed during replication; checkpoint was not advanced");
    }

    GetTableResponseBody destinationAfterCopy =
        getTable(destinationTableApi, destinationIdentifier, "destination after copy");
    requireTableGeneration(destinationAfterCopy, destinationGeneration, "destination after copy");
    requireTableCluster(
        destinationAfterCopy, config.destinationClusterId, "destination after copy");
    requireReplicaTable(destinationAfterCopy, destinationIdentifier);
    String destinationTableVersion = requiredTableVersion(destinationAfterCopy, "destination");

    ReplicationCheckpointUpdate update =
        new ReplicationCheckpointUpdate()
            .sourceClusterId(config.sourceClusterId)
            .sourceTableUUID(sourceGeneration.getTableUuid())
            .sourceCreationTime(sourceGeneration.getCreationTime())
            .destinationClusterId(config.destinationClusterId)
            .destinationTableUUID(destinationGeneration.getTableUuid())
            .destinationCreationTime(destinationGeneration.getCreationTime())
            .sourceDatabaseId(namespace(sourceIdentifier))
            .sourceTableId(sourceIdentifier.name())
            .destinationDatabaseId(namespace(destinationIdentifier))
            .destinationTableId(destinationIdentifier.name())
            .expectedRevision(checkpointRevision(checkpoint))
            .sourceTableVersion(sourceTableVersion)
            .sourceSnapshotId(copyResult.getSourceSnapshotId())
            .destinationSnapshotId(copyResult.getDestinationSnapshotId())
            .destinationTableVersion(destinationTableVersion);
    retry(context -> replicationApi.advanceCheckpointV1(update).block(REQUEST_TIMEOUT));
    log.info(
        "Advanced replication checkpoint for {} to source snapshot {} and destination snapshot {}",
        sourceIdentifier,
        copyResult.getSourceSnapshotId(),
        copyResult.getDestinationSnapshotId());
  }

  private ReplicationDestination updateLocatorsIfNeeded(
      ReplicationDestination current,
      TableIdentifier sourceIdentifier,
      TableIdentifier destinationIdentifier,
      TableGeneration sourceGeneration,
      TableGeneration destinationGeneration) {
    boolean sourceLocatorChanged =
        !sameLocator(current.getSourceDatabaseId(), namespace(sourceIdentifier))
            || !sameLocator(current.getSourceTableId(), sourceIdentifier.name());
    boolean destinationLocatorChanged =
        !sameLocator(current.getDestinationDatabaseId(), namespace(destinationIdentifier))
            || !sameLocator(current.getDestinationTableId(), destinationIdentifier.name());
    if (!sourceLocatorChanged && !destinationLocatorChanged) {
      return current;
    }
    Long expectedVersion =
        requireValue(current.getVersion(), "Destination locator version is missing");
    ReplicationDestination updated =
        new ReplicationDestination()
            .sourceClusterId(config.sourceClusterId)
            .sourceTableUUID(sourceGeneration.getTableUuid())
            .sourceCreationTime(sourceGeneration.getCreationTime())
            .sourceDatabaseId(namespace(sourceIdentifier))
            .sourceTableId(sourceIdentifier.name())
            .destinationClusterId(config.destinationClusterId)
            .destinationTableUUID(destinationGeneration.getTableUuid())
            .destinationCreationTime(destinationGeneration.getCreationTime())
            .destinationDatabaseId(namespace(destinationIdentifier))
            .destinationTableId(destinationIdentifier.name())
            .expectedVersion(expectedVersion);
    return retry(
        context ->
            requireValue(
                replicationApi.putDestinationV1(updated).block(REQUEST_TIMEOUT),
                "Destination locator update returned no response"));
  }

  private Optional<ReplicationCheckpoint> getCheckpoint(
      TableGeneration sourceGeneration, TableGeneration destinationGeneration) {
    try {
      return Optional.of(
          retry(
              context ->
                  requireValue(
                      replicationApi
                          .getCheckpointV1(
                              config.sourceClusterId,
                              sourceGeneration.getTableUuid(),
                              sourceGeneration.getCreationTime(),
                              config.destinationClusterId,
                              destinationGeneration.getTableUuid(),
                              destinationGeneration.getCreationTime())
                          .block(REQUEST_TIMEOUT),
                      "Checkpoint API returned no response")));
    } catch (WebClientResponseException e) {
      if (e.getStatusCode().value() == 404) {
        return Optional.empty();
      }
      throw e;
    }
  }

  private boolean isCheckpointCurrent(
      Optional<ReplicationCheckpoint> checkpoint,
      String sourceTableVersion,
      long sourceSnapshotId,
      GetTableResponseBody destinationMetadata,
      TableIdentifier destinationIdentifier) {
    if (checkpoint.isEmpty()) {
      return false;
    }
    ReplicationCheckpoint current = checkpoint.get();
    OptionalLong destinationSnapshotId =
        dataPlane.findCurrentSnapshotId(config.destinationCatalog, destinationIdentifier);
    return Objects.equals(current.getSourceTableVersion(), sourceTableVersion)
        && Objects.equals(current.getSourceSnapshotId(), sourceSnapshotId)
        && Objects.equals(
            current.getDestinationTableVersion(),
            requiredTableVersion(destinationMetadata, "destination"))
        && current.getDestinationSnapshotId() != null
        && destinationSnapshotId.isPresent()
        && current.getDestinationSnapshotId() == destinationSnapshotId.getAsLong();
  }

  private long checkpointRevision(Optional<ReplicationCheckpoint> checkpoint) {
    if (checkpoint.isEmpty()) {
      return 0L;
    }
    return requireValue(checkpoint.get().getRevision(), "Checkpoint revision is missing");
  }

  private GetTableResponseBody getTable(
      TableApi tableApi, TableIdentifier identifier, String role) {
    return retry(
        context ->
            requireValue(
                tableApi
                    .getTableV1(namespace(identifier), identifier.name())
                    .block(REQUEST_TIMEOUT),
                "Tables API returned no " + role + " table"));
  }

  private TableIdentifier resolveCurrentIdentifier(
      String catalogName, TableGeneration generation, String role) {
    return dataPlane
        .findTableByGeneration(catalogName, generation)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Unable to resolve current "
                        + role
                        + " catalog locator for generation "
                        + generation));
  }

  private void requireEdgeIdentity(
      TableGeneration sourceGeneration, ReplicationDestination destination) {
    if (!sameLocator(destination.getSourceClusterId(), config.sourceClusterId)
        || !Objects.equals(destination.getSourceTableUUID(), sourceGeneration.getTableUuid())
        || !Objects.equals(destination.getSourceCreationTime(), sourceGeneration.getCreationTime())
        || !sameLocator(destination.getDestinationClusterId(), config.destinationClusterId)) {
      throw new IllegalStateException("Replication API returned an edge with mismatched identity");
    }
  }

  private static void requireTableGeneration(
      GetTableResponseBody table, TableGeneration expected, String role) {
    if (!Objects.equals(table.getTableUUID(), expected.getTableUuid())
        || !Objects.equals(table.getCreationTime(), expected.getCreationTime())) {
      throw new IllegalStateException(
          "Tables API returned a different " + role + " table generation than the catalog");
    }
  }

  private static void requireTableCluster(
      GetTableResponseBody table, String expectedClusterId, String role) {
    if (!sameLocator(table.getClusterId(), expectedClusterId)) {
      throw new IllegalStateException(
          "Tables API returned a different " + role + " cluster than configured");
    }
  }

  private static void requireReplicaTable(GetTableResponseBody table, TableIdentifier identifier) {
    if (table.getTableType() != GetTableResponseBody.TableTypeEnum.REPLICA_TABLE) {
      throw new IllegalArgumentException(
          "Replication destination is not a REPLICA_TABLE: " + identifier);
    }
  }

  private static String requiredTableVersion(GetTableResponseBody table, String role) {
    return requireValue(table.getTableVersion(), role + " table version is missing");
  }

  private static String namespace(TableIdentifier identifier) {
    if (identifier.namespace().levels().length != 1) {
      throw new IllegalArgumentException(
          "OpenHouse replication supports one-level database namespaces: " + identifier);
    }
    return identifier.namespace().level(0);
  }

  private static TableApi tableApiForCatalog(
      String catalogName,
      String sourceCatalogName,
      TableApi sourceTableApi,
      String destinationCatalogName,
      TableApi destinationTableApi) {
    if (sourceCatalogName.equals(catalogName)) {
      return sourceTableApi;
    }
    if (destinationCatalogName.equals(catalogName)) {
      return destinationTableApi;
    }
    throw new IllegalArgumentException(
        "No Tables API is configured for Spark catalog " + catalogName);
  }

  private static boolean sameLocator(String left, String right) {
    return left != null && right != null && left.equalsIgnoreCase(right);
  }

  private static <T> T requireValue(T value, String message) {
    if (value == null) {
      throw new IllegalStateException(message);
    }
    return value;
  }

  private <T> T retry(RetryCallback<T, RuntimeException> callback) {
    return retryTemplate.execute(callback);
  }

  private static ApiClient createApiClient(String basePath, String token) {
    try {
      return TablesApiClientFactory.getInstance().createApiClient(basePath, token, null);
    } catch (MalformedURLException | SSLException e) {
      throw new IllegalArgumentException("Unable to initialize the Tables API client", e);
    }
  }

  private static Config parseArgs(String[] args) {
    Options options = new Options();
    addRequiredOption(options, "sourceClusterId", "Source cluster identity");
    addRequiredOption(options, "sourceTableUUID", "Stable source table UUID");
    addRequiredOption(options, "sourceCreationTime", "Stable source table creation time");
    addRequiredOption(options, "sourceCatalog", "Spark catalog name for the source cluster");
    addRequiredOption(options, "sourceTablesApiUrl", "Source Tables API base URL");
    addRequiredOption(options, "destinationClusterId", "Destination cluster identity");
    addRequiredOption(
        options, "destinationCatalog", "Spark catalog name for the destination cluster");
    addRequiredOption(options, "destinationTablesApiUrl", "Destination Tables API base URL");
    options.addOption(new Option(null, "token", true, "Tables API authentication token"));

    try {
      CommandLine commandLine = new DefaultParser().parse(options, args);
      return new Config(
          commandLine.getOptionValue("sourceClusterId"),
          commandLine.getOptionValue("sourceTableUUID"),
          Long.parseLong(commandLine.getOptionValue("sourceCreationTime")),
          commandLine.getOptionValue("sourceCatalog"),
          commandLine.getOptionValue("sourceTablesApiUrl"),
          commandLine.getOptionValue("destinationClusterId"),
          commandLine.getOptionValue("destinationCatalog"),
          commandLine.getOptionValue("destinationTablesApiUrl"),
          commandLine.getOptionValue("token"));
    } catch (org.apache.commons.cli.ParseException | NumberFormatException e) {
      throw new IllegalArgumentException("Invalid reference replication arguments", e);
    }
  }

  private static void addRequiredOption(Options options, String name, String description) {
    Option option = new Option(null, name, true, description);
    option.setRequired(true);
    options.addOption(option);
  }

  static final class Config {
    private final String sourceClusterId;
    private final String sourceTableUUID;
    private final long sourceCreationTime;
    private final String sourceCatalog;
    private final String sourceTablesApiUrl;
    private final String destinationClusterId;
    private final String destinationCatalog;
    private final String destinationTablesApiUrl;
    private final String token;

    Config(
        String sourceClusterId,
        String sourceTableUUID,
        long sourceCreationTime,
        String sourceCatalog,
        String sourceTablesApiUrl,
        String destinationClusterId,
        String destinationCatalog,
        String destinationTablesApiUrl,
        String token) {
      if (sourceCatalog.equals(destinationCatalog)) {
        throw new IllegalArgumentException(
            "Source and destination Spark catalog names must be distinct");
      }
      this.sourceClusterId = sourceClusterId;
      this.sourceTableUUID = sourceTableUUID;
      this.sourceCreationTime = sourceCreationTime;
      this.sourceCatalog = sourceCatalog;
      this.sourceTablesApiUrl = sourceTablesApiUrl;
      this.destinationClusterId = destinationClusterId;
      this.destinationCatalog = destinationCatalog;
      this.destinationTablesApiUrl = destinationTablesApiUrl;
      this.token = token;
    }
  }
}
