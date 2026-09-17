package com.linkedin.openhouse.javaclient;

import static org.mockito.Mockito.*;

import com.linkedin.openhouse.gen.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.gen.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.History;
import com.linkedin.openhouse.gen.tables.client.model.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.gen.tables.client.model.Policies;
import com.linkedin.openhouse.gen.tables.client.model.PolicyTag;
import com.linkedin.openhouse.gen.tables.client.model.Retention;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.node.ArrayNode;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.openhouse.relocated.org.springframework.http.HttpHeaders;
import com.linkedin.openhouse.relocated.org.springframework.http.HttpMethod;
import com.linkedin.openhouse.relocated.org.springframework.http.HttpStatus;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientRequestException;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.Tasks;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

public class OpenHouseTableOperationsTest {

  /**
   * A minimal overwrite for {@link OpenHouseTableOperations} to avoid complicated mocking for the
   * constructMetadataRequestBody method.
   *
   * <p>Please refrain from enlarging the overwrite scope.
   */
  private class OpenHouseTableOperationsForTest extends OpenHouseTableOperations {
    OpenHouseTableOperationsForTest(
        TableIdentifier tableIdentifier,
        FileIO fileIO,
        TableApi tableApi,
        SnapshotApi snapshotApi,
        String cluster) {
      super(tableIdentifier, fileIO, tableApi, snapshotApi, cluster);
    }

    @Override
    protected CreateUpdateTableRequestBody constructMetadataRequestBody(
        TableMetadata base, TableMetadata metadata) {
      CreateUpdateTableRequestBody dummyBody = new CreateUpdateTableRequestBody();
      dummyBody.setDatabaseId("db");
      dummyBody.setTableId("tbl");
      return dummyBody;
    }
  }

  @Test
  public void testCreateUpdateTableErrorHandle() {
    TableIdentifier id = TableIdentifier.of("a", "b");
    FileIO mockFileIO = mock(FileIO.class);
    TableApi mockTableApi = mock(TableApi.class);
    SnapshotApi mockSnapshotApi = mock(SnapshotApi.class);
    OpenHouseTableOperationsForTest openHouseTableOperations =
        new OpenHouseTableOperationsForTest(
            id, mockFileIO, mockTableApi, mockSnapshotApi, "cluster");

    TableMetadata metadata = mock(TableMetadata.class);
    TableMetadata base = mock(TableMetadata.class);

    // ensure the metadata-comparison triggers
    Schema mockSchemaX = mock(Schema.class);
    Schema mockSchemaY = mock(Schema.class);
    when(metadata.schema()).thenReturn(mockSchemaX);
    when(base.schema()).thenReturn(mockSchemaY);
    Map<String, String> propsBase = ImmutableMap.of();
    Map<String, String> propsMeta = ImmutableMap.of("a", "b");
    when(metadata.properties()).thenReturn(propsMeta);
    when(base.properties()).thenReturn(propsBase);

    // ensure this is not a snapshot change
    List<Snapshot> snapshotList = Lists.newArrayList();
    when(metadata.snapshots()).thenReturn(snapshotList);
    when(base.snapshots()).thenReturn(snapshotList);

    String message = "Column default for field 7 cannot be resolved (100%).";
    String code = "COLUMN_DEFAULT_INVALID_VALUE";
    String requestId = "column-default-request-17";
    String body =
        new ObjectMapper()
            .createObjectNode()
            .put("message", message)
            .put("code", code)
            .put("requestId", requestId)
            .put("retryable", false)
            .put("stacktrace", "private-stacktrace-marker")
            .put("cause", "private-cause-marker")
            .toString();

    Map<HttpStatus, Class<? extends RuntimeException>> expected = new HashMap<>();
    expected.put(HttpStatus.BAD_REQUEST, BadRequestException.class);
    expected.put(HttpStatus.CONFLICT, CommitFailedException.class);
    expected.put(HttpStatus.NOT_FOUND, NoSuchTableException.class);
    expected.put(HttpStatus.METHOD_NOT_ALLOWED, WebClientResponseWithMessageException.class);
    expected.put(HttpStatus.NOT_IMPLEMENTED, WebClientResponseWithMessageException.class);
    // These classes prohibit Iceberg's unsafe cleanup after an ambiguous commit. The response's
    // retryable flag or human message must not weaken that guarantee.
    expected.put(HttpStatus.INTERNAL_SERVER_ERROR, CommitStateUnknownException.class);
    expected.put(HttpStatus.BAD_GATEWAY, CommitStateUnknownException.class);
    expected.put(HttpStatus.SERVICE_UNAVAILABLE, CommitStateUnknownException.class);
    expected.put(HttpStatus.GATEWAY_TIMEOUT, CommitStateUnknownException.class);
    for (Map.Entry<HttpStatus, Class<? extends RuntimeException>> entry : expected.entrySet()) {
      WebClientResponseException response = httpError(entry.getKey(), body);
      when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
          .thenReturn(Mono.error(response));
      RuntimeException failure =
          Assertions.assertThrows(
              entry.getValue(), () -> openHouseTableOperations.doCommit(base, metadata));
      Assertions.assertEquals(entry.getValue(), failure.getClass());
      Assertions.assertSame(response, failure.getCause());
      if (entry.getKey() != HttpStatus.NOT_FOUND) {
        Assertions.assertTrue(failure.getMessage().contains(message));
        Assertions.assertTrue(failure.getMessage().contains(code));
        Assertions.assertTrue(failure.getMessage().contains(requestId));
      }
      Assertions.assertFalse(failure.getMessage().contains("private-stacktrace-marker"));
      Assertions.assertFalse(failure.getMessage().contains("private-cause-marker"));
      Assertions.assertFalse(failure.getMessage().contains("\"message\":"));
      Assertions.assertFalse(failure.getMessage().contains("\"retryable\":"));
    }

    WebClientRequestException transportFailure =
        new WebClientRequestException(
            new IOException("Connection closed before response completed"),
            HttpMethod.PUT,
            URI.create("https://openhouse.invalid/v1/databases/db/tables/tbl"),
            HttpHeaders.EMPTY);
    when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(transportFailure));
    CommitStateUnknownException failure =
        Assertions.assertThrows(
            CommitStateUnknownException.class,
            () -> openHouseTableOperations.doCommit(base, metadata));
    Assertions.assertEquals(CommitStateUnknownException.class, failure.getClass());
    Assertions.assertSame(transportFailure, failure.getCause());
  }

  @Test
  public void testUnrecognizedErrorBodiesUseHttpStatusWithoutDiagnostics() {
    List<String> bodies =
        Arrays.asList(
            "<html>private-diagnostic-marker</html>",
            "{\"message\":\"private-diagnostic-marker\"",
            "{\"message\":\"private-diagnostic-marker\"} trailing-data",
            "{\"cause\":\"private-diagnostic-marker\",\"stacktrace\":\"private-diagnostic-marker\"}",
            "{\"message\":{\"private-diagnostic-marker\":true},\"code\":[],\"requestId\":42}",
            "[\"private-diagnostic-marker\"]",
            "null",
            "");
    for (String body : bodies) {
      WebClientResponseException response = httpError(HttpStatus.BAD_REQUEST, body);
      BadRequestException failure =
          Assertions.assertThrows(
              BadRequestException.class,
              () ->
                  OpenHouseTableOperations.handleCreateUpdateHttpError(response, "db", "tbl")
                      .block());
      Assertions.assertEquals(BadRequestException.class, failure.getClass());
      Assertions.assertSame(response, failure.getCause());
      Assertions.assertTrue(failure.getMessage().contains("400"));
      Assertions.assertTrue(
          failure.getMessage().contains(HttpStatus.BAD_REQUEST.getReasonPhrase()));
      Assertions.assertFalse(failure.getMessage().contains("private-diagnostic-marker"));
      Assertions.assertFalse(failure.getMessage().contains("trailing-data"));
      Assertions.assertFalse(failure.getMessage().contains("{"));
      Assertions.assertFalse(failure.getMessage().contains("<html>"));
    }
  }

  @Test
  public void testLegacyAndPartialErrorEnvelopes() {
    WebClientResponseException legacy =
        httpError(
            HttpStatus.CONFLICT,
            "{\"message\":\"Concurrent write for field 17 (100%).\",\"cause\":\"private-diagnostic-marker\"}");
    CommitFailedException conflict =
        Assertions.assertThrows(
            CommitFailedException.class,
            () ->
                OpenHouseTableOperations.handleCreateUpdateHttpError(legacy, "db", "tbl").block());
    Assertions.assertTrue(conflict.getMessage().contains("Concurrent write for field 17 (100%)."));
    Assertions.assertFalse(conflict.getMessage().contains("private-diagnostic-marker"));
    Assertions.assertSame(legacy, conflict.getCause());

    WebClientResponseException partial =
        httpError(
            HttpStatus.SERVICE_UNAVAILABLE,
            "{\"message\":null,\"code\":\"COLUMN_DEFAULT_UNAVAILABLE\",\"requestId\":\"request-31\"}");
    CommitStateUnknownException unavailable =
        Assertions.assertThrows(
            CommitStateUnknownException.class,
            () ->
                OpenHouseTableOperations.handleCreateUpdateHttpError(partial, "db", "tbl").block());
    Assertions.assertEquals(CommitStateUnknownException.class, unavailable.getClass());
    Assertions.assertTrue(unavailable.getMessage().contains("503"));
    Assertions.assertTrue(unavailable.getMessage().contains("COLUMN_DEFAULT_UNAVAILABLE"));
    Assertions.assertTrue(unavailable.getMessage().contains("request-31"));
    Assertions.assertFalse(unavailable.getMessage().contains("null"));
    Assertions.assertSame(partial, unavailable.getCause());
  }

  @Test
  public void testUnavailableCommitPreservesPendingManifests(@TempDir Path directory)
      throws IOException {
    java.nio.file.Files.createDirectory(directory.resolve("metadata"));
    TableMetadata initial =
        TableMetadata.newTableMetadata(
            new Schema(NestedField.optional(1, "id", Types.IntegerType.get())),
            PartitionSpec.unpartitioned(),
            directory.toString(),
            Collections.emptyMap());
    String metadataLocation = directory.resolve("initial.metadata.json").toString();
    TableMetadataParser.overwrite(initial, Files.localOutput(metadataLocation));
    GetTableResponseBody tableResponse = mock(GetTableResponseBody.class);
    when(tableResponse.getTableLocation()).thenReturn(metadataLocation);
    TableApi tableApi = mock(TableApi.class);
    when(tableApi.getTableV1(anyString(), anyString())).thenReturn(Mono.just(tableResponse));

    WebClientResponseException response =
        httpError(
            HttpStatus.SERVICE_UNAVAILABLE,
            "{\"message\":\"Dependency unavailable; retry later.\","
                + "\"code\":\"COLUMN_DEFAULT_UNAVAILABLE\",\"requestId\":\"request-57\",\"retryable\":true}");
    SnapshotApi snapshotApi = mock(SnapshotApi.class);
    when(snapshotApi.putSnapshotsV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(response));
    FileIO fileIO = spy(localFileIO());
    OpenHouseTableOperations ops =
        new OpenHouseTableOperationsForTest(
            TableIdentifier.of("db", "tbl"), fileIO, tableApi, snapshotApi, "cluster");
    AppendFiles append =
        new BaseTable(ops, "db.tbl")
            .newFastAppend()
            .appendFile(
                DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(directory.resolve("data.parquet").toString())
                    .withRecordCount(1)
                    .withFileSizeInBytes(10)
                    .build());
    Snapshot pending = append.apply();
    String manifest = pending.allManifests(fileIO).get(0).path();
    Assertions.assertTrue(fileIO.newInputFile(manifest).exists());
    Assertions.assertTrue(fileIO.newInputFile(pending.manifestListLocation()).exists());

    CommitStateUnknownException failure =
        Assertions.assertThrows(CommitStateUnknownException.class, append::commit);
    Assertions.assertEquals(CommitStateUnknownException.class, failure.getClass());
    Assertions.assertSame(response, failure.getCause());
    Assertions.assertTrue(failure.getMessage().contains("COLUMN_DEFAULT_UNAVAILABLE"));
    Assertions.assertTrue(failure.getMessage().contains("request-57"));
    Assertions.assertTrue(fileIO.newInputFile(manifest).exists());
    Assertions.assertTrue(fileIO.newInputFile(pending.manifestListLocation()).exists());
    verify(fileIO, never()).deleteFile(anyString());
    verify(snapshotApi, times(1)).putSnapshotsV1(anyString(), anyString(), any());
  }

  private static WebClientResponseException httpError(HttpStatus status, String body) {
    return WebClientResponseException.create(
        status.value(),
        status.getReasonPhrase(),
        HttpHeaders.EMPTY,
        body.getBytes(StandardCharsets.UTF_8),
        StandardCharsets.UTF_8);
  }

  @Test
  public void testCreateUpdateTableInterruptedErrorHandle() throws InterruptedException {
    TableIdentifier id = TableIdentifier.of("a", "b");
    FileIO mockFileIO = mock(FileIO.class);
    TableApi mockTableApi = mock(TableApi.class);
    SnapshotApi mockSnapshotApi = mock(SnapshotApi.class);
    OpenHouseTableOperationsForTest openHouseTableOperations =
        new OpenHouseTableOperationsForTest(
            id, mockFileIO, mockTableApi, mockSnapshotApi, "cluster");

    TableMetadata metadata = mock(TableMetadata.class);
    TableMetadata base = mock(TableMetadata.class);

    // ensure the metadata-comparison triggers
    Schema mockSchemaX = mock(Schema.class);
    Schema mockSchemaY = mock(Schema.class);
    when(metadata.schema()).thenReturn(mockSchemaX);
    when(base.schema()).thenReturn(mockSchemaY);
    Map<String, String> propsBase = ImmutableMap.of();
    Map<String, String> propsMeta = ImmutableMap.of("a", "b");
    when(metadata.properties()).thenReturn(propsMeta);
    when(base.properties()).thenReturn(propsBase);

    // ensure this is a snapshot change
    List<Snapshot> snapshotList = Lists.newArrayList();
    List<Snapshot> snapshotList1 = Lists.newArrayList();
    when(metadata.snapshots()).thenReturn(snapshotList);
    when(base.snapshots()).thenReturn(snapshotList1);

    // Simulates a long-running operation
    when(mockTableApi.updateTableV1(anyString(), anyString(), any())).thenReturn(Mono.never());

    // Interrupt the current thread before calling .block()
    // When .block() is called on response mono from snapshotApi, it detects that the thread is
    // interrupted
    // and throws a RuntimeException wrapping an InterruptedException
    Thread.currentThread().interrupt();

    Assertions.assertThrows(
        CommitStateUnknownException.class, () -> openHouseTableOperations.doCommit(base, metadata));
    // ensure that the thread interrupt is cleared for other tests
    Assertions.assertFalse(Thread.interrupted());
  }

  @Test
  public void testNoPoliciesInMetadata() {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(Collections.emptyMap());
    Policies updatedPolicies = mock(OpenHouseTableOperations.class).buildUpdatedPolicies(metadata);
    Assertions.assertNull(updatedPolicies);
  }

  @Test
  public void testPoliciesInMetadataNoUpdate() {
    Map<String, String> props = new HashMap<>();
    props.put("policies", "{\"retention\": {\"count\": \"1\", \"granularity\": \"DAY\"}}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertEquals(1, updatedPolicies.getRetention().getCount());
    Assertions.assertEquals(
        Retention.GranularityEnum.DAY, updatedPolicies.getRetention().getGranularity());
  }

  @Test
  public void testNoPoliciesButUpdateExists() {
    Map<String, String> props = new HashMap<>();
    props.put("updated.openhouse.policy", "{\"sharingEnabled\": true}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertTrue(updatedPolicies.getSharingEnabled().booleanValue());
  }

  @Test
  public void testPoliciesExistUpdateExist() {
    Map<String, String> props = new HashMap<>();
    props.put("policies", "{\"retention\": {\"count\": \"1\", \"granularity\": \"DAY\"}}");
    props.put("updated.openhouse.policy", "{\"sharingEnabled\": true}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertTrue(updatedPolicies.getSharingEnabled().booleanValue());
    Assertions.assertEquals(1, updatedPolicies.getRetention().getCount());
    Assertions.assertEquals(
        Retention.GranularityEnum.DAY, updatedPolicies.getRetention().getGranularity());
  }

  @Test
  public void testPoliciesSharingAndRetentionUpdate() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "policies",
        "{\"retention\": {\"count\": \"1\", \"granularity\": \"DAY\"}, \"sharingEnabled\": true}");
    props.put(
        "updated.openhouse.policy",
        "{\"retention\": {\"count\": \"5\", \"granularity\": \"HOUR\"}, \"sharingEnabled\": false}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertFalse(updatedPolicies.getSharingEnabled().booleanValue());
    Assertions.assertEquals(5, updatedPolicies.getRetention().getCount());
    Assertions.assertEquals(
        Retention.GranularityEnum.HOUR, updatedPolicies.getRetention().getGranularity());
  }

  @Test
  public void testNoColumnPolicyTagsButUpdateExists() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "updated.openhouse.policy", "{ \"columnTags\": " + "{ \"col1\": {\"tags\": [\"PII\"]}} }");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Set<PolicyTag.TagsEnum> tags =
        new HashSet<PolicyTag.TagsEnum>(Arrays.asList(PolicyTag.TagsEnum.PII));
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertTrue(updatedPolicies.getColumnTags().containsKey("col1"));
    Assertions.assertEquals(tags, updatedPolicies.getColumnTags().get("col1").getTags());
  }

  @Test
  public void testColumnPolicyTagsExistUpdateExists() {
    Map<String, String> props = new HashMap<>();
    props.put("policies", "{ \"columnTags\": " + "{ \"col1\": {\"tags\": [\"PII\"]}} }");
    props.put(
        "updated.openhouse.policy", "{ \"columnTags\": " + "{ \"col2\": {\"tags\": [\"HC\"]}} }");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Set<PolicyTag.TagsEnum> tagPII =
        new HashSet<PolicyTag.TagsEnum>(Arrays.asList(PolicyTag.TagsEnum.PII));
    Set<PolicyTag.TagsEnum> tagHC =
        new HashSet<PolicyTag.TagsEnum>(Arrays.asList(PolicyTag.TagsEnum.HC));
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertTrue(updatedPolicies.getColumnTags().containsKey("col1"));
    Assertions.assertEquals(tagPII, updatedPolicies.getColumnTags().get("col1").getTags());
    Assertions.assertTrue(updatedPolicies.getColumnTags().containsKey("col2"));
    Assertions.assertEquals(tagHC, updatedPolicies.getColumnTags().get("col2").getTags());
  }

  @Test
  public void testColumnPolicyTagsExistUpdateExistingPolicyTags() {
    Map<String, String> props = new HashMap<>();
    props.put("policies", "{ \"columnTags\": " + "{ \"col1\": {\"tags\": [\"PII\"]}} }");
    props.put(
        "updated.openhouse.policy", "{ \"columnTags\": " + "{ \"col1\": {\"tags\": [\"HC\"]}} }");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Set<PolicyTag.TagsEnum> tagHC =
        new HashSet<PolicyTag.TagsEnum>(Arrays.asList(PolicyTag.TagsEnum.HC));
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertTrue(updatedPolicies.getColumnTags().containsKey("col1"));
    Assertions.assertEquals(tagHC, updatedPolicies.getColumnTags().get("col1").getTags());
  }

  @Test
  public void testPoliciesReplicationExistsButNoUpdateEmptyInterval() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "policies", "{\"replication\":{\"config\":[{\"destination\":\"a\", \"interval\":\"\"}]}}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertEquals(
        updatedPolicies.getReplication().getConfig().get(0).getDestination(), "a");
    Assertions.assertTrue(
        updatedPolicies.getReplication().getConfig().get(0).getInterval().isEmpty());
    Assertions.assertEquals(updatedPolicies.getReplication().getConfig().size(), 1);
  }

  @Test
  public void testNoPoliciesReplicationButUpdateExists() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "updated.openhouse.policy",
        "{\"replication\":{\"config\":[{\"destination\":\"aa\", \"interval\":\"1D\"}]}}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertEquals(
        updatedPolicies.getReplication().getConfig().get(0).getDestination(), "aa");
    Assertions.assertEquals(
        updatedPolicies.getReplication().getConfig().get(0).getInterval(), "1D");
    Assertions.assertEquals(updatedPolicies.getReplication().getConfig().size(), 1);
  }

  @Test
  public void testPoliciesReplicationExistsUpdateExists() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "policies",
        "{\"replication\":{\"config\":[{\"destination\":\"a\", \"interval\":\"1D\"}, {\"destination\":\"b\", \"interval\":\"1D\"}]}}");
    props.put(
        "updated.openhouse.policy",
        "{\"replication\":{\"config\":[{\"destination\":\"aa\", \"interval\":\"2D\"}]}}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertEquals(
        updatedPolicies.getReplication().getConfig().get(0).getDestination(), "aa");
    Assertions.assertEquals(
        updatedPolicies.getReplication().getConfig().get(0).getInterval(), "2D");
    Assertions.assertEquals(updatedPolicies.getReplication().getConfig().size(), 1);
  }

  @Test
  public void testPoliciesReplicationExistsUpdateExistsForMultiple() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "policies",
        "{\"replication\":{\"config\":[{\"destination\":\"a\", \"interval\":\"1D\"}]}}");
    props.put(
        "updated.openhouse.policy",
        "{\"replication\":{\"config\":[{\"destination\":\"a\", \"interval\":\"1D\"}, {\"destination\":\"aa\", \"interval\":\"2D\"}]}}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertEquals(
        updatedPolicies.getReplication().getConfig().get(0).getDestination(), "a");
    Assertions.assertEquals(
        updatedPolicies.getReplication().getConfig().get(0).getInterval(), "1D");
    Assertions.assertEquals(
        updatedPolicies.getReplication().getConfig().get(1).getDestination(), "aa");
    Assertions.assertEquals(
        updatedPolicies.getReplication().getConfig().get(1).getInterval(), "2D");
    Assertions.assertEquals(updatedPolicies.getReplication().getConfig().size(), 2);
  }

  @Test
  public void testTableTypeForReplicationFlow() {
    Map<String, String> baseProps = new HashMap<>();
    Map<String, String> metaDataProps = new HashMap<>();
    baseProps.put("openhouse.tableType", "REPLICA_TABLE");
    baseProps.put("openhouse.clusterId", "cluster1");
    baseProps.put(
        "openhouse.policy",
        "{\"replication\":{\"config\":[{\"destination\":\"a\", \"interval\":\"1D\"}, {\"destination\":\"aa\", \"interval\":\"2D\"}]}}");

    TableMetadata base = mock(TableMetadata.class);
    metaDataProps.put("openhouse.tableType", "PRIMARY_TABLE");
    metaDataProps.put("openhouse.clusterId", "cluster2");
    metaDataProps.put(
        "openhouse.policy",
        "{\"replication\":{\"config\":[{\"destination\":\"a\", \"interval\":\"1D\"}, {\"destination\":\"aa\", \"interval\":\"2D\"}]}}");
    TableMetadata metadata = mock(TableMetadata.class);

    when(base.properties()).thenReturn(baseProps);
    Schema schema = new Schema();
    when(base.schema()).thenReturn(schema);

    when(metadata.properties()).thenReturn(metaDataProps);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);

    when(openHouseTableOperations.getTableType(base, metadata)).thenCallRealMethod();
    CreateUpdateTableRequestBody.TableTypeEnum tableType =
        openHouseTableOperations.getTableType(base, metadata);

    Assertions.assertEquals(tableType, CreateUpdateTableRequestBody.TableTypeEnum.REPLICA_TABLE);
  }

  @Test
  public void testPoliciesHistoryInMetadataNoUpdate() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "policies",
        "{\"history\": {\"maxAge\": \"1\", \"granularity\": \"DAY\", \"versions\": \"2\"}}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertEquals(1, updatedPolicies.getHistory().getMaxAge());
    Assertions.assertEquals(
        History.GranularityEnum.DAY, updatedPolicies.getHistory().getGranularity());
    Assertions.assertEquals(2, updatedPolicies.getHistory().getVersions());
  }

  @Test
  public void testNoPoliciesHistoryExistsButUpdateExists() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "updated.openhouse.policy",
        "{\"history\": {\"maxAge\": \"1\", \"granularity\": \"DAY\", \"versions\": \"2\"}}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertEquals(1, updatedPolicies.getHistory().getMaxAge());
    Assertions.assertEquals(
        History.GranularityEnum.DAY, updatedPolicies.getHistory().getGranularity());
    Assertions.assertEquals(2, updatedPolicies.getHistory().getVersions());
  }

  @Test
  public void testPoliciesHistoryExistsUpdate() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "openhouse.policy",
        "{\"history\": {\"maxAge\": \"2\", \"granularity\": \"HOUR\", \"versions\": \"3\"}}");
    props.put(
        "updated.openhouse.policy",
        "{\"history\": {\"maxAge\": \"1\", \"granularity\": \"DAY\", \"versions\": \"2\"}, \"sharingEnabled\": true}");
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(props);
    OpenHouseTableOperations openHouseTableOperations = mock(OpenHouseTableOperations.class);
    when(openHouseTableOperations.buildUpdatedPolicies(metadata)).thenCallRealMethod();
    Policies updatedPolicies = openHouseTableOperations.buildUpdatedPolicies(metadata);
    Assertions.assertNotNull(updatedPolicies);
    Assertions.assertEquals(1, updatedPolicies.getHistory().getMaxAge());
    Assertions.assertEquals(
        History.GranularityEnum.DAY, updatedPolicies.getHistory().getGranularity());
    Assertions.assertEquals(2, updatedPolicies.getHistory().getVersions());
    Assertions.assertEquals(true, updatedPolicies.getSharingEnabled());
  }

  private OpenHouseTableOperations refreshableOps(TableApi tableApi) {
    return refreshableOps(tableApi, mock(FileIO.class));
  }

  private OpenHouseTableOperations refreshableOps(TableApi tableApi, FileIO fileIO) {
    return OpenHouseTableOperations.builder()
        .tableIdentifier(TableIdentifier.of("db", "tbl"))
        .fileIO(fileIO)
        .tableApi(tableApi)
        .snapshotApi(mock(SnapshotApi.class))
        .cluster("cluster")
        .build();
  }

  /** No load yet → no config. */
  @Test
  public void testCurrentConfigNullBeforeRefresh() {
    Assertions.assertNull(refreshableOps(mock(TableApi.class)).currentConfig());
  }

  /** Config is captured when Iceberg actually reloads metadata, not merely on GET. */
  @Test
  public void testDoRefreshCapturesConfig() {
    String location = writeTempMetadata();
    Map<String, String> stamped =
        Collections.singletonMap("openhouse.read-bridge", "{\"read\":\"ON\"}");
    TableApi mockTableApi = mock(TableApi.class);
    GetTableResponseBody body = mock(GetTableResponseBody.class);
    when(body.getTableLocation()).thenReturn(location);
    when(body.getConfig()).thenReturn(stamped);
    when(mockTableApi.getTableV1(anyString(), anyString())).thenReturn(Mono.just(body));

    OpenHouseTableOperations ops = refreshableOps(mockTableApi, localFileIO());
    ops.doRefresh();

    Assertions.assertSame(stamped, ops.currentConfig());
  }

  /** Absent config on a real load => null. */
  @Test
  public void testDoRefreshNullConfigWhenAbsent() {
    String location = writeTempMetadata();
    TableApi mockTableApi = mock(TableApi.class);
    GetTableResponseBody body = mock(GetTableResponseBody.class);
    when(body.getTableLocation()).thenReturn(location);
    when(body.getConfig()).thenReturn(null);
    when(mockTableApi.getTableV1(anyString(), anyString())).thenReturn(Mono.just(body));

    OpenHouseTableOperations ops = refreshableOps(mockTableApi, localFileIO());
    ops.doRefresh();

    Assertions.assertNull(ops.currentConfig());
  }

  /**
   * Same metadata location: Iceberg skips reload, so a later GET that stops stamping must not clear
   * the config still paired with in-memory overlays.
   */
  @Test
  public void testDoRefreshKeepsConfigWhenLocationUnchanged() {
    String location = writeTempMetadata();
    Map<String, String> stamped =
        Collections.singletonMap("openhouse.read-bridge", "{\"read\":\"ON\"}");

    GetTableResponseBody withConfig = mock(GetTableResponseBody.class);
    when(withConfig.getTableLocation()).thenReturn(location);
    when(withConfig.getConfig()).thenReturn(stamped);

    GetTableResponseBody withoutConfig = mock(GetTableResponseBody.class);
    when(withoutConfig.getTableLocation()).thenReturn(location);
    when(withoutConfig.getConfig()).thenReturn(null);

    TableApi mockTableApi = mock(TableApi.class);
    when(mockTableApi.getTableV1(anyString(), anyString()))
        .thenReturn(Mono.just(withConfig))
        .thenReturn(Mono.just(withoutConfig));

    OpenHouseTableOperations ops = refreshableOps(mockTableApi, localFileIO());
    ops.doRefresh();
    Assertions.assertSame(stamped, ops.currentConfig());

    ops.doRefresh();
    Assertions.assertSame(stamped, ops.currentConfig());
  }

  /**
   * Skip-reload after a GET that stops stamping must still send overlays from the bound config. The
   * server drops them; the client must not strip the default-aware signal.
   */
  @Test
  public void testDoRefreshSkipReloadStillSendsStampedDefaults() {
    String location = writeTempMetadata();
    Map<String, String> stamped =
        Collections.singletonMap(ReadBridge.COLUMN_DEFAULT_PREFIX + "2", "\"US\"");

    GetTableResponseBody withConfig = mock(GetTableResponseBody.class);
    when(withConfig.getTableLocation()).thenReturn(location);
    when(withConfig.getConfig()).thenReturn(stamped);

    GetTableResponseBody withoutConfig = mock(GetTableResponseBody.class);
    when(withoutConfig.getTableLocation()).thenReturn(location);
    when(withoutConfig.getConfig()).thenReturn(null);

    TableApi mockTableApi = mock(TableApi.class);
    when(mockTableApi.getTableV1(anyString(), anyString()))
        .thenReturn(Mono.just(withConfig))
        .thenReturn(Mono.just(withoutConfig));

    OpenHouseTableOperations ops = refreshableOps(mockTableApi, localFileIO());
    ops.doRefresh();
    Assertions.assertSame(stamped, ops.currentConfig());

    ops.doRefresh();
    Assertions.assertSame(stamped, ops.currentConfig());

    TableMetadata commit =
        tableWithSchema(
            "file:/tmp/rb-signal-skip-reload",
            new Schema(
                NestedField.optional(1, "id", Types.IntegerType.get()),
                NestedField.from(NestedField.optional(2, "country", Types.StringType.get()))
                    .withInitialDefault(Expressions.lit("US"))
                    .build()));
    Assertions.assertEquals(
        "US",
        SchemaParser.fromJson(ops.constructMetadataRequestBody(null, commit).getSchema())
            .findField(2)
            .initialDefault());
  }

  /** A later load from a new metadata location binds that response's config. */
  @Test
  public void testDoRefreshBindsNewConfigWhenLocationChanges() {
    // Same table UUID, two files: Iceberg reloads on location change and rejects a UUID mismatch.
    String[] locations = writeTempMetadataPair();
    String first = locations[0];
    String second = locations[1];
    Map<String, String> stamped =
        Collections.singletonMap("openhouse.read-bridge", "{\"read\":\"ON\"}");

    GetTableResponseBody withConfig = mock(GetTableResponseBody.class);
    when(withConfig.getTableLocation()).thenReturn(first);
    when(withConfig.getConfig()).thenReturn(stamped);

    GetTableResponseBody withoutConfig = mock(GetTableResponseBody.class);
    when(withoutConfig.getTableLocation()).thenReturn(second);
    when(withoutConfig.getConfig()).thenReturn(null);

    TableApi mockTableApi = mock(TableApi.class);
    when(mockTableApi.getTableV1(anyString(), anyString()))
        .thenReturn(Mono.just(withConfig))
        .thenReturn(Mono.just(withoutConfig));

    OpenHouseTableOperations ops = refreshableOps(mockTableApi, localFileIO());
    ops.doRefresh();
    Assertions.assertSame(stamped, ops.currentConfig());

    ops.doRefresh();
    Assertions.assertNull(ops.currentConfig());
  }

  /**
   * Iceberg rejects a UUID change after the loader returns. Config must stay paired with the
   * in-memory metadata that is still installed.
   */
  @Test
  public void testDoRefreshKeepsConfigWhenUuidCheckFails() {
    String first = writeTempMetadata();
    String second = writeTempMetadata();
    Map<String, String> stamped =
        Collections.singletonMap("openhouse.read-bridge", "{\"read\":\"ON\"}");
    Map<String, String> other =
        Collections.singletonMap("openhouse.read-bridge", "{\"read\":\"OFF\"}");

    GetTableResponseBody withConfig = mock(GetTableResponseBody.class);
    when(withConfig.getTableLocation()).thenReturn(first);
    when(withConfig.getConfig()).thenReturn(stamped);

    GetTableResponseBody mismatched = mock(GetTableResponseBody.class);
    when(mismatched.getTableLocation()).thenReturn(second);
    when(mismatched.getConfig()).thenReturn(other);

    TableApi mockTableApi = mock(TableApi.class);
    when(mockTableApi.getTableV1(anyString(), anyString()))
        .thenReturn(Mono.just(withConfig))
        .thenReturn(Mono.just(mismatched));

    OpenHouseTableOperations ops = refreshableOps(mockTableApi, localFileIO());
    ops.doRefresh();
    Assertions.assertSame(stamped, ops.currentConfig());

    Assertions.assertThrows(IllegalStateException.class, ops::doRefresh);
    Assertions.assertSame(stamped, ops.currentConfig());
  }

  private static String writeTempMetadata() {
    return writeTempMetadataPair()[0];
  }

  private static String[] writeTempMetadataPair() {
    TableMetadata created =
        TableMetadata.newTableMetadata(
            new Schema(NestedField.optional(1, "id", Types.IntegerType.get())),
            PartitionSpec.unpartitioned(),
            "file:/tmp/rb-refresh",
            Collections.emptyMap());
    try {
      Path first = java.nio.file.Files.createTempFile("oh-rb-", ".metadata.json");
      Path second = java.nio.file.Files.createTempFile("oh-rb-", ".metadata.json");
      first.toFile().deleteOnExit();
      second.toFile().deleteOnExit();
      TableMetadataParser.overwrite(created, Files.localOutput(first.toFile()));
      TableMetadataParser.overwrite(created, Files.localOutput(second.toFile()));
      return new String[] {first.toAbsolutePath().toString(), second.toAbsolutePath().toString()};
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static FileIO localFileIO() {
    return new FileIO() {
      @Override
      public InputFile newInputFile(String path) {
        return Files.localInput(path);
      }

      @Override
      public OutputFile newOutputFile(String path) {
        return Files.localOutput(path);
      }

      @Override
      public void deleteFile(String path) {
        new java.io.File(path).delete();
      }
    };
  }

  /** Bad config fails before FileIO so Iceberg does not retry the metadata read. */
  @Test
  public void testMalformedConfigFailsBeforeTouchingStorage() {
    TableApi mockTableApi = mock(TableApi.class);
    FileIO mockFileIO = mock(FileIO.class);
    GetTableResponseBody body = mock(GetTableResponseBody.class);
    // Non-null location would otherwise trigger a metadata load.
    when(body.getTableLocation()).thenReturn("/tmp/does-not-matter/metadata.json");
    when(body.getConfig())
        .thenReturn(
            Collections.singletonMap("openhouse.read-bridge.column-default.7", "{bad json"));
    when(mockTableApi.getTableV1(anyString(), anyString())).thenReturn(Mono.just(body));

    OpenHouseTableOperations ops =
        OpenHouseTableOperations.builder()
            .tableIdentifier(TableIdentifier.of("db", "tbl"))
            .fileIO(mockFileIO)
            .tableApi(mockTableApi)
            .snapshotApi(mock(SnapshotApi.class))
            .cluster("cluster")
            .build();

    Tasks.UnrecoverableException thrown =
        Assertions.assertThrows(Tasks.UnrecoverableException.class, ops::doRefresh);
    Assertions.assertInstanceOf(ReadBridgeException.class, thrown.getCause());
    Assertions.assertEquals(
        ReadBridgeException.Kind.UNUSABLE_CONFIG,
        ((ReadBridgeException) thrown.getCause()).getKind());
    Assertions.assertTrue(thrown.getMessage().contains("db.tbl"));
    verifyNoInteractions(mockFileIO);
  }

  /** Config arrives as a string map on the table-load JSON. */
  @Test
  public void testConfigDeserializeFromResponse() throws Exception {
    ObjectMapper mapper = ApiClient.createDefaultObjectMapper(null);
    String json =
        "{\"tableId\":\"tbl\",\"databaseId\":\"db\",\"config\":{"
            + "\"openhouse.read-bridge\":\"{\\\"read\\\":\\\"ON\\\"}\"}}";

    GetTableResponseBody body = mapper.readValue(json, GetTableResponseBody.class);
    Map<String, String> config = body.getConfig();
    Assertions.assertNotNull(config);
    // Channel does not parse the value.
    Assertions.assertEquals("{\"read\":\"ON\"}", config.get("openhouse.read-bridge"));
  }

  /** Unknown JSON fields and unknown config keys are carried, not rejected. */
  @Test
  public void testConfigToleratesUnknownFields() throws Exception {
    ObjectMapper mapper = ApiClient.createDefaultObjectMapper(null);
    String json =
        "{\"tableId\":\"tbl\",\"databaseId\":\"db\",\"someFutureField\":\"x\",\"config\":{"
            + "\"openhouse.unknown-feature\":\"whatever\"}}";

    GetTableResponseBody body = mapper.readValue(json, GetTableResponseBody.class);
    Map<String, String> config = body.getConfig();
    Assertions.assertNotNull(config);
    Assertions.assertEquals("whatever", config.get("openhouse.unknown-feature"));
  }

  @Test
  public void constructMetadataRequestBody_sendsStampedIdsKeepsUnstampedColumnDefaults() {
    TableMetadata commit =
        tableWithSchema(
            "file:/tmp/rb-sanitize-ops-c",
            new Schema(
                NestedField.optional(1, "id", Types.IntegerType.get()),
                NestedField.from(NestedField.optional(2, "country", Types.StringType.get()))
                    .withInitialDefault(Expressions.lit("US"))
                    .build(),
                NestedField.from(NestedField.optional(3, "email", Types.StringType.get()))
                    .withInitialDefault(Expressions.lit("none"))
                    .build()));

    OpenHouseTableOperations ops = refreshableOps(mock(TableApi.class));
    ops.setCurrentConfig(
        Collections.singletonMap(ReadBridge.COLUMN_DEFAULT_PREFIX + "2", "\"US\""));

    CreateUpdateTableRequestBody body = ops.constructMetadataRequestBody(null, commit);
    Schema sent = SchemaParser.fromJson(body.getSchema());

    Assertions.assertEquals("US", sent.findField(2).initialDefault());
    Assertions.assertEquals("none", sent.findField(3).initialDefault());
    Assertions.assertEquals("email", sent.findField(3).name());
  }

  @Test
  public void constructMetadataRequestBody_withoutConfigLeavesWriterDefaults() {
    TableMetadata commit =
        tableWithSchema(
            "file:/tmp/rb-sanitize-create",
            new Schema(
                NestedField.from(NestedField.optional(1, "country", Types.StringType.get()))
                    .withInitialDefault(Expressions.lit("US"))
                    .build()));

    CreateUpdateTableRequestBody body =
        refreshableOps(mock(TableApi.class)).constructMetadataRequestBody(null, commit);

    Assertions.assertEquals(
        "US", SchemaParser.fromJson(body.getSchema()).findField(1).initialDefault());
  }

  /**
   * {@link TableMetadata#newTableMetadata} reassigns ids and drops defaults. Put this schema back
   * so PUT tests can see writer/overlay defaults.
   */
  private static TableMetadata tableWithSchema(String location, Schema schema) {
    TableMetadata created =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), location, Collections.emptyMap());
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode root = (ObjectNode) mapper.readTree(TableMetadataParser.toJson(created));
      Schema kept =
          new Schema(
              created.currentSchemaId(),
              schema.columns(),
              schema.getAliases(),
              schema.identifierFieldIds());
      ((ArrayNode) root.get("schemas")).set(0, mapper.readTree(SchemaParser.toJson(kept)));
      return TableMetadataParser.fromJson(
          created.metadataFileLocation(), mapper.writeValueAsString(root));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * A snapshot that round-trips through {@link SnapshotParser}, so commits carrying it exercise the
   * real request-body serialization rather than a mock.
   */
  private static final String SNAPSHOT_JSON =
      "{\"snapshot-id\":3051729675574597004,\"timestamp-ms\":1515100955770,"
          + "\"summary\":{\"operation\":\"append\"},"
          + "\"manifest-list\":\"file:/tmp/manifest-list-1.avro\"}";

  /** The {@code (base, metadata)} pair handed to {@code doCommit}. */
  private static final class Commit {
    private final TableMetadata base;
    private final TableMetadata metadata;

    Commit(TableMetadata base, TableMetadata metadata) {
      this.base = base;
      this.metadata = metadata;
    }
  }

  /**
   * A commit against an existing table that carries both a metadata change and a snapshot change.
   *
   * <p>This single shape is the crux of the routing problem: Iceberg's {@code BaseTransaction}
   * funnels a replace transaction and an ordinary transaction (say, a schema evolution plus an
   * append) into the identical {@code ops.commit(base, metadata)} call, so both arrive looking
   * exactly like this. Routing therefore has to come from the transaction that produced the commit,
   * never from the diff.
   */
  private static Commit metadataAndSnapshotsUpdated() {
    TableMetadata base = mock(TableMetadata.class);
    TableMetadata metadata = mock(TableMetadata.class);

    when(base.schema()).thenReturn(mock(Schema.class));
    when(metadata.schema()).thenReturn(mock(Schema.class));
    when(base.properties()).thenReturn(ImmutableMap.of());
    when(metadata.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(base.metadataFileLocation()).thenReturn("file:/tmp/v1.metadata.json");

    when(base.snapshots()).thenReturn(Collections.emptyList());
    when(metadata.snapshots())
        .thenReturn(Collections.singletonList(SnapshotParser.fromJson(SNAPSHOT_JSON)));
    when(base.refs()).thenReturn(Collections.emptyMap());
    when(metadata.refs()).thenReturn(Collections.emptyMap());

    return new Commit(base, metadata);
  }

  private OpenHouseTableOperationsForTest opsFor(SnapshotApi snapshotApi) {
    return new OpenHouseTableOperationsForTest(
        TableIdentifier.of("a", "b"),
        mock(FileIO.class),
        mock(TableApi.class),
        snapshotApi,
        "cluster");
  }

  private static SnapshotApi acceptingSnapshotApi() {
    SnapshotApi snapshotApi = mock(SnapshotApi.class);
    when(snapshotApi.putSnapshotsV1(anyString(), anyString(), any())).thenReturn(Mono.empty());
    return snapshotApi;
  }

  /** The {@code replaceCommit} flag on the {@code nth} snapshot request the client sent. */
  private static Boolean sentReplaceCommitFlag(SnapshotApi snapshotApi, int nth) {
    ArgumentCaptor<IcebergSnapshotsRequestBody> captor =
        ArgumentCaptor.forClass(IcebergSnapshotsRequestBody.class);
    verify(snapshotApi, times(nth)).putSnapshotsV1(eq("db"), eq("tbl"), captor.capture());
    return captor.getAllValues().get(nth - 1).getCreateUpdateTableRequestBody().getReplaceCommit();
  }

  /** Commits are ordinary updates until the catalog declares a replace transaction. */
  @Test
  public void testNotAReplaceTransactionByDefault() {
    Assertions.assertFalse(refreshableOps(mock(TableApi.class)).isReplaceTransaction());
  }

  /**
   * The regression this guards: an ordinary transaction that updates metadata and appends a
   * snapshot in one commit must stay an update. This diff used to be read as an RTAS and sent as a
   * replace commit, which makes the server rebuild the table from scratch — dropping schema and
   * partition history, and demanding replace privileges — instead of updating it.
   */
  @Test
  public void testMetadataAndSnapshotUpdateIsNotAReplaceCommit() {
    SnapshotApi snapshotApi = acceptingSnapshotApi();
    OpenHouseTableOperationsForTest ops = opsFor(snapshotApi);
    Commit commit = metadataAndSnapshotsUpdated();

    ops.doCommit(commit.base, commit.metadata);

    Assertions.assertFalse(sentReplaceCommitFlag(snapshotApi, 1));
  }

  /** The very same diff, once the catalog marks it as a replace, is sent as a replace commit. */
  @Test
  public void testMarkedTransactionSendsReplaceCommit() {
    SnapshotApi snapshotApi = acceptingSnapshotApi();
    OpenHouseTableOperationsForTest ops = opsFor(snapshotApi);
    ops.markReplaceTransaction();
    Commit commit = metadataAndSnapshotsUpdated();

    ops.doCommit(commit.base, commit.metadata);

    Assertions.assertTrue(sentReplaceCommitFlag(snapshotApi, 1));
  }

  /**
   * A replace with no base is not a replace: there is no table to swap out, so the commit falls
   * back to a plain snapshot put rather than asking the server to replace a table it cannot find.
   */
  @Test
  public void testReplaceTransactionWithoutBaseIsNotAReplaceCommit() {
    SnapshotApi snapshotApi = acceptingSnapshotApi();
    OpenHouseTableOperationsForTest ops = opsFor(snapshotApi);
    ops.markReplaceTransaction();

    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(metadata.snapshots())
        .thenReturn(Collections.singletonList(SnapshotParser.fromJson(SNAPSHOT_JSON)));
    when(metadata.refs()).thenReturn(Collections.emptyMap());

    ops.doCommit(null, metadata);

    Assertions.assertFalse(sentReplaceCommitFlag(snapshotApi, 1));
  }

  /**
   * The replace intent is one-shot. A transaction's table can outlive {@code commitTransaction}, so
   * once the replace lands, later commits on the same instance must go back to being updates.
   */
  @Test
  public void testReplaceIntentClearedAfterSuccessfulCommit() {
    SnapshotApi snapshotApi = acceptingSnapshotApi();
    OpenHouseTableOperationsForTest ops = opsFor(snapshotApi);
    ops.markReplaceTransaction();

    Commit replace = metadataAndSnapshotsUpdated();
    ops.doCommit(replace.base, replace.metadata);
    Assertions.assertTrue(sentReplaceCommitFlag(snapshotApi, 1));
    Assertions.assertFalse(ops.isReplaceTransaction());

    Commit followUp = metadataAndSnapshotsUpdated();
    ops.doCommit(followUp.base, followUp.metadata);
    Assertions.assertFalse(sentReplaceCommitFlag(snapshotApi, 2));
  }

  /**
   * Iceberg retries a failed replace against this same instance, so a {@link CommitFailedException}
   * has to leave the intent standing — otherwise the retry would silently degrade into a plain
   * update and only half-apply the replace.
   */
  @Test
  public void testReplaceIntentSurvivesCommitFailure() {
    SnapshotApi snapshotApi = mock(SnapshotApi.class);
    WebClientResponseException conflict = mock(WebClientResponseException.Conflict.class);
    when(conflict.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
    when(snapshotApi.putSnapshotsV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(conflict))
        .thenReturn(Mono.empty());

    OpenHouseTableOperationsForTest ops = opsFor(snapshotApi);
    ops.markReplaceTransaction();

    Commit attempt = metadataAndSnapshotsUpdated();
    Assertions.assertThrows(
        CommitFailedException.class, () -> ops.doCommit(attempt.base, attempt.metadata));
    Assertions.assertTrue(ops.isReplaceTransaction());

    Commit retry = metadataAndSnapshotsUpdated();
    ops.doCommit(retry.base, retry.metadata);

    Assertions.assertTrue(sentReplaceCommitFlag(snapshotApi, 2));
    Assertions.assertFalse(ops.isReplaceTransaction());
  }
}
