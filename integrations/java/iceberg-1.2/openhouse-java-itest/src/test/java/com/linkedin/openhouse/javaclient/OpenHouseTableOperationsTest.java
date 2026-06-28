package com.linkedin.openhouse.javaclient;

import static org.mockito.Mockito.*;

import com.linkedin.openhouse.gen.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.gen.tables.client.model.FeatureFlag;
import com.linkedin.openhouse.gen.tables.client.model.FeatureFlags;
import com.linkedin.openhouse.gen.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.History;
import com.linkedin.openhouse.gen.tables.client.model.Policies;
import com.linkedin.openhouse.gen.tables.client.model.PolicyTag;
import com.linkedin.openhouse.gen.tables.client.model.Retention;
import com.linkedin.openhouse.javaclient.exception.WebClientWithMessageException;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.relocated.org.springframework.http.HttpStatus;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientRequestException;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    // Ensure tableApi throw expected exception

    when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(mock(WebClientResponseException.ServiceUnavailable.class)));
    Assertions.assertThrows(
        CommitStateUnknownException.class, () -> openHouseTableOperations.doCommit(base, metadata));
    when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(mock(WebClientResponseException.GatewayTimeout.class)));
    Assertions.assertThrows(
        CommitStateUnknownException.class, () -> openHouseTableOperations.doCommit(base, metadata));
    when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(mock(WebClientResponseException.NotFound.class)));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> openHouseTableOperations.doCommit(base, metadata));
    when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(mock(WebClientResponseException.InternalServerError.class)));
    Assertions.assertThrows(
        CommitStateUnknownException.class, () -> openHouseTableOperations.doCommit(base, metadata));
    when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(mock(WebClientResponseException.NotImplemented.class)));
    Assertions.assertThrows(
        WebClientWithMessageException.class,
        () -> openHouseTableOperations.doCommit(base, metadata));
    when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(mock(WebClientRequestException.class)));
    Assertions.assertThrows(
        CommitStateUnknownException.class, () -> openHouseTableOperations.doCommit(base, metadata));
    WebClientResponseException exception40x =
        mock(WebClientResponseException.MethodNotAllowed.class);
    when(exception40x.getStatusCode()).thenReturn(HttpStatus.METHOD_NOT_ALLOWED);
    when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(exception40x));
    Assertions.assertThrows(
        WebClientWithMessageException.class,
        () -> openHouseTableOperations.doCommit(base, metadata));
    WebClientResponseException exception50x = mock(WebClientResponseException.BadGateway.class);
    when(exception50x.getStatusCode()).thenReturn(HttpStatus.BAD_GATEWAY);
    when(mockTableApi.updateTableV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(exception50x));
    Assertions.assertThrows(
        CommitStateUnknownException.class, () -> openHouseTableOperations.doCommit(base, metadata));
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
    return OpenHouseTableOperations.builder()
        .tableIdentifier(TableIdentifier.of("db", "tbl"))
        .fileIO(mock(FileIO.class))
        .tableApi(tableApi)
        .snapshotApi(mock(SnapshotApi.class))
        .cluster("cluster")
        .build();
  }

  /** Before any refresh, there is no server-stamped policy, so the safe default is null. */
  @Test
  public void testCurrentFeatureFlagsNullBeforeRefresh() {
    Assertions.assertNull(refreshableOps(mock(TableApi.class)).currentFeatureFlags());
  }

  /** doRefresh stashes the server-stamped featureFlags so subclasses can read it back. */
  @Test
  public void testDoRefreshCapturesFeatureFlags() {
    TableApi mockTableApi = mock(TableApi.class);
    FeatureFlags stamped = mock(FeatureFlags.class);
    GetTableResponseBody body = mock(GetTableResponseBody.class);
    when(body.getTableLocation()).thenReturn(null);
    when(body.getFeatureFlags()).thenReturn(stamped);
    when(mockTableApi.getTableV1(anyString(), anyString())).thenReturn(Mono.just(body));

    OpenHouseTableOperations ops = refreshableOps(mockTableApi);
    ops.doRefresh();

    Assertions.assertSame(stamped, ops.currentFeatureFlags());
  }

  /** Absent featureFlags on the response => null, the consumer's safe default. */
  @Test
  public void testDoRefreshNullFeatureFlagsWhenAbsent() {
    TableApi mockTableApi = mock(TableApi.class);
    GetTableResponseBody body = mock(GetTableResponseBody.class);
    when(body.getTableLocation()).thenReturn(null);
    when(body.getFeatureFlags()).thenReturn(null);
    when(mockTableApi.getTableV1(anyString(), anyString())).thenReturn(Mono.just(body));

    OpenHouseTableOperations ops = refreshableOps(mockTableApi);
    ops.doRefresh();

    Assertions.assertNull(ops.currentFeatureFlags());
  }

  /**
   * The held policy is a snapshot of the latest refresh, never sticky: once the server stops
   * stamping featureFlags, a subsequent refresh must clear the previously-captured value back to
   * null. Guards against a stale directive lingering after the server turns it off.
   */
  @Test
  public void testDoRefreshClearsStaleFeatureFlags() {
    TableApi mockTableApi = mock(TableApi.class);
    FeatureFlags stamped = mock(FeatureFlags.class);

    GetTableResponseBody withFlags = mock(GetTableResponseBody.class);
    when(withFlags.getTableLocation()).thenReturn(null);
    when(withFlags.getFeatureFlags()).thenReturn(stamped);

    GetTableResponseBody withoutFlags = mock(GetTableResponseBody.class);
    when(withoutFlags.getTableLocation()).thenReturn(null);
    when(withoutFlags.getFeatureFlags()).thenReturn(null);

    // First refresh stamps flags, second refresh stops stamping them.
    when(mockTableApi.getTableV1(anyString(), anyString()))
        .thenReturn(Mono.just(withFlags))
        .thenReturn(Mono.just(withoutFlags));

    OpenHouseTableOperations ops = refreshableOps(mockTableApi);

    ops.doRefresh();
    Assertions.assertSame(stamped, ops.currentFeatureFlags());

    ops.doRefresh();
    Assertions.assertNull(ops.currentFeatureFlags());
  }

  /**
   * Wire contract: a server-stamped featureFlags envelope deserializes into the generated READ_ONLY
   * components on the client (the typed envelope + opaque per-feature payload). This is how the
   * value actually arrives on a real table-load response.
   */
  @Test
  public void testFeatureFlagsDeserializeFromResponse() throws Exception {
    ObjectMapper mapper = ApiClient.createDefaultObjectMapper(null);
    String json =
        "{\"tableId\":\"tbl\",\"databaseId\":\"db\",\"featureFlags\":{"
            + "\"version\":1,\"serverTimeMs\":1718900000000,\"flags\":{"
            + "\"read-bridge\":{\"enforcement\":\"REQUIRED\",\"minClientVersion\":\"1.2.1\","
            + "\"reason\":\"canary\",\"payload\":\"{\\\"read\\\":\\\"ON\\\"}\"}}}}";

    GetTableResponseBody body = mapper.readValue(json, GetTableResponseBody.class);
    FeatureFlags flags = body.getFeatureFlags();
    Assertions.assertNotNull(flags);
    Assertions.assertEquals(1, flags.getVersion());
    Assertions.assertEquals(1718900000000L, flags.getServerTimeMs());

    FeatureFlag readBridge = flags.getFlags().get("read-bridge");
    Assertions.assertNotNull(readBridge);
    Assertions.assertEquals("REQUIRED", readBridge.getEnforcement());
    Assertions.assertEquals("1.2.1", readBridge.getMinClientVersion());
    Assertions.assertEquals("canary", readBridge.getReason());
    // payload stays an opaque JSON string; the envelope never parses it.
    Assertions.assertEquals("{\"read\":\"ON\"}", readBridge.getPayload());
  }

  /**
   * Unknown directive keys and unknown future fields must not break deserialization — older clients
   * ignore what they do not understand (FAIL_ON_UNKNOWN_PROPERTIES=false).
   */
  @Test
  public void testFeatureFlagsToleratesUnknownFields() throws Exception {
    ObjectMapper mapper = ApiClient.createDefaultObjectMapper(null);
    String json =
        "{\"tableId\":\"tbl\",\"databaseId\":\"db\",\"someFutureField\":\"x\",\"featureFlags\":{"
            + "\"version\":2,\"someFutureEnvelopeField\":true,\"flags\":{"
            + "\"unknown-feature\":{\"enforcement\":\"ADVISORY\",\"futureKnob\":42}}}}";

    GetTableResponseBody body = mapper.readValue(json, GetTableResponseBody.class);
    FeatureFlags flags = body.getFeatureFlags();
    Assertions.assertNotNull(flags);
    Assertions.assertEquals(2, flags.getVersion());
    Assertions.assertEquals("ADVISORY", flags.getFlags().get("unknown-feature").getEnforcement());
  }
}
