package com.linkedin.openhouse.javaclient;

import static org.mockito.Mockito.*;

import com.linkedin.openhouse.gen.tables.client.api.ViewApi;
import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.gen.tables.client.model.GetViewResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.ViewRepresentation;
import com.linkedin.openhouse.javaclient.exception.WebClientRequestWithMessageException;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.relocated.org.springframework.http.HttpHeaders;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientRequestException;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link OpenHouseViewOperations}, the iceberg-1.5-only, {@link ViewApi}-backed view
 * operations. The tests mock {@link ViewApi} plus the Iceberg {@link ViewMetadata}/{@link
 * ViewVersion}/{@link SQLViewRepresentation} interfaces to exercise the {@code doCommit} create /
 * replace mapping, its HTTP-status error classification, and the {@code doRefresh} not-found and
 * connection-failure handling in isolation, without a running service. Mirrors the conventions of
 * {@code OpenHouseTableOperationsTest}.
 */
public class OpenHouseViewOperationsTest {

  private OpenHouseViewOperations viewOps(ViewApi viewApi) {
    return OpenHouseViewOperations.builder()
        .viewIdentifier(TableIdentifier.of("db", "v"))
        .fileIO(mock(FileIO.class))
        .viewApi(viewApi)
        .build();
  }

  /** A {@link ViewMetadata} carrying exactly one SQL representation with the given dialect. */
  private ViewMetadata singleSqlViewMetadata(String dialect) {
    return singleSqlViewMetadata(dialect, Collections.singletonMap("comment", "hi"));
  }

  /**
   * A {@link ViewMetadata} carrying exactly one SQL representation with the given dialect and view
   * properties. {@code properties} may be {@code null} to exercise the null-properties path.
   */
  private ViewMetadata singleSqlViewMetadata(String dialect, Map<String, String> properties) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SQLViewRepresentation sqlRep = mock(SQLViewRepresentation.class);
    when(sqlRep.type()).thenReturn("sql");
    when(sqlRep.sql()).thenReturn("SELECT 1");
    when(sqlRep.dialect()).thenReturn(dialect);
    org.apache.iceberg.view.ViewRepresentation repAsBase = sqlRep;

    ViewVersion version = mock(ViewVersion.class);
    when(version.representations()).thenReturn(Collections.singletonList(repAsBase));
    when(version.defaultCatalog()).thenReturn("openhouse");
    when(version.defaultNamespace()).thenReturn(Namespace.of("db"));

    ViewMetadata metadata = mock(ViewMetadata.class);
    when(metadata.currentVersion()).thenReturn(version);
    when(metadata.schema()).thenReturn(schema);
    when(metadata.properties()).thenReturn(properties);
    return metadata;
  }

  /** A {@link ViewMetadata} whose current version carries exactly the given representations. */
  private ViewMetadata viewMetadataWithRepresentations(
      List<org.apache.iceberg.view.ViewRepresentation> representations) {
    ViewVersion version = mock(ViewVersion.class);
    when(version.representations()).thenReturn(representations);
    ViewMetadata metadata = mock(ViewMetadata.class);
    when(metadata.currentVersion()).thenReturn(version);
    return metadata;
  }

  /**
   * A real {@link WebClientResponseException} of the subclass matching {@code status} (e.g. 409 ->
   * {@code Conflict}), so the classifier's {@code instanceof} checks and its reads of {@code
   * getStatusCode()} / {@code getResponseBodyAsString()} run against a faithful instance rather
   * than a bare mock.
   */
  private static WebClientResponseException responseException(int status) {
    return WebClientResponseException.create(
        status,
        "status " + status,
        new HttpHeaders(),
        ("body for " + status).getBytes(StandardCharsets.UTF_8),
        StandardCharsets.UTF_8);
  }

  @Test
  public void testDoCommitCreateMapsFieldsAndCallsCreateView() {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.createViewV1(anyString(), any())).thenReturn(Mono.just(new GetViewResponseBody()));
    OpenHouseViewOperations ops = viewOps(viewApi);

    ops.doCommit(null, singleSqlViewMetadata("spark"));

    ArgumentCaptor<CreateUpdateViewRequestBody> captor =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    verify(viewApi).createViewV1(eq("db"), captor.capture());
    verify(viewApi, never()).updateViewV1(anyString(), anyString(), any());

    CreateUpdateViewRequestBody body = captor.getValue();
    Assertions.assertEquals("v", body.getViewId());
    Assertions.assertEquals("db", body.getDatabaseId());
    Assertions.assertTrue(body.getSchema().contains("\"id\""));
    Assertions.assertEquals(1, body.getRepresentations().size());
    ViewRepresentation rep = body.getRepresentations().get(0);
    Assertions.assertEquals("sql", rep.getType());
    Assertions.assertEquals("SELECT 1", rep.getSql());
    Assertions.assertEquals("spark", rep.getDialect());
    // The authoring dialect travels as the reserved property openhouse.view.sourceDialect in the
    // view properties (derived from the single representation), not a top-level field.
    Assertions.assertEquals("spark", body.getViewProperties().get("openhouse.view.sourceDialect"));
    Assertions.assertEquals("openhouse", body.getDefaultCatalog());
    Assertions.assertEquals(Collections.singletonList("db"), body.getDefaultNamespace());
    Assertions.assertEquals("hi", body.getViewProperties().get("comment"));
    // No optimistic-concurrency token is sent on create.
    Assertions.assertNull(body.getBaseMetadataLocation());
  }

  @Test
  public void testDoCommitReplaceSetsBaseLocationAndCallsUpdateView() {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.updateViewV1(anyString(), anyString(), any()))
        .thenReturn(Mono.just(new GetViewResponseBody()));
    OpenHouseViewOperations ops = viewOps(viewApi);

    ViewMetadata base = mock(ViewMetadata.class);
    when(base.metadataFileLocation()).thenReturn("s3://bucket/db/v/old-metadata.json");

    ops.doCommit(base, singleSqlViewMetadata("spark"));

    ArgumentCaptor<CreateUpdateViewRequestBody> captor =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    verify(viewApi).updateViewV1(eq("db"), eq("v"), captor.capture());
    verify(viewApi, never()).createViewV1(anyString(), any());
    // On replace, the base metadata location drives the service's optimistic-concurrency check.
    Assertions.assertEquals(
        "s3://bucket/db/v/old-metadata.json", captor.getValue().getBaseMetadataLocation());
  }

  static Stream<Arguments> nonSingleSqlRepresentations() {
    org.apache.iceberg.view.ViewRepresentation sqlRepA = mock(SQLViewRepresentation.class);
    org.apache.iceberg.view.ViewRepresentation sqlRepB = mock(SQLViewRepresentation.class);
    org.apache.iceberg.view.ViewRepresentation nonSql =
        mock(org.apache.iceberg.view.ViewRepresentation.class);
    return Stream.of(
        Arguments.of("zero representations", Collections.emptyList()),
        Arguments.of("two SQL representations", Arrays.asList(sqlRepA, sqlRepB)),
        Arguments.of("only a non-SQL representation", Collections.singletonList(nonSql)));
  }

  /**
   * The client derives the source dialect from a single SQL representation, so anything that does
   * not reduce to exactly one SQL representation — zero, multiple, or non-SQL-only — must be
   * rejected before any API call.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("nonSingleSqlRepresentations")
  public void testDoCommitRejectsOtherThanOneSqlRepresentation(
      String description, List<org.apache.iceberg.view.ViewRepresentation> representations) {
    ViewApi viewApi = mock(ViewApi.class);
    OpenHouseViewOperations ops = viewOps(viewApi);

    ViewMetadata metadata = viewMetadataWithRepresentations(representations);
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> ops.doCommit(null, metadata));
    verify(viewApi, never()).createViewV1(anyString(), any());
    verify(viewApi, never()).updateViewV1(anyString(), anyString(), any());
  }

  static Stream<Arguments> absentViewResponses() {
    return Stream.of(
        Arguments.of("empty response body", Mono.empty()),
        Arguments.of(
            "404 from service", Mono.error(mock(WebClientResponseException.NotFound.class))));
  }

  /**
   * An absent view — signalled either by an empty response body or a real 404 — with no current
   * metadata location (never committed) is a no-op refresh, not an error.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("absentViewResponses")
  public void testDoRefreshTreatsAbsentViewAsNoOp(
      String description, Mono<GetViewResponseBody> response) {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.getViewV1("db", "v")).thenReturn(response);
    OpenHouseViewOperations ops = viewOps(viewApi);

    Assertions.assertDoesNotThrow(ops::doRefresh);
    verify(viewApi).getViewV1("db", "v");
  }

  @Test
  public void testDoRefreshServerErrorSurfacesMessageException() {
    ViewApi viewApi = mock(ViewApi.class);
    // Any non-404 response error is surfaced to the caller with the server message attached.
    when(viewApi.getViewV1("db", "v"))
        .thenReturn(Mono.error(mock(WebClientResponseException.InternalServerError.class)));
    OpenHouseViewOperations ops = viewOps(viewApi);

    Assertions.assertThrows(WebClientResponseWithMessageException.class, ops::doRefresh);
  }

  @Test
  public void testDoRefreshRequestErrorSurfacesRequestMessageException() {
    ViewApi viewApi = mock(ViewApi.class);
    // A connection-level failure that produced no response is surfaced with its message, not
    // swallowed as an absent view or thrown bare.
    when(viewApi.getViewV1("db", "v"))
        .thenReturn(Mono.error(mock(WebClientRequestException.class)));
    OpenHouseViewOperations ops = viewOps(viewApi);

    Assertions.assertThrows(WebClientRequestWithMessageException.class, ops::doRefresh);
  }

  static Stream<Arguments> commitHttpErrors() {
    return Stream.of(
        Arguments.of("404 -> NoSuchView", responseException(404), NoSuchViewException.class),
        Arguments.of("409 -> CommitFailed", responseException(409), CommitFailedException.class),
        Arguments.of("400 -> BadRequest", responseException(400), BadRequestException.class),
        Arguments.of(
            "422 -> message exception",
            responseException(422),
            WebClientResponseWithMessageException.class),
        Arguments.of(
            "501 -> message exception",
            responseException(501),
            WebClientResponseWithMessageException.class),
        Arguments.of(
            "500 -> CommitStateUnknown", responseException(500), CommitStateUnknownException.class),
        Arguments.of(
            "503 -> CommitStateUnknown", responseException(503), CommitStateUnknownException.class),
        Arguments.of(
            "504 -> CommitStateUnknown", responseException(504), CommitStateUnknownException.class),
        Arguments.of(
            "request exception -> CommitStateUnknown",
            mock(WebClientRequestException.class),
            CommitStateUnknownException.class));
  }

  /**
   * doCommit maps each failure the Views service can return to the exception the Iceberg commit
   * contract expects, by HTTP status (the service's error taxonomy never reaches the wire). 409 ->
   * {@link CommitFailedException} lets Iceberg's view builder translate a race or name collision
   * into its own {@code AlreadyExistsException}; 5xx and a response-less request failure -> {@link
   * CommitStateUnknownException} so an unknown outcome is never reported as a clean failure.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("commitHttpErrors")
  public void testDoCommitClassifiesHttpError(
      String description, Throwable error, Class<? extends Throwable> expected) {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.createViewV1(anyString(), any())).thenReturn(Mono.error(error));
    OpenHouseViewOperations ops = viewOps(viewApi);

    Assertions.assertThrows(expected, () -> ops.doCommit(null, singleSqlViewMetadata("spark")));
  }

  @Test
  public void testDoCommitReplaceRoutesErrorThroughClassifier() {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.updateViewV1(anyString(), anyString(), any()))
        .thenReturn(Mono.error(responseException(409)));
    OpenHouseViewOperations ops = viewOps(viewApi);

    ViewMetadata base = mock(ViewMetadata.class);
    when(base.metadataFileLocation()).thenReturn("s3://bucket/db/v/old-metadata.json");

    // Replace (base != null) calls updateViewV1 and its errors go through the same classifier, so a
    // 409 lost-race surfaces as CommitFailedException just as it does on create.
    Assertions.assertThrows(
        CommitFailedException.class, () -> ops.doCommit(base, singleSqlViewMetadata("spark")));
    verify(viewApi, never()).createViewV1(anyString(), any());
  }

  @Test
  public void testDoCommitInterruptedThrowsCommitStateUnknown() {
    ViewApi viewApi = mock(ViewApi.class);
    // A long-running commit whose thread is interrupted reports CommitStateUnknown: the service may
    // or may not have applied the change, so the client must not claim success or a clean failure.
    when(viewApi.createViewV1(anyString(), any())).thenReturn(Mono.never());
    OpenHouseViewOperations ops = viewOps(viewApi);

    Thread.currentThread().interrupt();
    Assertions.assertThrows(
        CommitStateUnknownException.class,
        () -> ops.doCommit(null, singleSqlViewMetadata("spark")));
    // The blocking get consumed and cleared the interrupt; assert so it does not leak to other
    // tests.
    Assertions.assertFalse(Thread.interrupted());
  }

  @Test
  public void testDoCommitCreateNullPropertiesInjectsOnlyDialect() {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.createViewV1(anyString(), any())).thenReturn(Mono.just(new GetViewResponseBody()));
    OpenHouseViewOperations ops = viewOps(viewApi);

    // Null user properties must not NPE; only the reserved dialect key is injected.
    ops.doCommit(null, singleSqlViewMetadata("trino", null));

    ArgumentCaptor<CreateUpdateViewRequestBody> captor =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    verify(viewApi).createViewV1(eq("db"), captor.capture());
    Map<String, String> props = captor.getValue().getViewProperties();
    Assertions.assertEquals(1, props.size());
    Assertions.assertEquals("trino", props.get("openhouse.view.sourceDialect"));
  }

  @Test
  public void testDoCommitCreateDoesNotMutateCallerProperties() {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.createViewV1(anyString(), any())).thenReturn(Mono.just(new GetViewResponseBody()));
    OpenHouseViewOperations ops = viewOps(viewApi);

    // An unmodifiable caller map must survive dialect injection unchanged (defensive copy).
    Map<String, String> callerProps =
        Collections.unmodifiableMap(Collections.singletonMap("comment", "hi"));
    ops.doCommit(null, singleSqlViewMetadata("spark", callerProps));

    ArgumentCaptor<CreateUpdateViewRequestBody> captor =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    verify(viewApi).createViewV1(eq("db"), captor.capture());
    Map<String, String> sent = captor.getValue().getViewProperties();
    Assertions.assertEquals("hi", sent.get("comment"));
    Assertions.assertEquals("spark", sent.get("openhouse.view.sourceDialect"));
    // The caller's own map is untouched (no reserved key leaked back into it).
    Assertions.assertEquals(1, callerProps.size());
    Assertions.assertFalse(callerProps.containsKey("openhouse.view.sourceDialect"));
  }
}
