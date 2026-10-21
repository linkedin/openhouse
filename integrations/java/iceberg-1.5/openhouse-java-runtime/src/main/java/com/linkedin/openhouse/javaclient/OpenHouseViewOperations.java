package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.javaclient.exception.WebClientRequestWithMessageException;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.tables.client.api.ViewApi;
import com.linkedin.openhouse.tables.client.model.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.client.model.GetViewResponseBody;
import com.linkedin.openhouse.tables.client.model.ViewRepresentation;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/**
 * OpenHouse {@link org.apache.iceberg.view.ViewOperations}, active only when {@code
 * spark.sql.catalog.<name>.iceberg-views-enabled=true}. Extends {@link BaseViewOperations} for the
 * {@code base == current} commit protocol and {@code current()}/{@code refresh()} caching, and
 * implements the catalog-specific {@link #doCommit} / {@link #doRefresh} against the OpenHouse
 * Views service through the generated {@link ViewApi}.
 *
 * <p>{@link #doCommit} sends the view definition ({@link CreateUpdateViewRequestBody}) to the
 * service, which authors the {@code metadata.json} and owns its location; {@link #doRefresh} reads
 * the pointer ({@link GetViewResponseBody}) and loads that server-written metadata via {@code
 * refreshFromMetadataLocation(...)}. The service-side create/read endpoints return {@code 404}
 * until the Views service is implemented, so these operations are not yet end-to-end.
 */
@Builder
@Slf4j
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class OpenHouseViewOperations extends BaseViewOperations {

  /**
   * Reserved view property carrying the dialect the view was authored in. Mirrors the server's
   * {@code ReservedViewProperties.SOURCE_DIALECT} as a local literal, since the shaded client
   * cannot depend on the server module that defines it. Set once at create; the server freezes it
   * thereafter.
   */
  private static final String OPENHOUSE_VIEW_SOURCE_DIALECT_KEY = "openhouse.view.sourceDialect";

  @Getter(AccessLevel.PROTECTED)
  private TableIdentifier viewIdentifier;

  @Getter(AccessLevel.PROTECTED)
  private FileIO fileIO;

  @Getter(AccessLevel.PROTECTED)
  private ViewApi viewApi;

  @Override
  protected void doRefresh() {
    log.info("Calling doRefresh for view: {}", viewName());
    String databaseId = viewIdentifier.namespace().toString();
    String viewId = viewIdentifier.name();
    Optional<GetViewResponseBody> viewResponse =
        viewApi
            .getViewV1(databaseId, viewId)
            /*
             On 404 the view is absent, so resume with an empty response. A response with any other
             status is surfaced with the server message attached, and a connection-level failure
             that produced no response (WebClientRequestException) is surfaced with its message too,
             so a refresh never fails with a bare, message-less stack trace.
            */
            .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.empty())
            .onErrorResume(
                WebClientResponseException.class,
                e -> Mono.error(new WebClientResponseWithMessageException(e)))
            .onErrorResume(
                WebClientRequestException.class,
                e -> Mono.error(new WebClientRequestWithMessageException(e)))
            .blockOptional();
    Optional<String> metadataLocation = viewResponse.map(GetViewResponseBody::getMetadataLocation);
    if (!metadataLocation.isPresent() && currentMetadataLocation() != null) {
      throw new NoSuchViewException(
          "Cannot find view %s after refresh, maybe another process deleted it", viewName());
    }
    // Always drive refreshFromMetadataLocation, even when the view is absent (location null): it
    // clears shouldRefresh, so BaseViewOperations.refresh()/current() stops instead of recursing
    // forever. A first probe before create hits this path.
    refreshFromMetadataLocation(metadataLocation.orElse(null));
    // Potential TODO: check if default value support needs to be added
    log.debug("Calling doRefresh succeeded for view: {}", viewName());
  }

  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata metadata) {
    log.info("Calling doCommit for view: {}", viewName());
    String databaseId = viewIdentifier.namespace().toString();
    String viewId = viewIdentifier.name();
    CreateUpdateViewRequestBody requestBody = constructRequestBody(base, metadata);
    try {
      // base == null is a create (CREATE VIEW); base != null is a replace (CREATE OR REPLACE).
      Mono<GetViewResponseBody> request =
          base == null
              ? viewApi.createViewV1(databaseId, requestBody)
              : viewApi.updateViewV1(databaseId, viewId, requestBody);
      request.onErrorResume(e -> handleCommitHttpError(e, databaseId, viewId)).block();
    } catch (RuntimeException e) {
      if (e.getCause() instanceof InterruptedException) {
        log.error("Interrupted during doCommit for view: {}", viewName(), e);
        throw new CommitStateUnknownException(e);
      }
      throw e;
    }
    log.debug("Calling doCommit succeeded for view: {}", viewName());
  }

  /**
   * Classifies a view create/replace failure into the Iceberg exception the commit contract
   * expects. The service's {@code ViewErrorCode} taxonomy never reaches the wire, so the HTTP
   * status is all the client has; the declared statuses are 404, 409, 400, 422 and 503.
   *
   * <ul>
   *   <li>404 -&gt; {@link NoSuchViewException}: the view or its database is gone.
   *   <li>409 -&gt; {@link CommitFailedException}: optimistic-concurrency check failed or the name
   *       collided. Views have no commit-retry loop; Iceberg's view builder catches this and
   *       rethrows {@link org.apache.iceberg.exceptions.AlreadyExistsException} ("already exists"
   *       on create, "updated concurrently" on replace), where the user-facing meaning is decided.
   *   <li>400 -&gt; {@link BadRequestException}: invalid view definition, dialect or schema.
   *   <li>501 or any other 4xx, including 422 (view admission, missing representation, dependency
   *       cycle, max depth) -&gt; surfaced verbatim with the server message as a permanent,
   *       non-retriable rejection.
   *   <li>500/503/504, or any non-response error such as {@link WebClientRequestException} -&gt;
   *       {@link CommitStateUnknownException}: the write may have landed, so the outcome is not
   *       reported as a definite failure.
   * </ul>
   */
  private static Mono<GetViewResponseBody> handleCommitHttpError(
      Throwable e, String databaseId, String viewId) {
    if (e instanceof WebClientResponseException.NotFound) {
      return Mono.error(new NoSuchViewException(e, "View %s.%s doesn't exist", databaseId, viewId));
    } else if (e instanceof WebClientResponseException.Conflict) {
      WebClientResponseException casted = (WebClientResponseException) e;
      return Mono.error(
          new CommitFailedException(
              casted, casted.getStatusCode().value() + " , " + casted.getResponseBodyAsString()));
    } else if (e instanceof WebClientResponseException.GatewayTimeout
        || e instanceof WebClientResponseException.ServiceUnavailable
        || e instanceof WebClientResponseException.InternalServerError) {
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
      // Catch-all for non-WebClientResponseException errors, including WebClientRequestException
      // (the response was not fully received even if the write landed). Report unknown commit state
      // to skip caller-side cleanup that could delete committed data.
      log.error(
          "Unexpected exception during doCommit for view {}.{}: {}",
          databaseId,
          viewId,
          e.getClass().getSimpleName(),
          e);
      return Mono.error(new CommitStateUnknownException(e));
    }
  }

  /**
   * Maps an Iceberg {@link ViewMetadata} to an OpenHouse {@link CreateUpdateViewRequestBody}. The
   * service owns the {@code metadata.json}, so only the view definition is sent. For now a view has
   * a single SQL representation whose dialect is one-to-one with {@code sourceDialect}, so the
   * source dialect is derived from that representation.
   *
   * <p>TODO: generalize to multiple representations and a less rigid dialect derivation.
   */
  private CreateUpdateViewRequestBody constructRequestBody(
      ViewMetadata base, ViewMetadata metadata) {
    ViewVersion currentVersion = metadata.currentVersion();
    List<SQLViewRepresentation> sqlRepresentations =
        currentVersion.representations().stream()
            .filter(SQLViewRepresentation.class::isInstance)
            .map(SQLViewRepresentation.class::cast)
            .collect(Collectors.toList());
    // TODO: this client derives sourceDialect from a single SQL representation and therefore
    // rejects anything but one, tying the view to a single dialect/engine. The server already
    // accepts multiple representations (one per unique dialect) as long as sourceDialect names one
    // of them; generalize this client to carry multiple representations (e.g. Spark and Trino) and
    // select sourceDialect as the immutable edit-owner that decides which engine may later update
    // the view. Until then, reject anything but one representation.
    if (sqlRepresentations.size() != 1) {
      throw new UnsupportedOperationException(
          String.format(
              "OpenHouse views support exactly one SQL representation; found %d for view %s",
              sqlRepresentations.size(), viewName()));
    }
    List<ViewRepresentation> representations =
        sqlRepresentations.stream()
            .map(
                rep ->
                    new ViewRepresentation().type(rep.type()).sql(rep.sql()).dialect(rep.dialect()))
            .collect(Collectors.toList());

    CreateUpdateViewRequestBody requestBody =
        new CreateUpdateViewRequestBody()
            .viewId(viewIdentifier.name())
            .databaseId(viewIdentifier.namespace().toString())
            .schema(SchemaParser.toJson(metadata.schema(), false))
            .representations(representations);

    // defaultCatalog/defaultNamespace mirror the Iceberg view spec's resolution context (the
    // "current" catalog/namespace used to resolve *unqualified* identifiers in the view SQL). Both
    // are optional on the wire; we pass them through only to round-trip a faithful ViewMetadata.
    // When the persisted view SQL is fully qualified, these defaults are never consulted at
    // resolution time. Also, deferred decision, to check if engines want to populate default
    // catalog
    // or OH overrides ViewBuilder to setup default value.
    // Optional fields: set only when present so each JsonNullable stays "undefined" otherwise.
    if (currentVersion.defaultCatalog() != null) {
      requestBody.defaultCatalog(currentVersion.defaultCatalog());
    }
    Namespace defaultNamespace = currentVersion.defaultNamespace();
    if (defaultNamespace != null && !defaultNamespace.isEmpty()) {
      requestBody.defaultNamespace(Arrays.asList(defaultNamespace.levels()));
    }
    // The authoring dialect travels as the reserved property openhouse.view.sourceDialect, carried
    // in the view properties alongside any user-set keys. The server allow-lists this key only at
    // create and freezes it thereafter. For now it is derived from the single SQL representation's
    // dialect; TODO: generalize once a view may carry multiple representations. Copy into a mutable
    // map so a caller's immutable or absent properties map is never mutated.
    Map<String, String> sourceProperties = metadata.properties();
    Map<String, String> viewProperties =
        sourceProperties == null ? new HashMap<>() : new HashMap<>(sourceProperties);
    viewProperties.put(OPENHOUSE_VIEW_SOURCE_DIALECT_KEY, sqlRepresentations.get(0).dialect());
    requestBody.viewProperties(viewProperties);
    if (base != null) {
      requestBody.baseMetadataLocation(base.metadataFileLocation());
    }
    return requestBody;
  }

  @Override
  protected String viewName() {
    return viewIdentifier.toString();
  }

  @Override
  protected FileIO io() {
    return fileIO;
  }
}
