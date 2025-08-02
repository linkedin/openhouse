package com.linkedin.openhouse.internal.catalog.repository;

import static com.linkedin.openhouse.common.utils.PageableUtil.getSortByStr;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.model.CreateUpdateEntityRequestBodyUserTable;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyUserTable;
import com.linkedin.openhouse.housetables.client.model.GetAllEntityResponseBodyUserTable;
import com.linkedin.openhouse.housetables.client.model.PageUserTable;
<<<<<<< HEAD
import com.linkedin.openhouse.housetables.client.model.UserTable;
=======
>>>>>>> cdca73ee (Add table service integration for purge table)
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalTableOperations;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import io.netty.resolver.dns.DnsNameResolverTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.directory.api.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Implementation of {@link HouseTableRepository} using House Table Service using client generated
 * in build time.
 */
@SuppressWarnings("unchecked")
@Repository
@Slf4j
public class HouseTableRepositoryImpl implements HouseTableRepository {

  /**
   * The read request timeout is decided based on retry template logic and server side gateway
   * timeout of 60 sec. The retry template has retry max attempt of 3 with 2 secs delay (with delay
   * multiplier as attempt increases) between each retry. So the overall retry process should
   * complete within 60 sec.
   */
  private static final int READ_REQUEST_TIMEOUT_SECONDS = 30;

  /** Write request timeout is 60 secs due to no retries on table write operations */
  private static final int WRITE_REQUEST_TIMEOUT_SECONDS = 60;

  @Autowired private UserTableApi apiInstance;

  @Autowired private HouseTableMapper houseTableMapper;

  private volatile RetryTemplate retryTemplate;

  /**
   * The retry policy of this template shall only react to exceptions under
   * com.linkedin.openhouse.internal.catalog.repository.exception
   *
   * <p>Note that the {@link HouseTableConcurrentUpdateException} cannot be retried to ensure
   * concurrent exception propagated to client side for examination. However, the retryTemplate
   * disallow declaring retryOn and notRetryOn at the same time.
   */
  @VisibleForTesting
  protected synchronized RetryTemplate getHtsRetryTemplate(
      List<Class<? extends Throwable>> throwables) {
    if (retryTemplate == null) {
      RetryTemplateBuilder builder = new RetryTemplateBuilder();
      // Timeout on Mono block/blockOptional method throws java.lang.IllegalStateException on
      // timeout
      // hence retry is added for IllegalStateException
      retryTemplate =
          builder
              .maxAttempts(HtsRetryUtils.MAX_RETRY_ATTEMPT)
              .customBackoff(HtsRetryUtils.DEFAULT_HTS_BACKOFF_POLICY)
              .retryOn(throwables)
              .build();
    }
    return retryTemplate;
  }

  @Override
  public List<HouseTable> findAllByDatabaseId(String databaseId) {
    Map<String, String> params = new HashMap<>();
    if (Strings.isNotEmpty(databaseId)) {
      params.put("databaseId", databaseId);
    }

    return getHtsRetryTemplate(
            Arrays.asList(
                HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
        .execute(
            context ->
                apiInstance
                    .getUserTables(params)
                    .map(GetAllEntityResponseBodyUserTable::getResults)
                    .flatMapMany(Flux::fromIterable)
                    .map(houseTableMapper::toHouseTable)
                    .collectList()
                    .block(Duration.ofSeconds(READ_REQUEST_TIMEOUT_SECONDS)));
  }

  @Override
  public Page<HouseTable> findAllByDatabaseId(String databaseId, Pageable pageable) {
    Map<String, String> params = new HashMap<>();
    if (Strings.isNotEmpty(databaseId)) {
      params.put("databaseId", databaseId);
    }

    GetAllEntityResponseBodyUserTable result =
        getHtsRetryTemplate(
                Arrays.asList(
                    HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
            .execute(
                context ->
                    apiInstance
                        .getPaginatedUserTables(
                            params,
                            pageable.getPageNumber(),
                            pageable.getPageSize(),
                            getSortByStr(pageable))
                        .block(Duration.ofSeconds(READ_REQUEST_TIMEOUT_SECONDS)));

    Page<UserTable> userTablePage = getUserTablePageFromPageUserTable(result.getPageResults());
    return userTablePage.map(houseTableMapper::toHouseTable);
  }

  private Page<UserTable> getUserTablePageFromPageUserTable(PageUserTable pageResults) {
    return new PageImpl<>(
        pageResults.getContent(),
        PageRequest.of(pageResults.getNumber(), pageResults.getSize()),
        pageResults.getTotalElements());
  }

  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
      value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
      justification = "Handled in switchIfEmpty")
  @Override
  public HouseTable save(HouseTable entity) {
    CreateUpdateEntityRequestBodyUserTable requestBody =
        new CreateUpdateEntityRequestBodyUserTable().entity(houseTableMapper.toUserTable(entity));

    return apiInstance
        .putUserTable(requestBody)
        .map(EntityResponseBodyUserTable::getEntity)
        .map(houseTableMapper::toHouseTable)
        .onErrorResume(this::handleHtsHttpError)
        .block(Duration.ofSeconds(WRITE_REQUEST_TIMEOUT_SECONDS));
  }

  @Override
  public Optional<HouseTable> findById(HouseTablePrimaryKey houseTablePrimaryKey) {

    return getHtsRetryTemplate(
            Arrays.asList(
                HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
        .execute(
            context ->
                apiInstance
                    .getUserTable(
                        houseTablePrimaryKey.getDatabaseId(), houseTablePrimaryKey.getTableId())
                    .map(EntityResponseBodyUserTable::getEntity)
                    .map(houseTableMapper::toHouseTable)
                    .switchIfEmpty(Mono.empty())
                    .onErrorResume(this::handleHtsHttpError)
                    .blockOptional(Duration.ofSeconds(READ_REQUEST_TIMEOUT_SECONDS)));
  }

  /**
   * Translation layer for {@link WebClientResponseException} to typed exceptions that are
   * compatible in the context of {@link HouseTableRepository}, i.e. either {@link
   * com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog}, or {@link
   * OpenHouseInternalTableOperations}.
   */
  private Mono<? extends HouseTable> handleHtsHttpError(Throwable e) {
    if (e instanceof WebClientResponseException.NotFound) {
      return Mono.error(new HouseTableNotFoundException("", e));
    } else if (e instanceof WebClientResponseException.Conflict) {
      return Mono.error(new HouseTableConcurrentUpdateException("", e));
    } else if (e instanceof WebClientResponseException.BadRequest
        || e instanceof WebClientResponseException.Forbidden
        || e instanceof WebClientResponseException.Unauthorized
        || e instanceof WebClientResponseException.TooManyRequests) {
      return Mono.error(
          new HouseTableCallerException(
              "[Client side failure]Error status code for HTS:"
                  + ((WebClientResponseException) e).getStatusCode(),
              e));
    } else if (e instanceof WebClientResponseException
        && ((WebClientResponseException) e).getStatusCode().is5xxServerError()) {
      return Mono.error(
          new HouseTableRepositoryStateUnknownException(
              "Cannot determine if HTS has persisted the proposed change", e));
    } else if (ExceptionUtils.indexOfThrowable(e, DnsNameResolverTimeoutException.class)
        != -1) { // DnsNameResolverTimeoutException appears nested within exception causes and
      // ExceptionUtils class is used to match the occurrence of this failure. Retry is done
      // for this failure using existing retry template.
      return Mono.error(
          new HouseTableRepositoryStateUnknownException(
              "HTS service could not be resolved due to DNS lookup timeout", e));
    } else {
      return Mono.error(new RuntimeException("UNKNOWN and unhandled failure from HTS:", e));
    }
  }

  @Override
  public void deleteById(HouseTablePrimaryKey houseTablePrimaryKey) {
    // Default to hard delete (purge = true) for backward compatibility
    deleteById(houseTablePrimaryKey, true);
  }

  @Override
  public void deleteById(HouseTablePrimaryKey houseTablePrimaryKey, boolean purge) {
    getHtsRetryTemplate(Arrays.asList(IllegalStateException.class))
        .execute(
            context ->
                apiInstance
                    .deleteTable(
                        houseTablePrimaryKey.getDatabaseId(),
                        houseTablePrimaryKey.getTableId(),
                        !purge)
                    .onErrorResume(e -> handleHtsHttpError(e).then())
                    .block());
  }

  @Override
  public void rename(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String metadataLocation) {
    getHtsRetryTemplate(Arrays.asList(IllegalStateException.class))
        .execute(
            context ->
                apiInstance
                    .renameTable(
                        fromDatabaseId, fromTableId, toDatabaseId, toTableId, metadataLocation)
                    .onErrorResume(e -> handleHtsHttpError(e).then())
                    .block());
  }

  @Override
  public <S extends HouseTable> Iterable<S> saveAll(Iterable<S> entities) {
    throw new UnsupportedOperationException("saveAll is not supported.");
  }

  /* ----  Implement the following as needed. ---- */

  @Override
  public boolean existsById(HouseTablePrimaryKey houseTablePrimaryKey) {
    throw new UnsupportedOperationException("existsById is not supported.");
  }

  @Override
  public Iterable<HouseTable> findAll() {
    return getHtsRetryTemplate(
            Arrays.asList(
                HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
        .execute(
            context ->
                apiInstance
                    .getUserTables(new HashMap<>())
                    .map(GetAllEntityResponseBodyUserTable::getResults)
                    .flatMapMany(Flux::fromIterable)
                    .map(houseTableMapper::toHouseTableWithDatabaseId)
                    .collectList()
                    .block(Duration.ofSeconds(READ_REQUEST_TIMEOUT_SECONDS)));
  }

  @Override
  public Page<HouseTable> findAll(Pageable pageable) {
    GetAllEntityResponseBodyUserTable result =
        getHtsRetryTemplate(
                Arrays.asList(
                    HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
            .execute(
                context ->
                    apiInstance
                        .getPaginatedUserTables(
                            new HashMap<>(),
                            pageable.getPageNumber(),
                            pageable.getPageSize(),
                            getSortByStr(pageable))
                        .block(Duration.ofSeconds(READ_REQUEST_TIMEOUT_SECONDS)));

    Page<UserTable> userTablePage = getUserTablePageFromPageUserTable(result.getPageResults());
    return userTablePage.map(houseTableMapper::toHouseTableWithDatabaseId);
  }

  @Override
  public Page<HouseTable> findAll(Sort sort) {
    throw new UnsupportedOperationException("FindAll by Sort is not supported.");
  }

  @Override
  public Iterable<HouseTable> findAllById(Iterable<HouseTablePrimaryKey> houseTablePrimaryKeys) {
    throw new UnsupportedOperationException("FindAllById is not supported.");
  }

  @Override
  public long count() {
    throw new UnsupportedOperationException("Count is not supported.");
  }

  @Override
  public void delete(HouseTable entity) {
    throw new UnsupportedOperationException("Entity deletion is not supported.");
  }

  @Override
  public void deleteAllById(Iterable<? extends HouseTablePrimaryKey> houseTablePrimaryKeys) {
    throw new UnsupportedOperationException("deleteAllById is not supported.");
  }

  @Override
  public void deleteAll(Iterable<? extends HouseTable> entities) {
    throw new UnsupportedOperationException("Entity-deleteAll is not supported.");
  }

  @Override
  public void deleteAll() {
    throw new UnsupportedOperationException("deleteAll is not supported.");
  }

  @Override
  public Page<HouseTable> searchSoftDeletedTables(
      String databaseId, String tableId, int page, int pageSize, String sortBy) {
    GetAllEntityResponseBodyUserTable userTableResults =
        getHtsRetryTemplate(
                Arrays.asList(
                    HouseTableRepositoryStateUnknownException.class, IllegalStateException.class))
            .execute(
                context ->
                    apiInstance
                        .getSoftDeletedUserTables(databaseId, tableId, null, page, pageSize, sortBy)
                        .block());

    return generatePageFromResults(userTableResults.getPageResults());
  }

  @Override
  public void purgeSoftDeletedTables(String databaseId, String tableId, long purgeAfterMs) {
    getHtsRetryTemplate(Arrays.asList(IllegalStateException.class))
        .execute(
            context ->
                apiInstance
                    .purgeSoftDeletedUserTables(databaseId, tableId, purgeAfterMs)
                    .onErrorResume(e -> handleHtsHttpError(e).then())
                    .block());
  }

  private Page<HouseTable> generatePageFromResults(PageUserTable pageResults) {
    List<HouseTable> houseTables = new ArrayList<>();
    if (pageResults.getContent() != null) {
      houseTables =
          pageResults.getContent().stream()
              .map(houseTableMapper::toHouseTableWithDatabaseId)
              .collect(Collectors.toList());
    }
    return new PageImpl<>(
        houseTables,
        PageRequest.of(pageResults.getNumber(), pageResults.getSize()),
        pageResults.getTotalElements());
  }
}
