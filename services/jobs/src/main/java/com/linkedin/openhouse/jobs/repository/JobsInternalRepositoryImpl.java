package com.linkedin.openhouse.jobs.repository;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.housetables.client.api.JobApi;
import com.linkedin.openhouse.housetables.client.model.CreateUpdateEntityRequestBodyJob;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyJob;
import com.linkedin.openhouse.housetables.client.model.GetAllEntityResponseBodyJob;
import com.linkedin.openhouse.housetables.client.model.Job;
import com.linkedin.openhouse.internal.catalog.repository.HtsRetryUtils;
import com.linkedin.openhouse.jobs.dto.mapper.JobsMapper;
import com.linkedin.openhouse.jobs.model.JobDto;
import com.linkedin.openhouse.jobs.model.JobDtoPrimaryKey;
import com.linkedin.openhouse.jobs.repository.exception.JobsInternalRepositoryTimeoutException;
import com.linkedin.openhouse.jobs.repository.exception.JobsInternalRepositoryUnavailableException;
import com.linkedin.openhouse.jobs.repository.exception.JobsTableCallerException;
import com.linkedin.openhouse.jobs.repository.exception.JobsTableConcurrentUpdateException;
import io.netty.resolver.dns.DnsNameResolverTimeoutException;
import java.time.Duration;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class JobsInternalRepositoryImpl implements JobsInternalRepository {

  @Autowired private JobApi jobApi;

  @Autowired private JobsMapper jobsMapper;

  private volatile RetryTemplate retryTemplate;

  /**
   * The request timeout is decided based on retry template logic and server side gateway timeout of
   * 60 sec. The retry template has retry max attempt of 3 with 2 secs delay (with delay multiplier
   * as attempt increases) between each retry. So the overall retry process should complete within
   * 60 sec.
   */
  private static final int REQUEST_TIMEOUT_SECONDS = 17;

  /**
   * The retry policy of this template shall only allow retry on exceptions under
   * com.linkedin.openhouse.jobs.repository.exception
   */
  public synchronized RetryTemplate getHtsRetryTemplate() {
    if (retryTemplate == null) {
      RetryTemplateBuilder builder = new RetryTemplateBuilder();
      // Timeout on Mono block/blockOptional method throws java.lang.IllegalStateException on
      // timeout
      // hence retry is added for IllegalStateException
      retryTemplate =
          builder
              .maxAttempts(HtsRetryUtils.MAX_RETRY_ATTEMPT)
              .customBackoff(HtsRetryUtils.DEFAULT_HTS_BACKOFF_POLICY)
              .retryOn(JobsInternalRepositoryTimeoutException.class)
              .retryOn(JobsInternalRepositoryUnavailableException.class)
              .retryOn(IllegalStateException.class)
              .build();
    }
    return retryTemplate;
  }

  @Override
  public JobDto save(JobDto entity) {
    CreateUpdateEntityRequestBodyJob createUpdateEntityRequestBodyJob =
        new CreateUpdateEntityRequestBodyJob();
    createUpdateEntityRequestBodyJob.setEntity(
        jobsMapper.toJob(entity).version(getCurrentVersion(entity.getJobId()).orElse(null)));

    return getHtsRetryTemplate()
        .execute(
            context ->
                jobApi
                    .putJob(createUpdateEntityRequestBodyJob)
                    .map(EntityResponseBodyJob::getEntity)
                    .mapNotNull(jobsMapper::toJobDto)
                    .onErrorResume(this::handleHtsHttpError)
                    .blockOptional(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS))
                    .get());
  }

  @Override
  public Optional<JobDto> findById(JobDtoPrimaryKey jobDtoPrimaryKey) {
    return getHtsRetryTemplate()
        .execute(
            context ->
                jobApi
                    .getJob(jobDtoPrimaryKey.getJobId())
                    .map(EntityResponseBodyJob::getEntity)
                    .mapNotNull(jobsMapper::toJobDto)
                    .switchIfEmpty(Mono.empty())
                    .onErrorResume(this::handleHtsHttpError)
                    .blockOptional(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)));
  }

  @Override
  public boolean existsById(JobDtoPrimaryKey jobDtoPrimaryKey) {
    return findById(jobDtoPrimaryKey).isPresent();
  }

  @Override
  public Iterable<JobDto> findAll() {
    return getHtsRetryTemplate()
        .execute(
            context ->
                jobApi
                    .getAllJobs(ImmutableMap.of())
                    .map(GetAllEntityResponseBodyJob::getResults)
                    .flatMapMany(Flux::fromIterable)
                    .map(jobsMapper::toJobDto)
                    .onErrorResume(this::handleHtsHttpError)
                    .collectList()
                    .blockOptional(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS))
                    .get());
  }

  private Optional<String> getCurrentVersion(String jobId) {
    return getHtsRetryTemplate()
        .execute(
            context ->
                jobApi
                    .getJob(jobId)
                    .map(EntityResponseBodyJob::getEntity)
                    .mapNotNull(Job::getVersion)
                    .switchIfEmpty(Mono.empty())
                    .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.empty())
                    .blockOptional(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)));
  }

  /**
   * Layer to map web client exceptions {@link WebClientResponseException} to Jobs-table typed
   * exceptions compatible in the context of {@link JobsInternalRepository} actions
   */
  private Mono<? extends JobDto> handleHtsHttpError(Throwable e) {
    if (e instanceof WebClientResponseException.NotFound) {
      return Mono.empty();
    } else if (e instanceof WebClientResponseException.Conflict) {
      return Mono.error(new JobsTableConcurrentUpdateException(e.getMessage(), e));
    } else if (e instanceof WebClientResponseException.BadRequest
        || e instanceof WebClientResponseException.Forbidden
        || e instanceof WebClientResponseException.Unauthorized
        || e instanceof WebClientResponseException.TooManyRequests) {
      return Mono.error(
          new JobsTableCallerException(
              "[Client side failure]Error status code for HTS:"
                  + ((WebClientResponseException) e).getStatusCode(),
              e));
    } else if (e instanceof WebClientResponseException.InternalServerError
        || e instanceof WebClientResponseException.ServiceUnavailable
        || e instanceof WebClientResponseException.BadGateway) {
      return Mono.error(
          new JobsInternalRepositoryUnavailableException(
              "[Server side failure]Error status code for HTS:"
                  + ((WebClientResponseException) e).getStatusCode(),
              e));
    } else if (e instanceof WebClientResponseException.GatewayTimeout) {
      return Mono.error(
          new JobsInternalRepositoryTimeoutException(
              "Cannot determine if HTS has persisted the proposed change", e));
    } else if (ExceptionUtils.indexOfThrowable(e, DnsNameResolverTimeoutException.class)
        != -1) { // DnsNameResolverTimeoutException appears nested within exception causes and
      // ExceptionUtils class is used to match the occurrence of this failure. Retry is done
      // for this failure using existing retry template.
      return Mono.error(
          new JobsInternalRepositoryTimeoutException(
              "HTS service could not be resolved due to DNS lookup timeout", e));
    } else {
      return Mono.error(new RuntimeException("UNKNOWN and unhandled failure from HTS:", e));
    }
  }

  /* ----  Implement the following as needed. ---- */

  @Override
  public Iterable<JobDto> findAllById(Iterable<JobDtoPrimaryKey> jobDtoPrimaryKeys) {
    throw new UnsupportedOperationException("findAllById is not supported.");
  }

  @Override
  public <S extends JobDto> Iterable<S> saveAll(Iterable<S> entities) {
    throw new UnsupportedOperationException("saveAll is not supported.");
  }

  @Override
  public long count() {
    throw new UnsupportedOperationException("Count is not supported.");
  }

  @Override
  public void deleteById(JobDtoPrimaryKey jobDtoPrimaryKey) {
    throw new UnsupportedOperationException("deleteById is not supported.");
  }

  @Override
  public void delete(JobDto entity) {
    throw new UnsupportedOperationException("Entity deletion is not supported.");
  }

  @Override
  public void deleteAllById(Iterable<? extends JobDtoPrimaryKey> jobDtoPrimaryKeys) {
    throw new UnsupportedOperationException("deleteAllById is not supported.");
  }

  @Override
  public void deleteAll(Iterable<? extends JobDto> entities) {
    throw new UnsupportedOperationException("Entity-deleteAll is not supported.");
  }

  @Override
  public void deleteAll() {
    throw new UnsupportedOperationException("deleteAll is not supported.");
  }
}
