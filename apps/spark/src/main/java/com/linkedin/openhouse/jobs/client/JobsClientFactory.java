package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.client.ssl.JobsApiClientFactory;
import com.linkedin.openhouse.jobs.client.api.JobApi;
import com.linkedin.openhouse.jobs.client.invoker.ApiClient;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import java.net.MalformedURLException;
import javax.net.ssl.SSLException;
import lombok.AllArgsConstructor;
import org.springframework.retry.support.RetryTemplate;

/** A factory class for {@link JobsClient}. */
@AllArgsConstructor
public class JobsClientFactory {
  private final String basePath;
  private final String clusterId;

  public JobsClient create() {
    return create(RetryUtil.getJobsApiRetryTemplate());
  }

  public JobsClient create(RetryTemplate retryTemplate) {
    ApiClient client = null;
    try {
      client = JobsApiClientFactory.getInstance().createApiClient(basePath, "", null);
    } catch (MalformedURLException | SSLException e) {
      throw new RuntimeException(
          "Jobs Client initialization failed: Failure while initializing ApiClient", e);
    }
    client.setBasePath(basePath);
    return create(retryTemplate, new JobApi(client));
  }

  public JobsClient create(RetryTemplate retryTemplate, JobApi api) {
    return new JobsClient(retryTemplate, api, clusterId);
  }
}
