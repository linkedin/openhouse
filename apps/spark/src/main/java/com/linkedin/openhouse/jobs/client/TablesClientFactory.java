package com.linkedin.openhouse.jobs.client;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.openhouse.client.ssl.TablesApiClientFactory;
import com.linkedin.openhouse.jobs.util.DatabaseTableFilter;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import com.linkedin.openhouse.tables.client.api.DatabaseApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import java.net.MalformedURLException;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.support.RetryTemplate;

/** A factory class for {@link TablesClient}. */
@Slf4j
@AllArgsConstructor
public class TablesClientFactory {
  private final String basePath;
  private final DatabaseTableFilter filter;
  private final @Nullable String token;

  public TablesClient create() {
    return create(RetryUtil.getTablesApiRetryTemplate());
  }

  public TablesClient create(RetryTemplate retryTemplate) {
    ApiClient client = null;
    try {
      client = TablesApiClientFactory.getInstance().createApiClient(basePath, token, null);
    } catch (MalformedURLException | SSLException e) {
      throw new RuntimeException(
          "Tables Client initialization failed: Failure while initializing ApiClient", e);
    }
    client.setBasePath(basePath);
    return create(retryTemplate, new TableApi(client), new DatabaseApi(client));
  }

  @VisibleForTesting
  public TablesClient create(RetryTemplate retryTemplate, TableApi tableApi, DatabaseApi dbApi) {
    return new TablesClient(retryTemplate, tableApi, dbApi, filter);
  }
}
