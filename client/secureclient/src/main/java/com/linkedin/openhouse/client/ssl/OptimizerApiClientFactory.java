package com.linkedin.openhouse.client.ssl;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.optimizer.client.invoker.ApiClient;
import java.net.MalformedURLException;
import java.text.DateFormat;
import javax.net.ssl.SSLException;
import lombok.NonNull;
import org.springframework.web.reactive.function.client.WebClient;

/** Factory to create optimizer-specific {@link ApiClient}. Mirrors {@link JobsApiClientFactory}. */
public final class OptimizerApiClientFactory extends WebClientFactory {

  private static OptimizerApiClientFactory instance;

  private OptimizerApiClientFactory() {
    super();
  }

  public static synchronized OptimizerApiClientFactory getInstance() {
    if (null == instance) {
      instance = new OptimizerApiClientFactory();
    }
    return instance;
  }

  @Override
  protected WebClient.Builder createWebClientBuilder() {
    DateFormat defaultDateFormat = ApiClient.createDefaultDateFormat();
    ObjectMapper defaultObjectMapper =
        ApiClient.createDefaultObjectMapper(defaultDateFormat)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return ApiClient.buildWebClientBuilder(defaultObjectMapper);
  }

  /**
   * Creates the optimizer-specific {@link ApiClient} that the generated {@code
   * TableOperationsControllerApi} / {@code TableStatsControllerApi} / {@code
   * TableOperationsHistoryControllerApi} wrap.
   */
  public ApiClient createApiClient(@NonNull String baseUrl, String token, String truststoreLocation)
      throws MalformedURLException, SSLException {
    WebClient webClient = createWebClient(baseUrl, token, truststoreLocation);
    ApiClient apiClient = new ApiClient(webClient);
    apiClient.setBasePath(baseUrl);
    return apiClient;
  }
}
