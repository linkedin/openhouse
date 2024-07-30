package com.linkedin.openhouse.tables.config;

import static com.linkedin.openhouse.common.Constants.*;

import com.linkedin.openhouse.cluster.metrics.TagUtils;
import com.linkedin.openhouse.cluster.storage.FsStorageUtils;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.common.config.BaseApplicationConfig;
import com.linkedin.openhouse.common.provider.HttpConnectionPoolProviderConfig;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.springdoc.core.customizers.OpenApiCustomiser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.actuate.metrics.web.servlet.WebMvcTagsContributor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

/** Main Application Configuration to load cluster properties. */
@Configuration
@Slf4j
public class MainApplicationConfig extends BaseApplicationConfig {
  public static final String APP_NAME = "tables";
  private static final Pattern VERSION_PART_PATTERN = Pattern.compile("v[0-9]+");
  private static final int IN_MEMORY_BUFFER_SIZE = 10 * 1000 * 1024;

  private static final int DNS_QUERY_TIMEOUT_SECONDS = 10;

  @Autowired StorageManager storageManager;

  /**
   * When cluster properties are available, obtain hts base URI and inject API client
   * implementation. This also puts availability of HTS as prerequisite for /table services.
   *
   * @return API instance for HTS client.
   */
  @Bean
  public UserTableApi provideApiInstance() {
    String htsBasePath = clusterProperties.getClusterHouseTablesBaseUri();
    // Not able to leverage HousetablesApiClientFactory due to cyclic dependency
    // The default DNS query timeout is 5 sec for NameResolverProvider. Increasing this to 10 sec to
    // reduce intermittent
    // DNS lookup failure with timeout.
    HttpClient httpClient =
        HttpClient.create(
                HttpConnectionPoolProviderConfig.getCustomConnectionProvider(
                    "tables-hts-custom-connection-pool"))
            .resolver(spec -> spec.queryTimeout(Duration.ofSeconds(DNS_QUERY_TIMEOUT_SECONDS)));
    WebClient webClient =
        ApiClient.buildWebClientBuilder()
            .baseUrl(htsBasePath)
            .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(IN_MEMORY_BUFFER_SIZE))
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            .build();
    ApiClient apiClient = new ApiClient(webClient);
    apiClient.setBasePath(htsBasePath);
    return new UserTableApi(apiClient);
  }

  @Bean
  public WebMvcTagsContributor clientIdTagContributor() {
    return new WebMvcTagsContributor() {
      @Override
      public Iterable<Tag> getTags(
          HttpServletRequest request,
          HttpServletResponse response,
          Object handler,
          Throwable exception) {
        String clientName = request.getHeader(HTTPHEADER_CLIENT_NAME);

        return Collections.singletonList(
            Tag.of(
                METRIC_KEY_CLIENT_NAME,
                ALLOWED_CLIENT_NAME_VALUES.contains(clientName)
                    ? clientName
                    : CLIENT_NAME_DEFAULT_VALUE));
      }

      @Override
      public Iterable<Tag> getLongRequestTags(HttpServletRequest request, Object handler) {
        return Collections.emptyList();
      }
    };
  }

  @Bean
  MeterRegistryCustomizer<MeterRegistry> provideMeterRegistry() {
    return registry ->
        registry.config().commonTags(TagUtils.buildCommonTag(clusterProperties, APP_NAME));
  }

  @Bean
  Consumer<Supplier<Path>> provideFileSecurer() {
    return pathSeqSupplier -> {
      try {
        // TODO: This should use high-level storage api such as Storage::secureTableObject.
        if (storageManager.getDefaultStorage().getType().equals(StorageType.HDFS)
            || storageManager.getDefaultStorage().getType().equals(StorageType.LOCAL)) {
          FsStorageUtils.securePath(
              (FileSystem) storageManager.getDefaultStorage().getClient().getNativeClient(),
              pathSeqSupplier.get());
        } else {
          log.warn(
              "No secure path implementation for storage type: {}",
              storageManager.getDefaultStorage().getType());
        }
      } catch (IOException ioe) {
        // Throwing unchecked exception and leave the handling explicitly to the caller.
        throw new UncheckedIOException(
            "Failed to secure paths included in provided snapshot, ", ioe);
      }
    };
  }

  /**
   * A bean object that customizes the operationId in OAS spec in favor of API versioning.
   *
   * <p>The operationid for each method of each path is used by openapi-clientgen library as the
   * method name. By default, operationId is identical to controller's method name that corresponds
   * to the path. For example, `getTable` is the method name that handles GET
   * /databases/{database}/tables/{table}, thus the generated client library will have
   * `tableApi.getTable` that issues the request to the path above.
   *
   * <p>Due to API versioning, there could be multiple paths routing to the same controller method.
   * The default collision resolution assign sequence number to those method based on the order they
   * appear in spec, which introduces the nondeterminism which means `tableApi.getTable` could reach
   * v0 URL while `tableApi.deleteTable` could reach v1 URL.
   *
   * <p>To avoid such, we customize the naming of operationId by obtaining the api version from path
   * and attach it as part of operation Id. For example, for GET
   * /v8/databases/{database}/tables/{table}, the corresponding method name will be getTableV8
   */
  @Bean
  OpenApiCustomiser springDocOperationIdCustomizer() {
    return openApi -> {
      Map<String, String> pathToPostfix =
          generatePostfix(
              openApi.getPaths().keySet().stream().map(Paths::get).collect(Collectors.toSet()));

      openApi
          .getPaths()
          .forEach(
              (path, pathItem) ->
                  pathItem
                      .readOperations()
                      .forEach(
                          operation ->
                              operation.setOperationId(
                                  sanitizeOpId(operation.getOperationId())
                                      + pathToPostfix.get(path))));
    };
  }

  /**
   * Generate postfix for the customized operationId using the versioning contained as part of
   * paths. The result mapping is: Raw Path --> the version that should be attached to operationId.
   *
   * @param pathKeys All paths observed within single spring application.
   * @return mapping between raw path to its postfix used in customized operationId.
   */
  @VisibleForTesting
  static Map<String, String> generatePostfix(Set<java.nio.file.Path> pathKeys) {
    Map<String, String> rawPathToMethodVer = new HashMap<>();
    for (java.nio.file.Path path : pathKeys) {

      java.nio.file.Path firstLevel = path.subpath(0, 1);
      String version = isVersion(firstLevel.toString()) ? firstLevel.toString() : "";
      rawPathToMethodVer.put(path.toString(), version.toUpperCase());
    }

    return rawPathToMethodVer;
  }

  @VisibleForTesting
  static boolean isVersion(String component) {
    return VERSION_PART_PATTERN.matcher(component).matches();
  }

  /**
   * By default springdoc adds "_" and a sequence number to operation Id(method name in controller)
   * when name collision occurs. Since we are going to customize the postfix here, such component
   * shall be removed.
   *
   * <p>Note that we assume only single "_" appear in the default operationId. We also don't allow
   * operationId starting with underscore. If that assumption doesn't hold, an {@link
   * IllegalStateException} is thrown and naming in {@link
   * com.linkedin.openhouse.tables.controller.TablesController} or other controllers will have to be
   * fixed.
   */
  @VisibleForTesting
  static String sanitizeOpId(String operationId) {
    if (operationId.startsWith("_") || operationId.indexOf("_") != operationId.lastIndexOf("_")) {
      throw new IllegalStateException(
          String.format(
              "Please check the corresponding controller method of %s; Method name in controller may contain underscore illegally.",
              operationId));
    }

    return operationId.contains("_")
        ? operationId.substring(0, operationId.indexOf("_"))
        : operationId;
  }
}
