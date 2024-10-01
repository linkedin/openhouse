package com.linkedin.openhouse.jobs.services;

import static com.linkedin.openhouse.jobs.model.JobConf.EXECUTION_CONF_KEY_PREFIX;

import com.linkedin.openhouse.common.exception.JobEngineException;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.config.JobsProperties;
import com.linkedin.openhouse.jobs.model.JobConf;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.SerializationUtils;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JobsRegistry {
  private final String storageUri;
  private final String authTokenPath;
  private final Map<String, JobLaunchConf> jobLaunchDefaultConfByType;

  public JobLaunchConf createLaunchConf(String jobId, JobConf requestConf) {
    final String type = requestConf.getJobType().name();
    if (!jobLaunchDefaultConfByType.containsKey(type)) {
      throw new JobEngineException(String.format("Job %s is not supported", type));
    }
    JobLaunchConf extendedRequestConf = createDefaultLaunchConf(requestConf.getJobType());
    if (MapUtils.isNotEmpty(requestConf.getExecutionConf())) {
      populateSparkProperties(
          requestConf.getExecutionConf(), extendedRequestConf.getSparkProperties());
    }
    // required arguments
    List<String> extendedArgs =
        new ArrayList<>(Arrays.asList("--jobId", jobId, "--storageURL", storageUri));
    // arguments coming from yaml config
    extendedArgs.addAll(extendedRequestConf.getArgs());
    // runtime arguments provided in the request
    extendedArgs.addAll(requestConf.getArgs());
    return extendedRequestConf
        .toBuilder()
        .proxyUser(requestConf.getProxyUser())
        .args(extendedArgs)
        .build();
  }

  private JobLaunchConf createDefaultLaunchConf(JobConf.JobType type) {
    // deep copy to avoid modifying the default config bean
    // use serialization as a robust method to deep copy
    return SerializationUtils.clone(jobLaunchDefaultConfByType.get(type.name()));
  }

  private void populateSparkProperties(
      @NonNull Map<String, String> executionConf, Map<String, String> sparkProperties) {
    /*
    if properties has authTokenPath, read and set authToken as spark.sql.catalog.openhouse.auth-token
    in properties
    */
    if (authTokenPath != null) {
      sparkProperties.put("spark.sql.catalog.openhouse.auth-token", getToken(authTokenPath));
    }

    for (Map.Entry<String, String> entry : executionConf.entrySet()) {
      if (entry.getKey().startsWith(EXECUTION_CONF_KEY_PREFIX)) {
        sparkProperties.put(entry.getKey(), entry.getValue());
      }
    }
  }

  public static JobsRegistry from(JobsProperties properties, Map<String, String> storageProps) {
    Map<String, JobLaunchConf> map = new HashMap<>();
    for (JobLaunchConf conf : properties.getApps()) {
      if (map.containsKey(conf.getType())) {
        throw new RuntimeException(
            String.format("Apps with duplicate types '{%s}' are not allowed", conf.getType()));
      }
      setStorageProviderConf(conf, storageProps);
      map.put(conf.getType(), conf);
    }
    return new JobsRegistry(properties.getStorageUri(), properties.getAuthTokenPath(), map);
  }

  private static void setStorageProviderConf(JobLaunchConf conf, Map<String, String> storageProps) {
    Map<String, String> propsMap = conf.getSparkProperties();
    storageProps.entrySet().stream()
        .iterator()
        .forEachRemaining(entry -> propsMap.put(entry.getKey(), entry.getValue()));
    conf.setSparkProperties(propsMap);
  }

  private String getToken(@NonNull String filePath) {
    Path path = Paths.get(filePath);
    try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      return br.readLine();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Could not read token file %s", filePath), e);
    }
  }
}
