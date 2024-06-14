package com.linkedin.openhouse.jobs.services;

import com.google.common.base.Strings;
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

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JobsRegistry {
  private String storageUri;
  private String authTokenPath;
  private Map<String, JobLaunchConf> jobLaunchDefaultConfByType;

  public JobLaunchConf createLaunchConf(String jobId, JobConf conf) {
    final String type = conf.getJobType().name();
    if (!jobLaunchDefaultConfByType.containsKey(type)) {
      throw new JobEngineException(String.format("Job %s is not supported", type));
    }
    JobLaunchConf defaultConf = jobLaunchDefaultConfByType.get(type);
    Map<String, String> propsMap = defaultConf.getSparkProperties();
    /*
    if properties has authTokenPath, read and set authToken as spark.sql.catalog.openhouse.auth-token
    in properties
    */
    if (authTokenPath != null) {
      propsMap.put("spark.sql.catalog.openhouse.auth-token", getToken(authTokenPath));
    }
    if (MapUtils.isNotEmpty(conf.getExecutionConf())) {
      populateSparkProps(conf.getExecutionConf(), propsMap);
    }
    defaultConf.setSparkProperties(propsMap);
    JobLaunchConf.JobLaunchConfBuilder builder = defaultConf.toBuilder();

    // required arguments
    List<String> extendedArgs =
        new ArrayList<>(Arrays.asList("--jobId", jobId, "--storageURL", storageUri));
    // arguments coming from yaml config
    extendedArgs.addAll(defaultConf.getArgs());
    // runtime arguments provided in the request
    extendedArgs.addAll(conf.getArgs());
    return builder.proxyUser(conf.getProxyUser()).args(extendedArgs).build();
  }

  private void populateSparkProps(
      @NonNull Map<String, String> executionConf, Map<String, String> sparkPropsMap) {
    String memory = executionConf.getOrDefault("memory", null);
    if (!Strings.isNullOrEmpty(memory)) {
      sparkPropsMap.put("spark.driver.memory", memory);
      sparkPropsMap.put("spark.executor.memory", memory);
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
