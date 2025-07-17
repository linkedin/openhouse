package com.linkedin.openhouse.jobs.services;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.SerializationUtils;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class JobsRegistry {
  private final String storageUri;
  private final String authTokenPath;
  protected final Map<String, JobLaunchConf> jobLaunchDefaultConfByType;

  public JobLaunchConf createLaunchConf(String jobId, JobConf requestConf) {
    final String type = requestConf.getJobType().name();
    if (!jobLaunchDefaultConfByType.containsKey(type)) {
      throw new JobEngineException(String.format("Job %s is not supported", type));
    }
    JobLaunchConf extendedRequestConf = createDefaultLaunchConf(requestConf.getJobType());
    // spark conf
    populateAllSparkProperties(
        requestConf.getExecutionConf(), extendedRequestConf.getSparkProperties());
    // required arguments
    ArgMap extendedArgs = new ArgMap(Arrays.asList("--jobId", jobId, "--storageURL", storageUri));
    // arguments coming from yaml config
    extendedArgs.update(extendedRequestConf.getArgs());
    // runtime arguments provided in the request
    extendedArgs.update(requestConf.getArgs());
    return extendedRequestConf
        .toBuilder()
        .proxyUser(requestConf.getProxyUser())
        .args(extendedArgs.toStringList())
        .build();
  }

  private JobLaunchConf createDefaultLaunchConf(JobConf.JobType type) {
    // deep copy to avoid modifying the default config bean
    // use serialization as a robust method to deep copy
    return SerializationUtils.clone(jobLaunchDefaultConfByType.get(type.name()));
  }

  private void populateAllSparkProperties(
      @NonNull Map<String, String> executionConf, Map<String, String> sparkProperties) {
    /*
    if properties has authTokenPath, read and set authToken as spark.sql.catalog.openhouse.auth-token
    in properties
    */
    if (authTokenPath != null) {
      sparkProperties.put("spark.sql.catalog.openhouse.auth-token", getToken(authTokenPath));
    }
    if (MapUtils.isNotEmpty(executionConf)) {
      sparkProperties.putAll(executionConf);
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

  public static class ArgMap {
    private final Map<String, String> keyValues = new LinkedHashMap<>();
    private final Set<String> flags = new LinkedHashSet<>();

    /**
     * ArgMap is a utility class for parsing, managing, and updating command-line style arguments.
     * It supports arguments in the form of flags (e.g., "--verbose") and key-value pairs (e.g.,
     * "--mode prod").
     *
     * <p>Flags are stored without associated values and are deduplicated. Key-value pairs are
     * stored in insertion order and can be updated by calling the {@code update()} method. The
     * class provides a {@code toStringList()} method to serialize the arguments back into a {@code
     * List<String>} format, preserving order and structure.
     */
    public ArgMap(List<String> input) {
      update(input);
    }

    public void update(List<String> updates) {
      for (int i = 0; i < updates.size(); i++) {
        String token = updates.get(i);
        if (token.startsWith("--")) {
          String key = token;
          // Remove from both maps/sets in case type changed
          keyValues.remove(key);
          flags.remove(key);

          if (i + 1 < updates.size() && !updates.get(i + 1).startsWith("--")) {
            String value = updates.get(++i);
            keyValues.put(key, value);
          } else {
            flags.add(key);
          }
        }
      }
    }

    /**
     * Returns the value for a given key.
     *
     * @param key Key (e.g., "--jobId")
     * @return Associated value or null if not present.
     */
    public String get(String key) {
      return keyValues.get(key);
    }

    /**
     * Returns the parsed flags.
     *
     * @return Set of flags (e.g., "--verbose").
     */
    public Set<String> getFlags() {
      return Collections.unmodifiableSet(flags);
    }

    /**
     * Returns the parsed key-value pairs.
     *
     * @return Map of arguments.
     */
    public Map<String, String> getArgs() {
      return Collections.unmodifiableMap(keyValues);
    }

    public List<String> toStringList() {
      List<String> result = new ArrayList<>();
      for (Map.Entry<String, String> entry : keyValues.entrySet()) {
        result.add(entry.getKey());
        result.add(entry.getValue());
      }
      for (String flag : flags) {
        result.add(flag);
      }
      return result;
    }
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
