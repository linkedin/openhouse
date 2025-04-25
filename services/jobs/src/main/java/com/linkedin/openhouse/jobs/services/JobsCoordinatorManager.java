package com.linkedin.openhouse.jobs.services;

import com.linkedin.openhouse.common.exception.JobEngineException;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.config.JobsEngineProperties;
import com.linkedin.openhouse.jobs.config.JobsProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.util.ReflectionUtils;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Getter
public class JobsCoordinatorManager {
  private final Map<String, HouseJobsCoordinator> coordinators;
  private final String defaultEngineType;

  /** Initialize coordinators map and default engine type from the jobs properties. */
  public static JobsCoordinatorManager from(JobsProperties properties) {
    Map<String, HouseJobsCoordinator> coordinatorsMap = new HashMap<>();
    for (JobsEngineProperties engine : properties.getEngines()) {
      String coordinatorClassName = engine.getCoordinatorClassName();
      String baseEngineUrl = engine.getEngineUri();
      try {
        Class<?> coordinatorClass =
            ReflectionUtils.loadIfPresent(
                coordinatorClassName, coordinatorClassName.getClass().getClassLoader());
        if (coordinatorClass != null) {
          Optional<Constructor<?>> cons =
              ReflectionUtils.findConstructor(coordinatorClass, baseEngineUrl);
          if (cons.isPresent()) {
            HouseJobsCoordinator coordinator =
                (HouseJobsCoordinator) cons.get().newInstance(baseEngineUrl);
            coordinatorsMap.put(engine.getEngineType(), coordinator);
            log.info("Created coordinator for engine type: {}", engine.getEngineType());
          }
        } else {
          log.warn(
              String.format(
                  "Could not load class or find its constructor: %s", coordinatorClassName));
        }
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        log.warn(String.format("Could not create instance of class: %s", coordinatorClassName), e);
      }
    }
    return new JobsCoordinatorManager(coordinatorsMap, properties.getDefaultEngine());
  }

  /**
   * Given a JobLaunchConf, submits the job using the corresponding coordinator. Will use
   * defaultEngineType is engineType is not provided.
   */
  public HouseJobHandle submit(JobLaunchConf conf) {
    String resolvedEngineType = resolveEngineType(conf.getEngineType());
    HouseJobsCoordinator coordinator = coordinators.get(resolvedEngineType);
    log.info("Submitting job to {} with args: {}", resolvedEngineType, conf.getArgs());
    return coordinator.submit(conf);
  }

  /**
   * Given an executionId, returns a job handle according to the engineType. Will use
   * defaultEngineType is engineType is not provided.
   */
  public HouseJobHandle obtainHandle(String engineType, String executionId) {
    String resolvedEngineType = resolveEngineType(engineType);
    HouseJobsCoordinator coordinator = coordinators.get(resolvedEngineType);
    log.info("Obtaining handle from {} with executionId: {}", resolvedEngineType, executionId);
    return coordinator.obtainHandle(executionId);
  }

  private String resolveEngineType(String engineType) {
    if (engineType == null) {
      log.warn(
          String.format("No engine type provided, using default engine: %s", defaultEngineType));
      return defaultEngineType;
    } else if (!coordinators.containsKey(engineType)) {
      throw new JobEngineException(
          String.format("No coordinator found for engine type: %s", engineType));
    }
    return engineType;
  }
}
