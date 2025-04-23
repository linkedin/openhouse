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

  public static JobsCoordinatorManager from(JobsProperties properties) {
    Map<String, HouseJobsCoordinator> map = new HashMap<>();
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
            map.put(engine.getEngineType(), coordinator);
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
    return new JobsCoordinatorManager(map, properties.getDefaultEngine());
  }

  public HouseJobHandle submit(JobLaunchConf conf) {
    String engineType = conf.getEngineType();
    if (!coordinators.containsKey(engineType)) {
      throw new JobEngineException(
          String.format("No coordinator found for engine type: %s", engineType));
    }
    HouseJobsCoordinator coordinator = coordinators.get(engineType);
    log.info("Submitting job to {} with args: {}", engineType, conf.getArgs());
    return coordinator.submit(conf);
  }

  public HouseJobHandle obtainHandle(String engineType, String executionId) {
    // for backward compatibility
    if (engineType == null) {
      log.warn(
          String.format("No engine type provided, using default engine: %s", defaultEngineType));
      engineType = defaultEngineType;
    } else if (!coordinators.containsKey(engineType)) {
      throw new JobEngineException(
          String.format("No coordinator found for engine type: %s", engineType));
    }
    HouseJobsCoordinator coordinator = coordinators.get(engineType);
    log.info("Obtaining handle from {} with executionId: {}", engineType, executionId);
    return coordinator.obtainHandle(executionId);
  }
}
