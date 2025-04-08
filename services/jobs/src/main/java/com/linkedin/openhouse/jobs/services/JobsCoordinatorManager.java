package com.linkedin.openhouse.jobs.services;

import com.linkedin.openhouse.common.exception.JobEngineException;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.config.JobsEngineProperties;
import com.linkedin.openhouse.jobs.config.JobsProperties;
import com.linkedin.openhouse.jobs.model.EngineType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.util.ReflectionUtils;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JobsCoordinatorManager {
  private final Map<EngineType, HouseJobsCoordinator> coordinators;

  public static JobsCoordinatorManager from(JobsProperties properties) {
    Map<EngineType, HouseJobsCoordinator> map = new HashMap<>();
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
            map.put(EngineType.valueOf(engine.getType()), coordinator);
          }
        }
        log.warn(
            String.format(
                "Could not load class or find its constructor: %s, using Livy coordinator by default",
                coordinatorClassName));
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        log.warn(
            String.format(
                "Could not create instance of class: %s, using Livy coordinator by default",
                coordinatorClassName),
            e);
      }
    }
    return new JobsCoordinatorManager(map);
  }

  public HouseJobHandle submit(JobLaunchConf conf) {
    final EngineType engineType = EngineType.valueOf(conf.getEngineType());
    final HouseJobsCoordinator coordinator = coordinators.get(engineType);
    if (coordinator == null) {
      throw new JobEngineException(
          String.format("No coordinator found for engine type: %s", engineType));
    }
    log.info("Submitting job to {} with args: {}", engineType, conf.getArgs());
    return coordinator.submit(conf);
  }

  public HouseJobHandle obtainHandle(EngineType engineType, String executionId) {
    final HouseJobsCoordinator coordinator = coordinators.get(engineType);
    if (coordinator == null) {
      throw new JobEngineException(
          String.format("No coordinator found for engine type: %s", engineType));
    }
    log.info("Obtaining handle from {} with executionId: {}", engineType, executionId);
    return coordinator.obtainHandle(executionId);
  }
}
