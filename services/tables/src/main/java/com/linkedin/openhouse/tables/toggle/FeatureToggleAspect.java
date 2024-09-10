package com.linkedin.openhouse.tables.toggle;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.openhouse.cluster.configs.TblPropsToggleRegistry;
import com.linkedin.openhouse.internal.catalog.toggle.IcebergFeatureGate;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.impl.TblPropsEnabler;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.TableIdentifier;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * To use {@link TableFeatureToggle} one can add implementation of feature-toggle checking in this
 * class with different types of advises injected (e.g. {@link Before}).
 */
@Aspect
@Component
@Slf4j
public class FeatureToggleAspect {

  @Autowired private TableFeatureToggle tableFeatureToggle;

  @Autowired private TblPropsToggleRegistry tblPropsToggleRegistry;

  @Around("@annotation(tblPropsEnabler)")
  public boolean checkTblPropEnabled(
      ProceedingJoinPoint proceedingJoinPoint, TblPropsEnabler tblPropsEnabler) throws Throwable {
    if (((MethodSignature) proceedingJoinPoint.getSignature()).getReturnType() == boolean.class
        && proceedingJoinPoint.getArgs()[1] instanceof TableDto
        && proceedingJoinPoint.getArgs()[0] instanceof String) {
      TableDto tableDto = (TableDto) proceedingJoinPoint.getArgs()[1];
      String key = (String) proceedingJoinPoint.getArgs()[0];

      Optional<String> feature = tblPropsToggleRegistry.obtainFeatureByKey(key);
      if (!feature.isPresent()) {
        return (boolean) proceedingJoinPoint.proceed();
      }

      boolean tableFeatureEnabled =
          tableFeatureToggle.isFeatureActivated(
              tableDto.getDatabaseId(), tableDto.getTableId(), feature.get());

      // feature activation overwriting the decision of annotated method
      return ((boolean) proceedingJoinPoint.proceed()) || tableFeatureEnabled;
    } else {
      throw new RuntimeException(
          String.format(
              "Signature of method that annotated with %s is problematic, "
                  + "please check with OpenHouse server implementation for methods with this annotation",
              tblPropsEnabler.getClass().getCanonicalName()));
    }
  }

  @Before("@annotation(featureGate)")
  public void checkIcebergFeatureFlag(JoinPoint joinPoint, IcebergFeatureGate featureGate) {
    TableIdentifier tableIdentifier = obtainIdFromAnnotationArgs(joinPoint);

    if (tableFeatureToggle.isFeatureActivated(
        tableIdentifier.namespace().toString(), tableIdentifier.name(), featureGate.value())) {
      throw new ResourceGatedByToggledOnFeatureException(
          featureGate.value(), tableIdentifier.namespace().toString(), tableIdentifier.name());
    }
  }

  /** Obtain {@link TableIdentifier} from the list of arguments. */
  @VisibleForTesting
  TableIdentifier obtainIdFromAnnotationArgs(JoinPoint joinPoint) {
    for (Object arg : joinPoint.getArgs()) {
      if (arg instanceof TableIdentifier) {
        return (TableIdentifier) arg;
      }
    }

    // TODO:
    // * Enhance exception handling together with ResourceGatedByToggledOnFeatureException using
    // exception-handler,
    // right now it just pops as internal server error *
    throw new RuntimeException(
        "(Server error)Cannot find TableIdentifier from the method annotated by IcebergFeatureGate");
  }
}
