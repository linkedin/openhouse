package com.linkedin.openhouse.tables.toggle;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.openhouse.internal.catalog.toggle.IcebergFeatureGate;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.TableIdentifier;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * To use {@link TableFeatureToggle} one can add implementation of feature-toggle checking in this
 * class with different types of advises injected (e.g. {@link Before}).
 */
@Aspect
@Component
@Slf4j
public class FeatureGateAspect {

  @Autowired private TableFeatureToggle tableFeatureToggle;

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
