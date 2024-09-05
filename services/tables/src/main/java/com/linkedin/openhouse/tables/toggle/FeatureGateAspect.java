package com.linkedin.openhouse.tables.toggle;

import com.linkedin.openhouse.internal.catalog.toggle.IcebergFeatureGate;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.TableIdentifier;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Slf4j
public class FeatureGateAspect {

  @Autowired private TableFeatureToggle tableFeatureToggle;

  @Before("@annotation(featureGate)")
  public void checkIcebergFeatureFlag(JoinPoint joinPoint, IcebergFeatureGate featureGate) {
    TableIdentifier tableIdentifier = (TableIdentifier) joinPoint.getArgs()[0];

    if (tableFeatureToggle.isFeatureActivated(
        tableIdentifier.namespace().toString(), tableIdentifier.name(), featureGate.value())) {
      throw new ResourceGatedByToggledOnFeatureException(
          featureGate.value(), tableIdentifier.namespace().toString(), tableIdentifier.name());
    }
  }
}
