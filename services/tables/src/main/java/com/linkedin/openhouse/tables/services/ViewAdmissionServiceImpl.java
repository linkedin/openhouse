package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;

/**
 * M1 pass-through {@link ViewAdmissionService}: never fails. Gated to the Iceberg-view-capable
 * runtime like the rest of the bridge.
 */
@Component
@ConditionalOnClass(name = "org.apache.iceberg.view.ViewMetadata")
public class ViewAdmissionServiceImpl implements ViewAdmissionService {

  @Override
  public void admit(CreateUpdateViewRequestBody requestBody) {
    // Pass-through for M1. Future Spark/Trino validation and Coral generation attach here.
  }
}
