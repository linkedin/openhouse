package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.exception.TableValidationException;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.TableStateValidator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import lombok.extern.slf4j.Slf4j;

/** Base table app class implemented for table specific operations */
@Slf4j
public abstract class BaseTableSparkApp extends BaseSparkApp {
  protected final String fqtn;

  protected BaseTableSparkApp(String jobId, StateManager stateManager, String fqtn) {
    super(jobId, stateManager);
    this.fqtn = fqtn;
  }

  @Override
  protected void runValidations(Operations ops) {
    String className = this.getClass().getSimpleName();
    try {
      log.info("Starting post job validations for OH table {}", fqtn);
      TableStateValidator.run(ops.spark(), fqtn);
      log.info("Post job validations for OH table {} successful", fqtn);
    } catch (TableValidationException e) {
      log.error(
          String.format(
              "Post job validations for OH table %s failed with error %s", fqtn, e.getMessage()),
          e);
      otelEmitter.count(
          METRICS_SCOPE,
          "post_run_validation_error",
          1,
          Attributes.of(
              AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn,
              AttributeKey.stringKey(AppConstants.JOB_NAME), className));
      throw e;
    }
  }
}
