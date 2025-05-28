package com.linkedin.openhouse.jobs.scheduler.tasks;

public enum OperationMode {
  SUBMIT("submit"),
  POLL("poll"),
  SINGLE("single");

  private final String operationMode;

  OperationMode(String operationMode) {
    this.operationMode = operationMode;
  }

  public OperationMode toOperationMode(final String operationMode) {
    return OperationMode.valueOf(operationMode);
  }
}
