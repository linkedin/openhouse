package com.linkedin.openhouse.common.exception;

import java.util.NoSuchElementException;
import lombok.Getter;

/** Exception indicating Job is not found. */
@Getter
public class NoSuchJobException extends NoSuchElementException {
  private String jobId;
  private final Throwable cause;
  private static final String ERROR_MSG_TEMPLATE = "Job $jid cannot be found";

  public NoSuchJobException(String jobId) {
    this(jobId, ERROR_MSG_TEMPLATE.replace("$jid", jobId), null);
  }

  public NoSuchJobException(String jobId, Throwable cause) {
    this(jobId, ERROR_MSG_TEMPLATE.replace("$jid", jobId), cause);
  }

  public NoSuchJobException(String jobId, String errorMsg, Throwable cause) {
    super(errorMsg);
    this.jobId = jobId;
    this.cause = cause;
  }
}
