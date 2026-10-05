package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.jobs.util.RetryUtil;
import lombok.Builder;
import lombok.Getter;
import org.springframework.retry.support.RetryTemplate;

/** Options for {@link TablesClientFactory#create(TablesClientOptions)}. */
@Builder
@Getter
public class TablesClientOptions {
  /** Declares every request as a SYSTEM action. */
  private final boolean systemAction;

  @Builder.Default
  private final RetryTemplate retryTemplate = RetryUtil.getTablesApiRetryTemplate();
}
