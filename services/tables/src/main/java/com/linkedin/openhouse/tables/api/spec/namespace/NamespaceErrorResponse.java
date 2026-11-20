package com.linkedin.openhouse.tables.api.spec.namespace;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class NamespaceErrorResponse {
  private final NamespaceError error;

  @Getter
  @AllArgsConstructor
  public static class NamespaceError {
    private final String message;
    private final String type;
    private final int code;
  }
}
