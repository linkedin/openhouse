package com.linkedin.openhouse.client.ssl;

/** Enum to represent connection strategy such as POOLED and NEW connection */
public enum HttpConnectionStrategy {
  POOLED, // represents pooled http connection
  NEW; // represents new connection

  public static HttpConnectionStrategy fromString(String value) {
    // default to POOLED if null or empty
    if (value == null || value.isEmpty()) {
      return POOLED;
    }
    return Enum.valueOf(HttpConnectionStrategy.class, value.toUpperCase());
  }
}
