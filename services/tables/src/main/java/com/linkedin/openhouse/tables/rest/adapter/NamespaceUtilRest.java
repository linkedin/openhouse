package com.linkedin.openhouse.tables.rest.adapter;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;

/**
 * Helpers for translating between Iceberg REST namespace path variables and {@link Namespace}
 * objects. OpenHouse only supports single-level namespaces ("databases"); this helper enforces
 * that.
 */
public final class NamespaceUtilRest {

  private NamespaceUtilRest() {}

  /** Decode an Iceberg REST path-variable (unit-separator delimited) into a {@link Namespace}. */
  public static Namespace decode(String pathVariable) {
    if (pathVariable == null || pathVariable.isEmpty()) {
      throw new RequestValidationFailureException("namespace path variable is empty");
    }
    return RESTUtil.decodeNamespace(pathVariable);
  }

  /** Encode a {@link Namespace} back to a path-variable string. */
  public static String encode(Namespace ns) {
    return RESTUtil.encodeNamespace(ns);
  }

  /**
   * Reject multi-level namespaces — OpenHouse uses a single database level. Empty namespaces are
   * also rejected.
   */
  public static void requireDepthOne(Namespace ns) {
    if (ns == null || ns.isEmpty()) {
      throw new RequestValidationFailureException(
          "namespace must have exactly one level; got empty namespace");
    }
    if (ns.length() > 1) {
      throw new RequestValidationFailureException(
          "multi-level namespaces are not supported; depth=" + ns.length());
    }
  }

  /**
   * Return the single database level. Caller must have run {@link #requireDepthOne(Namespace)}
   * first; this method does so defensively as well.
   */
  public static String singleLevelDb(Namespace ns) {
    requireDepthOne(ns);
    return ns.level(0);
  }
}
