package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.checked.NamespaceException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface NamespacesService {
  NamespaceMetadata createNamespace(
      List<String> namespace, Map<String, String> properties, String principal)
      throws NamespaceException;

  NamespaceMetadata loadNamespaceMetadata(String namespace, String principal)
      throws NamespaceException;

  NamespacePropertyUpdate updateNamespaceProperties(
      String namespace, Map<String, String> updates, Set<String> removals, String principal)
      throws NamespaceException;

  void dropNamespace(String namespace, String principal) throws NamespaceException;
}
