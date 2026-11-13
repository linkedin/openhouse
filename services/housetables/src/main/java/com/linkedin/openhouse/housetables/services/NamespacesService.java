package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.common.exception.checked.NamespaceException;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespacePropertyUpdate;
import java.util.Map;
import java.util.Set;

public interface NamespacesService {
  NamespaceMetadata createNamespace(String namespaceName, Map<String, String> properties)
      throws NamespaceException;

  NamespaceMetadata getNamespace(String namespaceName) throws NamespaceException;

  void deleteNamespace(String namespaceName) throws NamespaceException;

  NamespacePropertyUpdate updateNamespaceProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws NamespaceException;
}
