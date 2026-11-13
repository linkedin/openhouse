package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.common.exception.checked.DomainException;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespacePropertyUpdate;
import java.util.Map;
import java.util.Set;

public interface NamespacesService {
  NamespaceMetadata createNamespace(String namespaceName, Map<String, String> properties)
      throws DomainException;

  NamespaceMetadata getNamespace(String namespaceName) throws DomainException;

  void deleteNamespace(String namespaceName) throws DomainException;

  NamespacePropertyUpdate updateNamespaceProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws DomainException;
}
