package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespacePropertyUpdate;
import java.util.Map;
import java.util.Set;

public interface NamespacesService {
  NamespaceMetadata createNamespace(String namespaceName, Map<String, String> properties)
      throws DuplicateCreationException;

  NamespaceMetadata getNamespace(String namespaceName) throws NotFoundException;

  void deleteNamespace(String namespaceName) throws NotFoundException;

  NamespacePropertyUpdate updateNamespaceProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws NotFoundException;
}
