package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespacePropertyUpdate;
import java.util.Map;
import java.util.Set;

public interface NamespacesService {
  NamespaceMetadata createNamespace(String namespaceName, Map<String, String> properties)
      throws NamespaceMetadataException;

  NamespaceMetadata getNamespace(String namespaceName) throws NamespaceMetadataException;

  void deleteNamespace(String namespaceName) throws NamespaceMetadataException;

  NamespacePropertyUpdate updateNamespaceProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws NamespaceMetadataException;
}
