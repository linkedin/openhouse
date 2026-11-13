package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.common.exception.checked.NamespaceException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import java.util.Map;
import java.util.Set;

public interface HouseNamespaceRepository {
  NamespaceMetadata create(String namespaceName, Map<String, String> properties)
      throws NamespaceException;

  NamespaceMetadata getByName(String namespaceName) throws NamespaceException;

  void delete(String namespaceName) throws NamespaceException;

  NamespacePropertyUpdate updateProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws NamespaceException;

  boolean containsEntities(String namespaceName) throws NamespaceException;
}
