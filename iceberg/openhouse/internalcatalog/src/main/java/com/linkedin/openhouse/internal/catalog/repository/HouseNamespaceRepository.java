package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface HouseNamespaceRepository {
  NamespaceMetadata create(String namespaceName, Map<String, String> properties)
      throws HouseNamespaceException;

  Optional<NamespaceMetadata> findByName(String namespaceName) throws HouseNamespaceException;

  void delete(String namespaceName) throws HouseNamespaceException;

  NamespacePropertyUpdate updateProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws HouseNamespaceException;

  boolean containsEntities(String namespaceName) throws HouseNamespaceException;
}
