package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import java.util.Map;
import java.util.Set;

public interface HouseNamespaceRepository {
  NamespaceMetadata create(String namespaceName, Map<String, String> properties)
      throws DuplicateCreationException;

  NamespaceMetadata getByName(String namespaceName) throws NotFoundException;

  void delete(String namespaceName) throws NotFoundException;

  NamespacePropertyUpdate updateProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws NotFoundException;

  boolean containsEntities(String namespaceName);
}
