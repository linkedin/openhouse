package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.common.exception.checked.DomainException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import java.util.Map;
import java.util.Set;

public interface HouseNamespaceRepository {
  NamespaceMetadata create(String namespaceName, Map<String, String> properties)
      throws DomainException;

  NamespaceMetadata getByName(String namespaceName) throws DomainException;

  void delete(String namespaceName) throws DomainException;

  NamespacePropertyUpdate updateProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws DomainException;

  boolean containsEntities(String namespaceName) throws DomainException;
}
