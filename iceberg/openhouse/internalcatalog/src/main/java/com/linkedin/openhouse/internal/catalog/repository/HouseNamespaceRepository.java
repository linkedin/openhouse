package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import java.util.Map;

public interface HouseNamespaceRepository {
  NamespaceMetadata create(String namespaceName, Map<String, String> properties)
      throws DuplicateCreationException;

  NamespaceMetadata getByName(String namespaceName) throws NotFoundException;

  void delete(String namespaceName) throws NotFoundException;

  NamespaceMetadata update(String namespaceName, NamespaceMetadata document)
      throws NotFoundException;
}
