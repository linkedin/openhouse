package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.tables.exception.InvalidPropertyUpdateException;
import java.util.Map;
import java.util.Set;

public interface NamespacesService {
  NamespaceMetadata createNamespace(
      String namespace, Map<String, String> properties, String principal)
      throws DuplicateCreationException;

  NamespaceMetadata loadNamespaceMetadata(String namespace, String principal)
      throws NotFoundException;

  NamespacePropertyUpdate updateNamespaceProperties(
      String namespace, Map<String, String> updates, Set<String> removals, String principal)
      throws NotFoundException, InvalidPropertyUpdateException;

  void dropNamespace(String namespace, String principal) throws NotFoundException;
}
