package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.checked.DomainException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface NamespacesService {
  NamespaceMetadata createNamespace(
      List<String> namespace, Map<String, String> properties, String principal)
      throws DomainException;

  NamespaceMetadata loadNamespaceMetadata(String namespace, String principal)
      throws DomainException;

  NamespacePropertyUpdate updateNamespaceProperties(
      String namespace, Map<String, String> updates, Set<String> removals, String principal)
      throws DomainException;

  void dropNamespace(String namespace, String principal) throws DomainException;
}
