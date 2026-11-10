package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException;
import com.linkedin.openhouse.tables.exception.NamespaceException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface NamespacesService {
  NamespaceMetadata createNamespace(
      List<String> namespace, Map<String, String> properties, String principal)
      throws NamespaceException, HouseNamespaceException;

  NamespaceMetadata loadNamespaceMetadata(String namespace, String principal)
      throws NamespaceException, HouseNamespaceException;

  NamespacePropertyUpdate updateNamespaceProperties(
      String namespace, Map<String, String> updates, Set<String> removals, String principal)
      throws NamespaceException, HouseNamespaceException;

  void dropNamespace(String namespace, String principal)
      throws NamespaceException, HouseNamespaceException;
}
