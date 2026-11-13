package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.InvalidPropertyUpdateException;
import com.linkedin.openhouse.common.exception.checked.InvalidRequestException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.tables.exception.NotEmptyException;
import com.linkedin.openhouse.tables.exception.PermissionDeniedException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface NamespacesService {
  NamespaceMetadata createNamespace(
      List<String> namespace, Map<String, String> properties, String principal)
      throws InvalidRequestException, PermissionDeniedException, DuplicateCreationException;

  NamespaceMetadata loadNamespaceMetadata(String namespace, String principal)
      throws InvalidRequestException, PermissionDeniedException, NotFoundException;

  NamespacePropertyUpdate updateNamespaceProperties(
      String namespace, Map<String, String> updates, Set<String> removals, String principal)
      throws InvalidRequestException, PermissionDeniedException, NotFoundException,
          InvalidPropertyUpdateException;

  void dropNamespace(String namespace, String principal)
      throws InvalidRequestException, PermissionDeniedException, NotFoundException,
          NotEmptyException;
}
