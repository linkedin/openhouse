package com.linkedin.openhouse.tables.services;

import static com.linkedin.openhouse.tables.exception.NamespaceException.Reason.*;

import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.HouseNamespaceRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException.Reason;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.NamespaceException;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class IcebergNamespacesService {
  private final HouseNamespaceRepository namespaceRepository;
  private final AuthorizationUtils authorizationUtils;

  public NamespaceMetadata create(
      List<String> namespace, Map<String, String> properties, String principal)
      throws NamespaceException, HouseNamespaceException {
    if (namespace.size() != 1) {
      throw new NamespaceException(INVALID_NAMESPACE, "Exactly one namespace level is required");
    }
    authorize(namespace.get(0), principal, Privileges.SYSTEM_ADMIN);
    return namespaceRepository.create(namespace.get(0), properties);
  }

  public NamespaceMetadata load(String namespace, String principal)
      throws NamespaceException, HouseNamespaceException {
    authorize(namespace, principal, Privileges.GET_TABLE_METADATA);
    return namespaceRepository
        .findByName(namespace)
        .orElseThrow(() -> new HouseNamespaceException(Reason.NOT_FOUND));
  }

  public NamespacePropertyUpdate update(
      String namespace, Map<String, String> updates, Set<String> removals, String principal)
      throws NamespaceException, HouseNamespaceException {
    authorize(namespace, principal, Privileges.SYSTEM_ADMIN);
    if (!Collections.disjoint(updates.keySet(), removals)) {
      throw new NamespaceException(
          INVALID_PROPERTIES, "Updates and removals must use disjoint keys");
    }
    return namespaceRepository.updateProperties(namespace, updates, removals);
  }

  public void drop(String namespace, String principal)
      throws NamespaceException, HouseNamespaceException {
    authorize(namespace, principal, Privileges.DELETE_TABLE);
    namespaceRepository
        .findByName(namespace)
        .orElseThrow(() -> new HouseNamespaceException(Reason.NOT_FOUND));
    if (namespaceRepository.containsEntities(namespace)) {
      throw new NamespaceException(NOT_EMPTY, "Namespace contains tables or views");
    }
    namespaceRepository.delete(namespace);
  }

  private void authorize(String namespace, String principal, Privileges privilege)
      throws NamespaceException {
    if (!namespace.matches("[a-zA-Z0-9_]{1,128}")) {
      throw new NamespaceException(
          INVALID_NAMESPACE,
          "Namespace must be one namespace name of at most 128 letters, digits, or underscores");
    }
    try {
      authorizationUtils.checkDatabasePrivilege(
          namespace.toLowerCase(Locale.ROOT), principal, privilege);
    } catch (AuthorizationServiceException exception) {
      throw new NamespaceException(
          AUTHORIZATION_UNAVAILABLE, "Authorization service unavailable", exception);
    } catch (AccessDeniedException exception) {
      throw new NamespaceException(ACCESS_DENIED, "Access denied", exception);
    }
  }
}
