package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.internal.catalog.model.DatabaseMetadata;
import com.linkedin.openhouse.internal.catalog.model.DatabasePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.HouseDatabaseRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException.Reason;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.NamespaceException;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class IcebergNamespacesService {
  private final HouseDatabaseRepository databaseRepository;
  private final AuthorizationUtils authorizationUtils;

  public DatabaseMetadata create(
      List<String> namespace, Map<String, String> properties, String principal)
      throws NamespaceException, HouseDatabaseException {
    if (namespace.size() != 1) {
      throw new NamespaceException(
          HttpStatus.BAD_REQUEST, "BadRequestException", "Exactly one namespace level is required");
    }
    authorize(namespace.get(0), principal, Privileges.SYSTEM_ADMIN);
    return databaseRepository.create(namespace.get(0), properties);
  }

  public DatabaseMetadata load(String namespace, String principal)
      throws NamespaceException, HouseDatabaseException {
    authorize(namespace, principal, Privileges.GET_TABLE_METADATA);
    return databaseRepository
        .findByName(namespace)
        .orElseThrow(() -> new HouseDatabaseException(Reason.NOT_FOUND));
  }

  public DatabasePropertyUpdate update(
      String namespace, Map<String, String> updates, Set<String> removals, String principal)
      throws NamespaceException, HouseDatabaseException {
    authorize(namespace, principal, Privileges.SYSTEM_ADMIN);
    if (!Collections.disjoint(updates.keySet(), removals)) {
      throw new NamespaceException(
          HttpStatus.UNPROCESSABLE_ENTITY,
          "UnprocessableEntityException",
          "Updates and removals must use disjoint keys");
    }
    return databaseRepository.updateProperties(namespace, updates, removals);
  }

  public void drop(String namespace, String principal)
      throws NamespaceException, HouseDatabaseException {
    authorize(namespace, principal, Privileges.DELETE_TABLE);
    databaseRepository
        .findByName(namespace)
        .orElseThrow(() -> new HouseDatabaseException(Reason.NOT_FOUND));
    if (databaseRepository.containsEntities(namespace)) {
      throw new NamespaceException(
          HttpStatus.CONFLICT, "NamespaceNotEmptyException", "Namespace contains tables or views");
    }
    databaseRepository.delete(namespace);
  }

  private void authorize(String namespace, String principal, Privileges privilege)
      throws NamespaceException {
    if (!namespace.matches("[a-zA-Z0-9_]{1,128}")) {
      throw new NamespaceException(
          HttpStatus.BAD_REQUEST,
          "BadRequestException",
          "Namespace must be one database name of at most 128 letters, digits, or underscores");
    }
    try {
      authorizationUtils.checkDatabasePrivilege(
          namespace.toLowerCase(Locale.ROOT), principal, privilege);
    } catch (AuthorizationServiceException exception) {
      throw new NamespaceException(
          HttpStatus.SERVICE_UNAVAILABLE,
          "ServiceUnavailableException",
          "Authorization service unavailable",
          exception);
    } catch (AccessDeniedException exception) {
      throw new NamespaceException(
          HttpStatus.FORBIDDEN, "ForbiddenException", "Access denied", exception);
    }
  }
}
