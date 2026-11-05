package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.housetables.client.model.DatabaseMetadata;
import com.linkedin.openhouse.housetables.client.model.UpdateDatabasePropertiesResponseBody;
import com.linkedin.openhouse.internal.catalog.repository.HouseDatabaseRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException.Reason;
import com.linkedin.openhouse.tables.api.spec.namespace.CreateNamespaceRequest;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceResponse;
import com.linkedin.openhouse.tables.api.spec.namespace.UpdateNamespacePropertiesRequest;
import com.linkedin.openhouse.tables.api.spec.namespace.UpdateNamespacePropertiesResponse;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.NamespaceException;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.Optional;
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

  public NamespaceResponse create(CreateNamespaceRequest request, String principal)
      throws NamespaceException, HouseDatabaseException {
    if (request.getNamespace().size() != 1) {
      throw new NamespaceException(
          HttpStatus.BAD_REQUEST, "BadRequestException", "Exactly one namespace level is required");
    }
    authorize(request.getNamespace().get(0), principal, Privileges.CREATE_TABLE);
    DatabaseMetadata metadata =
        databaseRepository.create(request.getNamespace().get(0), request.getProperties());
    return new NamespaceResponse(
        Collections.singletonList(
            Optional.ofNullable(metadata.getDatabaseName())
                .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE))),
        Optional.ofNullable(metadata.getProperties())
            .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE)));
  }

  public NamespaceResponse load(String namespace, String principal)
      throws NamespaceException, HouseDatabaseException {
    authorize(namespace, principal, Privileges.GET_TABLE_METADATA);
    DatabaseMetadata metadata =
        databaseRepository
            .findByName(namespace)
            .orElseThrow(() -> new HouseDatabaseException(Reason.NOT_FOUND));
    return new NamespaceResponse(
        Collections.singletonList(
            Optional.ofNullable(metadata.getDatabaseName())
                .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE))),
        Optional.ofNullable(metadata.getProperties())
            .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE)));
  }

  public UpdateNamespacePropertiesResponse update(
      String namespace, UpdateNamespacePropertiesRequest request, String principal)
      throws NamespaceException, HouseDatabaseException {
    authorize(namespace, principal, Privileges.UPDATE_TABLE_METADATA);
    if (request.getUpdates().keySet().stream().anyMatch(request.getRemovals()::contains)) {
      throw new NamespaceException(
          HttpStatus.UNPROCESSABLE_ENTITY,
          "UnprocessableEntityException",
          "Updates and removals must use disjoint keys");
    }
    UpdateDatabasePropertiesResponseBody result =
        databaseRepository.updateProperties(namespace, request.getUpdates(), request.getRemovals());
    return new UpdateNamespacePropertiesResponse(
        Optional.ofNullable(result.getUpdated())
            .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE)),
        Optional.ofNullable(result.getRemoved())
            .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE)),
        Optional.ofNullable(result.getMissing())
            .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE)));
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
      authorizationUtils.checkDatabasePrivilege(namespace, principal, privilege);
    } catch (AuthorizationServiceException exception) {
      throw new NamespaceException(
          HttpStatus.SERVICE_UNAVAILABLE,
          "ServiceUnavailableException",
          "Authorization service unavailable");
    } catch (AccessDeniedException exception) {
      throw new NamespaceException(HttpStatus.FORBIDDEN, "ForbiddenException", "Access denied");
    }
  }
}
