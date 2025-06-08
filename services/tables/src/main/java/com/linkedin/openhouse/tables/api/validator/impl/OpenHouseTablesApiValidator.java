package com.linkedin.openhouse.tables.api.validator.impl;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;
import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.common.api.validator.ApiValidatorUtil;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.common.TableType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseTablesApiValidator implements TablesApiValidator {

  @Autowired private Validator validator;

  @Autowired private RetentionPolicySpecValidator retentionPolicySpecValidator;

  @Autowired private ClusteringSpecValidator clusteringSpecValidator;

  @Autowired private ReplicationConfigValidator replicationConfigValidator;

  @Autowired private HistoryPolicySpecValidator historyPolicySpecValidator;

  @Override
  public void validateGetTable(String databaseId, String tableId) {
    List<String> validationFailures = new ArrayList<>();
    validateDatabaseId(databaseId, validationFailures);
    validateTableId(tableId, validationFailures);
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  @Override
  public void validateSearchTables(String databaseId) {
    List<String> validationFailures = new ArrayList<>();
    validateDatabaseId(databaseId, validationFailures);
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  @SuppressWarnings("checkstyle:OperatorWrap")
  @Override
  public void validateCreateTable(
      String clusterId,
      String databaseId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody) {
    List<String> validationFailures = new ArrayList<>();
    for (ConstraintViolation<CreateUpdateTableRequestBody> violation :
        validator.validate(createUpdateTableRequestBody)) {
      validationFailures.add(
          String.format("%s : %s", ApiValidatorUtil.getField(violation), violation.getMessage()));
    }
    if (!createUpdateTableRequestBody.getClusterId().equals(clusterId)) {
      validationFailures.add(
          String.format(
              "clusterId : provided %s, doesn't match with the server cluster %s",
              createUpdateTableRequestBody.getClusterId(), clusterId));
    }
    if (!createUpdateTableRequestBody.getDatabaseId().equals(databaseId)) {
      validationFailures.add(
          String.format(
              "databaseId : provided %s, doesn't match with the RequestBody %s",
              databaseId, createUpdateTableRequestBody.getDatabaseId()));
    }
    if (createUpdateTableRequestBody.getSchema() != null
        && getSchemaFromSchemaJson(createUpdateTableRequestBody.getSchema()).columns().isEmpty()) {
      validationFailures.add(
          String.format(
              "schema : provided %s, should contain at least one column",
              createUpdateTableRequestBody.getSchema()));
    }
    validationFailures.addAll(validateUUIDForReplicaTable(createUpdateTableRequestBody));
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
    validatePolicies(createUpdateTableRequestBody);
    if (createUpdateTableRequestBody.getClustering() != null) {
      clusteringSpecValidator.validate(
          createUpdateTableRequestBody.getClustering(),
          databaseId,
          createUpdateTableRequestBody.getTableId());
    }
  }

  /**
   * validateUUIDForReplicaTable checks if the openhouse.UUID value in table properties is valid for
   * a tableType=REPLICA_TABLE
   *
   * @param createUpdateTableRequestBody
   * @return List<String> representing validation errors
   */
  private List<String> validateUUIDForReplicaTable(
      CreateUpdateTableRequestBody createUpdateTableRequestBody) {
    List<String> validationFailures = new ArrayList<>();
    if (TableType.REPLICA_TABLE == createUpdateTableRequestBody.getTableType()) {
      Map<String, String> tableProperties = createUpdateTableRequestBody.getTableProperties();
      String uuid = tableProperties.get(CatalogConstants.OPENHOUSE_UUID_KEY);
      try {
        if (tableProperties.containsKey(CatalogConstants.OPENHOUSE_UUID_KEY)) {
          UUID.fromString(uuid);
        } else {
          validationFailures.add(
              String.format(
                  "tableProperties should contain a valid %s property for tableType: %s",
                  CatalogConstants.OPENHOUSE_UUID_KEY, TableType.REPLICA_TABLE));
        }
      } catch (IllegalArgumentException e) {
        validationFailures.add(String.format("UUID: provided %s is an invalid UUID string", uuid));
      }
    }
    return validationFailures;
  }

  private void validatePolicies(CreateUpdateTableRequestBody createUpdateTableRequestBody) {
    if (createUpdateTableRequestBody.getPolicies() == null) {
      return;
    }

    TableUri tableUri =
        TableUri.builder()
            .tableId(createUpdateTableRequestBody.getTableId())
            .clusterId(createUpdateTableRequestBody.getClusterId())
            .databaseId(createUpdateTableRequestBody.getDatabaseId())
            .build();

    List<PolicySpecValidator> validators =
        Arrays.asList(
            retentionPolicySpecValidator, replicationConfigValidator, historyPolicySpecValidator);
    for (PolicySpecValidator validator : validators) {
      if (!validator.validate(createUpdateTableRequestBody, tableUri)) {
        throw new RequestValidationFailureException(
            Arrays.asList(String.format("%s : %s", validator.getField(), validator.getMessage())));
      }
    }
  }

  @SuppressWarnings("checkstyle:OperatorWrap")
  @Override
  public void validateUpdateTable(
      String clusterId,
      String databaseId,
      String tableId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody) {
    List<String> validationFailures = new ArrayList<>();
    for (ConstraintViolation<CreateUpdateTableRequestBody> violation :
        validator.validate(createUpdateTableRequestBody)) {
      validationFailures.add(
          String.format("%s : %s", ApiValidatorUtil.getField(violation), violation.getMessage()));
    }
    if (!createUpdateTableRequestBody.getClusterId().equals(clusterId)) {
      validationFailures.add(
          String.format(
              "clusterId : provided %s, doesnt match with the server cluster %s",
              createUpdateTableRequestBody.getClusterId(), clusterId));
    }
    if (!createUpdateTableRequestBody.getDatabaseId().equals(databaseId)) {
      validationFailures.add(
          String.format(
              "databaseId : provided %s, doesn't match with the RequestBody %s",
              databaseId, createUpdateTableRequestBody.getDatabaseId()));
    }
    if (!createUpdateTableRequestBody.getTableId().equals(tableId)) {
      validationFailures.add(
          String.format(
              "tableId : provided %s, doesn't match with the RequestBody %s",
              tableId, createUpdateTableRequestBody.getTableId()));
    }
    if (createUpdateTableRequestBody.getSchema() != null
        && getSchemaFromSchemaJson(createUpdateTableRequestBody.getSchema()).columns().isEmpty()) {
      validationFailures.add(
          String.format(
              "schema : provided %s, should contain at least one column",
              createUpdateTableRequestBody.getSchema()));
    }
    if (createUpdateTableRequestBody.isStageCreate()) {
      validationFailures.add(
          String.format(
              "Staged table %s.%s cannot be created with PUT request, please use the POST apis",
              databaseId, tableId));
    }
    if (createUpdateTableRequestBody.getClustering() != null
        && createUpdateTableRequestBody.getClustering().size() > MAX_ALLOWED_CLUSTERING_COLUMNS) {
      validationFailures.add(
          String.format(
              "table %s.%s has %s clustering columns specified, max clustering columns supported is %s",
              databaseId,
              tableId,
              createUpdateTableRequestBody.getClustering().size(),
              MAX_ALLOWED_CLUSTERING_COLUMNS));
    }
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
    validatePolicies(createUpdateTableRequestBody);
    if (createUpdateTableRequestBody.getClustering() != null) {
      clusteringSpecValidator.validate(
          createUpdateTableRequestBody.getClustering(),
          databaseId,
          createUpdateTableRequestBody.getTableId());
    }
  }

  @Override
  public void validateDeleteTable(String databaseId, String tableId) {
    // Validation is similar to GetTable. just call that.
    validateGetTable(databaseId, tableId);
  }

  @Override
  public void validateRenameTable(
      String fromDatabaseId, String fromTableId, String toDatabaseId, String toTableId) {
    validateGetTable(fromDatabaseId, fromTableId);
    validateGetTable(toDatabaseId, toTableId);
    List<String> validationFailures = new ArrayList<>();
    // TODO: support renames across databases
    if (!fromDatabaseId.equalsIgnoreCase(toDatabaseId)) {
      validationFailures.add(
          String.format(
              "Rename table across databases is not supported. Cannot rename table %s from %s to %s",
              fromTableId, fromDatabaseId, toDatabaseId));
    } else if (fromTableId.equalsIgnoreCase(toTableId)) {
      validationFailures.add(
          String.format(
              "Cannot rename a table to the same current db name and table name: %s", toTableId));
    }
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  @SuppressWarnings("checkstyle:OperatorWrap")
  @Override
  public void validateUpdateAclPolicies(
      String databaseId,
      String tableId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody) {
    List<String> validationFailures = new ArrayList<>();

    for (ConstraintViolation<UpdateAclPoliciesRequestBody> violation :
        validator.validate(updateAclPoliciesRequestBody)) {
      validationFailures.add(
          String.format("%s : %s", ApiValidatorUtil.getField(violation), violation.getMessage()));
    }
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }

    // Validate databaseId and tableId
    validateGetTable(databaseId, tableId);
  }

  @Override
  public void validateGetAclPolicies(String databaseId, String tableId) {
    // Validation is similar to GetTable.
    validateGetTable(databaseId, tableId);
  }

  @Override
  public void validateCreateLock(
      String databaseId, String tableId, CreateUpdateLockRequestBody createUpdateLockRequestBody) {
    List<String> validationFailures = new ArrayList<>();
    validateGetTable(databaseId, tableId);
    if (!createUpdateLockRequestBody.isLocked()) {
      String errMsg =
          String.format(
              "Locked state on databaseId: %s tableId: %s should be set to true. Provided: false",
              databaseId, tableId);
      validationFailures.add(errMsg);
    }
    if (createUpdateLockRequestBody.getExpirationInDays() < 0) {
      String errMsg =
          String.format(
              "Lock creation time cannot be less that expiration time. Creation Time: %s, expiration time: %s days",
              createUpdateLockRequestBody.getCreationTime(),
              createUpdateLockRequestBody.getExpirationInDays());
      validationFailures.add(errMsg);
    }
    if (validationFailures.size() > 0) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  private void validateDatabaseId(String databaseId, List<String> validationFailures) {
    if (StringUtils.isEmpty(databaseId)) {
      validationFailures.add("databaseId : Cannot be empty");
    } else if (!databaseId.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      validationFailures.add(
          String.format(
              "databaseId : provided %s, %s", databaseId, ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    }
  }

  private void validateTableId(String tableId, List<String> validationFailures) {
    if (StringUtils.isEmpty(tableId)) {
      validationFailures.add("tableId : Cannot be empty");
    } else if (!tableId.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      validationFailures.add(
          String.format("tableId : provided %s, %s", tableId, ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    }
  }
}
